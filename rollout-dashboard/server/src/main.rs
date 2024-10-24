use async_stream::try_stream;
use axum::extract::Query;
use axum::http::StatusCode;
use axum::response::sse::Event;
use axum::response::Sse;
use axum::Json;
use axum::{routing::get, Router};
use chrono::{DateTime, Utc};
use frontend_api::RolloutDataCacheResponse;
use futures::stream::Stream;
use log::{debug, error, info};
use reqwest::Url;
use serde::{de, Deserialize, Deserializer, Serialize};
use serde_json::from_str;
use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::Infallible;
use std::env;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use std::process::ExitCode;
use std::str::FromStr;
use std::sync::Arc;

use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::watch::{self, Sender};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use tokio::{select, spawn};
use tower_http::services::ServeDir;

mod airflow_client;
mod frontend_api;
mod python;

use crate::airflow_client::{AirflowClient, AirflowError};
use crate::frontend_api::{RolloutApi, RolloutDataGatherError};
use rollout_dashboard::types::Rollout;

const BACKEND_REFRESH_UPDATE_INTERVAL: u64 = 15;

/// Contains either a list of rollouts ordered from newest to oldest,
/// dated from the last time it was successfully updated, or an HTTP
/// status code corresponding to -- and with -- a message for the last error.
type CurrentRolloutStatus = Result<VecDeque<Rollout>, (StatusCode, String)>;

struct Server {
    rollout_api: Arc<RolloutApi>,
    last_rollout_data: Arc<Mutex<CurrentRolloutStatus>>,
    stream_tx: Sender<CurrentRolloutStatus>,
}

impl Server {
    fn new(rollout_api: Arc<RolloutApi>) -> Self {
        let init: CurrentRolloutStatus = Err((StatusCode::NO_CONTENT, "".to_string()));
        let (stream_tx, _stream_rx) = watch::channel::<CurrentRolloutStatus>(init.clone());
        Self {
            rollout_api,
            last_rollout_data: Arc::new(Mutex::new(init)),
            stream_tx,
        }
    }
    async fn fetch_rollout_data(
        &self,
        max_rollouts: usize,
    ) -> Result<VecDeque<Rollout>, (StatusCode, String)> {
        match self.rollout_api.get_rollout_data(max_rollouts).await {
            Ok(rollouts) => Ok(rollouts.into()),
            Err(e) => {
                let res = match e {
                    RolloutDataGatherError::AirflowError(AirflowError::StatusCode(c)) => {
                        (c, "Internal server error".to_string())
                    }
                    RolloutDataGatherError::AirflowError(AirflowError::ReqwestError(err)) => {
                        let mut explanation = format!("Cannot contact Airflow: {}", err);
                        let mut err = err.source();
                        loop {
                            match err {
                                None => break,
                                Some(e) => {
                                    explanation = format!("{} -> {}", explanation.as_str(), e);
                                    err = e.source();
                                }
                            }
                        }
                        (StatusCode::BAD_GATEWAY, explanation)
                    }
                    RolloutDataGatherError::AirflowError(AirflowError::Other(msg)) => {
                        (StatusCode::INTERNAL_SERVER_ERROR, msg)
                    }
                    RolloutDataGatherError::RolloutPlanParseError(parse_error) => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("{}", parse_error),
                    ),
                    RolloutDataGatherError::CyclicDependency(dep) => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("A cyclic dependency was found in the task graph: {:?}", dep),
                    ),
                };
                Err(res)
            }
        }
    }

    async fn update_rollout_data<F>(&self, max_rollouts: usize, refresh_interval: u64, cancel: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut errored = false;
        tokio::pin!(cancel);

        loop {
            let loop_start_time: DateTime<Utc> = Utc::now();

            let d = select! {
                d = self.fetch_rollout_data(max_rollouts) => {
                    match &d {
                        Ok(new_rollouts) => {
                            let loop_delta_time = Utc::now() - loop_start_time;
                            info!(target: "server::update_loop", "After {}, obtained {} rollouts from Airflow", loop_delta_time, new_rollouts.len());
                            if errored {
                                info!(target: "server::update_loop", "Successfully processed rollout data again after temporary error");
                                // Clear error flag.
                                errored = false;
                                // Ensure our data structure is overwritten by whatever data we obtained after the last loop.
                            }
                        }
                        Err(res) => {
                            error!(
                                target: "server::update_loop", "After processing fetch_rollout_data: {}",
                                res.1
                            );
                            errored = true;
                        }
                    }
                    d
                },
                _ = &mut cancel => break,
            };

            let _ = self.stream_tx.send_replace(d.clone());
            let mut current_rollout_data = self.last_rollout_data.lock().await;
            *current_rollout_data = d;
            drop(current_rollout_data);

            select! {
                _ = sleep(Duration::from_secs(refresh_interval)) => (),
                _ = &mut cancel => break,
            }
        }
    }

    // #[debug_handler]
    async fn get_rollout_data(&self) -> Result<Json<VecDeque<Rollout>>, (StatusCode, String)> {
        let m = self.last_rollout_data.lock().await.clone();
        match m {
            Ok(rollouts) => Ok(Json(rollouts)),
            Err(e) => Err(e),
        }
    }

    // #[debug_handler]
    async fn get_cache(&self) -> Result<Json<Vec<RolloutDataCacheResponse>>, (StatusCode, String)> {
        let m = self.rollout_api.get_cache().await;
        Ok(Json(m))
    }

    /// Produce an SSE stream structured as a dictionary:
    /// * rollouts: appears and contains a list of Rollout when rollouts have been updated and
    ///             the caller did not indicate delta_support.  If the caller indicated
    ///             delta_support, then this appears only on initial connection, or after a
    ///             backend error has been reported in a prior message.
    /// * error: always appears, but only contains an error (is non-null) when there was an
    ///          error polling Airflow.
    /// * updated: when the caller indicates delta_support, this appears and updated rollouts
    ///            are listed here.
    /// * deleted: when the caller indicates delta_support, this appears and lists the names
    ///            of the rollouts that have disappeared.
    fn produce_rollouts_sse_stream(
        &self,
        delta_support: bool,
    ) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
        struct DisconnectionGuard {}

        impl Default for DisconnectionGuard {
            fn default() -> Self {
                debug!(target: "server::sse", "New client connected.");
                Self {}
            }
        }

        impl Drop for DisconnectionGuard {
            fn drop(&mut self) {
                debug!(target: "server::sse", "Client disconnected.");
            }
        }

        #[derive(Serialize, Default)]
        struct SseResponse {
            #[serde(skip_serializing_if = "Option::is_none")]
            rollouts: Option<VecDeque<Rollout>>,
            #[serde(skip_serializing_if = "VecDeque::is_empty")]
            updated: VecDeque<Rollout>,
            #[serde(skip_serializing_if = "VecDeque::is_empty")]
            deleted: VecDeque<String>,
            error: Option<(u16, String)>,
        }

        impl SseResponse {
            fn error(e: &(StatusCode, String)) -> Self {
                Self {
                    error: Some((e.0.as_u16(), e.1.clone())),
                    ..Default::default()
                }
            }
            fn full(rollouts: &VecDeque<Rollout>) -> Self {
                Self {
                    rollouts: Some(rollouts.clone()),
                    ..Default::default()
                }
            }
        }

        let mut stream_rx = self.stream_tx.subscribe();

        let stream = try_stream! {
            // Set up something that will be dropped (thus log) when SSE is disconnected.
            let disconnection_guard = DisconnectionGuard::default();

            // Set an initial message to diff the first broadcast message against.
            let mut last_rollout_data: CurrentRolloutStatus = Err((StatusCode::OK, "".to_string()));

            loop {
                let current_rollout_data = stream_rx.borrow_and_update().clone();

                let message = match (&current_rollout_data, &last_rollout_data) {
                    // Error before.  Send full sync.
                    (Ok(new_rollouts), Err(_)) => Some(SseResponse::full(new_rollouts)),
                    // Last time was a good update.  Send differential sync.
                    (Ok(new_rollouts), Ok(old_rollouts)) => {
                        let new_names = new_rollouts.iter().map(|r| r.name.clone()).collect::<HashSet<String>>();
                        let old_rollouts_map = old_rollouts.iter().map(|r| (r.name.clone(), r)).collect::<HashMap<String, &Rollout>>();
                            let updated = new_rollouts.iter().filter_map(|r| match old_rollouts_map.get(&r.name) { None => Some(r.clone()), Some(old_rollout) => match r.update_count != old_rollout.update_count {true => Some(r.clone()), false => None}}).collect::<VecDeque<Rollout>>();
                            let deleted = old_rollouts.iter().filter_map(|r| match new_names.contains(&r.name) { true => None, false => Some(r.name.clone())}).collect::<VecDeque<String>>();
                            match updated.is_empty() && deleted.is_empty() {
                                true => None,
                                false => match delta_support {
                                    false => Some(SseResponse::full(new_rollouts)),
                                    true => Some(SseResponse{
                                        updated,
                                        deleted,
                                        error: None,
                                        ..Default::default()
                                    }),
                                }
                            }
                    }
                    // Error after a good update.  Send error.
                    (Err(e), Ok(_)) => Some(SseResponse::error(e)),
                    // Error after an error.  Only send update if errors differ.
                    (Err(e), Err(olde)) => match e == olde {
                        true => None,
                        false => Some(SseResponse::error(e)),
                    },
                };

                match &message {
                    Some(m) => yield Event::default().data(serde_json::to_string(m).unwrap()),
                    None => ()
                }

                last_rollout_data = current_rollout_data;
                if stream_rx.changed().await.is_err() {
                    break;
                }
            }

            // Drop the disconnection guard to log the message that the client disconnected.
            drop(disconnection_guard);
        };

        Sse::new(stream).keep_alive(
            axum::response::sse::KeepAlive::new()
                .interval(Duration::from_secs(5))
                .text("keepalive"),
        )
    }
}

/// Serde deserialization decorator to map empty Strings to None,
fn empty_value_as_true<'de, D, T>(de: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: FromStr,
    T::Err: fmt::Display,
{
    let opt = Option::<String>::deserialize(de)?;
    match opt.as_deref() {
        Some("") | None => FromStr::from_str("true")
            .map_err(de::Error::custom)
            .map(Some),
        Some(s) => FromStr::from_str(s).map_err(de::Error::custom).map(Some),
    }
}

#[derive(Deserialize)]
struct SseHandlerParameters {
    #[serde(default, deserialize_with = "empty_value_as_true")]
    incremental: Option<bool>,
}

#[tokio::main]
async fn main() -> ExitCode {
    env_logger::init();

    let max_rollouts = from_str::<usize>(
        env::var("MAX_ROLLOUTS")
            .unwrap_or("10".to_string())
            .as_str(),
    )
    .unwrap();
    let refresh_interval = from_str::<u64>(
        &env::var("REFRESH_INTERVAL").unwrap_or(format!("{}", BACKEND_REFRESH_UPDATE_INTERVAL)),
    )
    .unwrap();
    let backend_host = env::var("BACKEND_HOST").unwrap_or("127.0.0.1:4174".to_string());
    let airflow_url_str =
        env::var("AIRFLOW_URL").unwrap_or("http://admin:password@localhost:8080/".to_string());
    let airflow_url = Url::parse(&airflow_url_str).unwrap();
    let frontend_static_dir = env::var("FRONTEND_STATIC_DIR").unwrap_or(".".to_string());
    let addr: SocketAddr = backend_host.parse().unwrap();

    let server = Arc::new(Server::new(Arc::new(RolloutApi::new(
        AirflowClient::new(airflow_url).unwrap(),
    ))));
    let server_background_update = server.clone();

    let (stop_loop_tx, mut stop_loop_rx) = watch::channel(());
    let (stop_serve_tx, mut stop_serve_rx) = watch::channel(());
    let (finish_loop_tx, mut finish_loop_rx) = watch::channel(());

    let server_for_rollouts_handler = server.clone();
    let server_for_cache_handler = server.clone();
    let server_for_sse_handler = server.clone();
    let rollouts_handler =
        move || async move { server_for_rollouts_handler.get_rollout_data().await };
    let cached_data_handler = move || async move { server_for_cache_handler.get_cache().await };
    let rollouts_sse_handler = move |options: Query<SseHandlerParameters>| {
        let options: SseHandlerParameters = options.0;
        async move {
            server_for_sse_handler
                .produce_rollouts_sse_stream(options.incremental.unwrap_or_default())
        }
    };
    let mut tree = Router::new();
    tree = tree
        .route("/api/v1/rollouts", get(rollouts_handler))
        .route("/api/v1/cache", get(cached_data_handler))
        .route("/api/v1/rollouts/sse", get(rollouts_sse_handler));
    tree = tree.nest_service("/", ServeDir::new(frontend_static_dir));

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();

    tokio::spawn(async move {
        let mut sigterm = signal(SignalKind::terminate()).unwrap();
        select! {
            _ignored1 = sigterm.recv() => info!("Received SIGTERM"),
            _ignored2 = finish_loop_rx.changed() => info!("Shutting down"),
        };
        stop_serve_tx.send(()).unwrap_or(());
        stop_loop_tx.send(()).unwrap_or(());
    });

    let serve_fut =
        axum::serve(listener, tree.into_make_service()).with_graceful_shutdown(async move {
            let _ = stop_serve_rx.changed().await;
        });

    let background_loop_fut = spawn(async move {
        server_background_update
            .update_rollout_data(max_rollouts, refresh_interval, async move {
                let _ = stop_loop_rx.changed().await;
            })
            .await
    });

    let ret = match serve_fut.await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            error!(target: "main", "Error serving: {}", err);
            ExitCode::FAILURE
        }
    };
    finish_loop_tx.send(()).unwrap_or(());
    background_loop_fut.await.unwrap();
    ret
}
