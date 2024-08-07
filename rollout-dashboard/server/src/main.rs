use async_stream::try_stream;
use axum::http::StatusCode;
use axum::response::sse::Event;
use axum::response::Sse;
use axum::Json;
use axum::{routing::get, Router};
use chrono::{DateTime, Utc};
use futures::stream::Stream;
use log::{debug, error, info};
use reqwest::Url;
use serde::Serialize;
use serde_json::from_str;
use std::collections::VecDeque;
use std::convert::Infallible;
use std::env;
use std::error::Error;
use std::future::Future;
use std::net::SocketAddr;
use std::process::ExitCode;
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
use crate::frontend_api::{Rollout, RolloutApi, RolloutDataGatherError};

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
        let init = Err((StatusCode::NO_CONTENT, "".to_string()));
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
    ) -> Result<(VecDeque<Rollout>, bool), (StatusCode, String)> {
        match self.rollout_api.get_rollout_data(max_rollouts).await {
            Ok((rollouts, updated)) => Ok((rollouts.into(), updated)),
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

            let mut changed = true;
            let data = select! {
                d = self.fetch_rollout_data(max_rollouts) => {
                    match d {
                        Ok((new_rollouts, updated)) => {
                            let loop_delta_time = Utc::now() - loop_start_time;
                            info!(target: "update_loop", "After {}, obtained {} rollouts from Airflow (updated: {})", loop_delta_time, new_rollouts.len(), updated);
                            changed = updated;
                            if errored {
                                info!(target: "update_loop", "Successfully processed rollout data again after temporary error");
                                // Clear error flag.
                                errored = false;
                                // Ensure our data structure is overwritten by whatever data we obtained after the last loop.
                                changed = true;
                            }
                            Ok(new_rollouts)
                        }
                        Err(res) => {
                            error!(
                                target: "update_loop", "After processing fetch_rollout_data: {}",
                                res.1
                            );
                            errored = true;
                            Err(res)
                        }
                    }
                },
                _ignored = &mut cancel => break,
            };

            if changed {
                let _ = self.stream_tx.send(data.clone());

                let mut container = self.last_rollout_data.lock().await;
                *container = data;
                drop(container);
            }

            select! {
                _ignored1 = sleep(Duration::from_secs(refresh_interval)) => (),
                _ignored2 = &mut cancel => break,
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
    fn produce_rollouts_sse_stream(&self) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
        debug!(target: "sse", "New client connected.");

        struct DisconnectionGuard {}

        impl Drop for DisconnectionGuard {
            fn drop(&mut self) {
                debug!(target: "sse", "Client disconnected.");
            }
        }

        #[derive(Serialize)]
        struct SseResponse {
            rollouts: VecDeque<Rollout>,
            error: Option<(u16, String)>,
        }

        let mut stream_rx = self.stream_tx.subscribe();
        let last_rollout_data = self.last_rollout_data.clone();
        let stream = try_stream! {
            // Set up something that will be dropped (thus log) when SSE is disconnected.
            let guard = DisconnectionGuard{};

            // Send first message from existing data.
            let mfk = last_rollout_data.lock().await.clone();
            let data_to_serialize = match &mfk {
                Ok(rollouts) => serde_json::to_string(&SseResponse{rollouts: rollouts.clone(), error: None}),
                Err(e) => serde_json::to_string(&SseResponse{rollouts: VecDeque::new(), error: Some((e.0.as_u16(), e.1.clone()))}),
            }.unwrap();
            drop(mfk);
            let event = Event::default().data(data_to_serialize);
            yield event;

            loop {
                if stream_rx.changed().await.is_err() {
                    debug!(target: "sse", "No more transmissions.  Stopping client SSE streaming.");
                    break;
                }
                let current_rollout_status = &stream_rx.borrow_and_update().clone();
                let mfk = current_rollout_status.clone();
                let data_to_serialize = match mfk {
                    Ok(rollouts) => serde_json::to_string(&SseResponse{rollouts, error: None}),
                    Err(e) => serde_json::to_string(&SseResponse{rollouts: VecDeque::new(), error: Some((e.0.as_u16(), e.1))}),
                }.unwrap();
                let event = Event::default().data(data_to_serialize);
                yield event;
            }
            drop(guard);
        };

        Sse::new(stream).keep_alive(
            axum::response::sse::KeepAlive::new()
                .interval(Duration::from_secs(5))
                .text("keepalive"),
        )
    }
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
    let server_for_sse_handler = server.clone();
    let rollouts_handler =
        move || async move { server_for_rollouts_handler.get_rollout_data().await };
    let rollouts_sse_handler =
        move || async move { server_for_sse_handler.produce_rollouts_sse_stream() };
    let mut tree = Router::new();
    tree = tree.route("/api/v1/rollouts", get(rollouts_handler));
    tree = tree.route("/api/v1/rollouts/sse", get(rollouts_sse_handler));
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
