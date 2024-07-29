// FIXME remove all use of unwrap().
// FIXME tolerate other types of error not just AirflowError.
// FIXME make AirflowError more explanatory, not just ::Other()

// use axum::debug_handler;
use axum::http::StatusCode;
use axum::Json;
use axum::{routing::get, Router};
use log::{error, info};
use reqwest::Url;
use std::env;
use std::error::Error;
use std::future::Future;
use std::net::SocketAddr;
use std::process::ExitCode;
use std::sync::Arc;
use std::vec::Vec;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::watch;
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

type CurrentRolloutStatus = Result<Vec<Rollout>, (StatusCode, String)>;

struct Server {
    rollout_api: Arc<RolloutApi>,
    last_rollout_data: Arc<Mutex<CurrentRolloutStatus>>,
}

impl Server {
    fn new(rollout_api: Arc<RolloutApi>) -> Self {
        Self {
            rollout_api,
            last_rollout_data: Arc::new(Mutex::new(Err((StatusCode::NO_CONTENT, "".to_string())))),
        }
    }
    async fn fetch_rollout_data(&self) -> Result<Vec<Rollout>, (StatusCode, String)> {
        match self.rollout_api.get_rollout_data().await {
            Ok(rollouts) => Ok(rollouts),
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
                };
                Err(res)
            }
        }
    }

    async fn update_rollout_data<F>(&self, cancel: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut errored = false;
        tokio::pin!(cancel);

        loop {
            let data = select! {
               d = self.fetch_rollout_data() => {
                   match &d {
                       Ok(_) => {
                           if errored {
                               info!(target: "http_client", "Successfully processed rollout data again after temporary error");
                               errored = false
                           }
                       }
                       Err(res) => {
                           error!(
                               target: "http_client", "After processing fetch_rollout_data: {}",
                               res.1
                           );
                           errored = true
                       }
                   };
                   d
               },
               _ignored = &mut cancel => break,
            };

            let mut container = self.last_rollout_data.lock().await;
            *container = data;
            drop(container);

            select! {
                _ignored1 = sleep(Duration::from_secs(BACKEND_REFRESH_UPDATE_INTERVAL)) => (),
                _ignored2 = &mut cancel => break,
            }
        }
    }

    // #[debug_handler]
    async fn get_rollout_data(&self) -> Result<Json<Vec<Rollout>>, (StatusCode, String)> {
        let m = self.last_rollout_data.lock().await.clone();
        match m {
            Ok(rollouts) => Ok(Json(rollouts)),
            Err(e) => Err(e),
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    env_logger::init();

    let backend_host = env::var("BACKEND_HOST").unwrap_or("127.0.0.1:4174".to_string());
    let airflow_url_str =
        env::var("AIRFLOW_URL").unwrap_or("http://admin:password@localhost:8080/".to_string());
    let airflow_url = Url::parse(&airflow_url_str).unwrap();
    let frontend_static_dir = env::var("FRONTEND_STATIC_DIR").unwrap_or(".".to_string());
    let addr: SocketAddr = backend_host.parse().unwrap();

    let server = Arc::new(Server::new(Arc::new(RolloutApi::new(AirflowClient::new(
        airflow_url,
    )))));
    let server_background_update = server.clone();

    let (stop_loop_tx, mut stop_loop_rx) = watch::channel(());
    let (stop_serve_tx, mut stop_serve_rx) = watch::channel(());
    let (finish_loop_tx, mut finish_loop_rx) = watch::channel(());

    let rollouts_handler = move || async move { server.get_rollout_data().await };
    let mut tree = Router::new();
    tree = tree.route("/api/v1/rollouts", get(rollouts_handler));
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
            .update_rollout_data(async move {
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
