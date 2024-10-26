use live_state::AirflowStateSyncer;
use log::{error, info};
use reqwest::Url;
use serde_json::from_str;
use std::env;
use std::net::SocketAddr;
use std::process::ExitCode;
use std::sync::Arc;

use tokio::select;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::watch;
use tower_http::services::ServeDir;

mod api_server;
mod live_state;

use crate::live_state::AirflowStateUpdater;
use rollout_dashboard::airflow_client::AirflowClient;

const BACKEND_REFRESH_UPDATE_INTERVAL: u64 = 15;

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

    let (end_tx, end_rx) = watch::channel(());

    let syncer = AirflowStateSyncer::new(
        AirflowStateUpdater::new(AirflowClient::new(airflow_url).unwrap()),
        max_rollouts,
        refresh_interval,
    );
    let (syncing_syncer, background_loop_fut) = syncer.start_syncing(end_rx.clone());
    let server = Arc::new(api_server::ApiServer::new(syncing_syncer));

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();

    tokio::spawn(async move {
        let mut sigterm = signal(SignalKind::terminate()).unwrap();
        select! {
            _ignored1 = sigterm.recv() => info!("Received SIGTERM"),
        };
        end_tx.send(()).unwrap_or(());
    });

    let mut end_rx_for_server = end_rx.clone();
    let ret = match axum::serve(
        listener,
        server
            .routes()
            .nest_service("/", ServeDir::new(frontend_static_dir))
            .into_make_service(),
    )
    .with_graceful_shutdown(async move {
        let _ = end_rx_for_server.changed().await;
    })
    .await
    {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            error!(target: "main", "Error serving: {}", err);
            ExitCode::FAILURE
        }
    };
    info!("Server finished");
    background_loop_fut.await.unwrap();
    info!("Update loop finished");
    ret
}
