// FIXME remove all use of unwrap().
// FIXME tolerate other types of error not just AirflowError.
// FIXME make AirflowError more explanatory, not just ::Other()

use axum::debug_handler;
use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use axum::{routing::get, Router};
use axum_server;
use reqwest::Url;
use std::env;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::vec::Vec;

mod airflow_client;
mod frontend_api;
mod python;

use crate::airflow_client::{AirflowClient, AirflowError};
use crate::frontend_api::{DashboardApi, Rollout, RolloutDataGatherError};

#[debug_handler]
async fn handler(
    State(state): State<Arc<DashboardApi>>,
) -> Result<Json<Vec<Rollout>>, (StatusCode, String)> {
    match state.get_rollout_data().await {
        Ok(rollouts) => Ok(Json(rollouts)),
        Err(e) => match e {
            RolloutDataGatherError::AirflowError(AirflowError::StatusCode(c)) => {
                Err((c, "Internal server error".to_string()))
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
                Err((StatusCode::BAD_GATEWAY, explanation))
            }
            RolloutDataGatherError::AirflowError(AirflowError::Other(msg)) => {
                Err((StatusCode::INTERNAL_SERVER_ERROR, msg))
            }
            RolloutDataGatherError::RolloutPlanParseError(parse_error) => Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("{}", parse_error),
            )),
        },
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let backend_host = env::var("BACKEND_HOST").unwrap_or("127.0.0.1:4174".to_string());
    let airflow_url_str =
        env::var("AIRFLOW_URL").unwrap_or("http://admin:password@localhost:8080/".to_string());
    let airflow_url = Url::parse(&airflow_url_str).unwrap();

    let proxy = Arc::new(DashboardApi::new(AirflowClient::new(airflow_url)));
    /*
    match proxy.get_rollout_data().await {
        Ok(rollouts) => {
            for rollout in rollouts.iter() {
                println!("\n{:#?}", rollout)
            }
        }
        Err(e) => println!("{:?}", e),
    };*/

    //exit(0);

    let app = Router::new().route("/api/v1/rollouts", get(handler).with_state(proxy));
    let addr: SocketAddr = backend_host.parse().unwrap();

    axum_server::bind(addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}
