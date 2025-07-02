use crate::live_state::{AirflowStateSyncer, CurrentState, Live};
use async_stream::try_stream;
use axum::extract::Path;
use axum::extract::Query;
use axum::http::StatusCode;
use axum::response::Sse;
use axum::response::sse;
use axum::routing::get;
use axum::{Json, Router};
use futures::stream::Stream;
use log::debug;
use rollout_dashboard::airflow_client::AirflowClient;
use rollout_dashboard::airflow_client::AirflowError;
use rollout_dashboard::airflow_client::DagRunsResponseItem;
use rollout_dashboard::types::v2::StateResponse;
use rollout_dashboard::types::{
    unstable, v1,
    v2::{DeletedRollout, Error as SError, Rollout, State as SOK, sse as SSE},
};
use serde::{Deserialize, Deserializer, de};
use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::Infallible;
use std::error::Error;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use tokio::time::Duration;

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

pub(crate) struct ApiServer {
    state_syncer: Arc<AirflowStateSyncer<Live>>,
    airflow_api: Arc<AirflowClient>,
}

impl ApiServer {
    pub fn new(
        state_syncer: Arc<AirflowStateSyncer<Live>>,
        airflow_api: Arc<AirflowClient>,
    ) -> Self {
        Self {
            state_syncer,
            airflow_api,
        }
    }

    // #[debug_handler]
    async fn get_rollout_data(&self) -> Result<Json<VecDeque<v1::Rollout>>, (StatusCode, String)> {
        match self.state_syncer.get_current_state().await {
            StateResponse::State(s) => Ok(Json(
                s.rollouts
                    .into_iter()
                    .filter_map(|r| r.try_into().ok())
                    .collect(),
            )),
            StateResponse::Error(SError { code, message }) => Err((code, message)),
        }
    }

    // #[debug_handler]
    async fn get_engine_state(&self) -> Result<Json<v1::RolloutEngineState>, (StatusCode, String)> {
        match self.state_syncer.get_current_state().await {
            StateResponse::State(s) => Ok(Json(s.rollout_engine_states.into())),
            StateResponse::Error(SError { code, message }) => Err((code, message)),
        }
    }

    // #[debug_handler]
    async fn get_cache(
        &self,
    ) -> Result<Json<Vec<unstable::FlowCacheResponse>>, (StatusCode, String)> {
        Ok(Json(self.state_syncer.get_cache().await))
    }

    async fn get_state(&self) -> Result<Json<SOK>, (StatusCode, String)> {
        match self.state_syncer.get_current_state().await {
            StateResponse::State(s) => Ok(Json(s)),
            StateResponse::Error(SError { code, message }) => Err((code, message)),
        }
    }

    async fn get_dag_run(
        &self,
        dag_id: &str,
        dag_run_id: &str,
    ) -> Result<Json<DagRunsResponseItem>, (StatusCode, String)> {
        Ok(Json(
            match self.airflow_api.dag_run(dag_id, dag_run_id).await {
                Ok(val) => val,
                Err(e) => {
                    return Err(match e {
                        AirflowError::StatusCode(c) => (c, "Internal server error".to_string()),
                        AirflowError::ReqwestError(err) => {
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
                        AirflowError::Other(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
                    });
                }
            },
        ))
    }

    /// Given an initial state and a final state, produce
    /// zero or more SSE messages representing the state changes.
    ///
    /// When rollout engine states change, and delta support
    /// is enabled, all rollout states are returned in the delta
    /// message, not just one.
    fn produce_sse_messages(
        self: Arc<Self>,
        current_rollout_data: &StateResponse,
        last_rollout_data: &Option<StateResponse>,
        delta_support: bool,
    ) -> Vec<SSE::Message> {
        match (&current_rollout_data, &last_rollout_data, delta_support) {
            // First sync or error before.  Send full sync.
            (StateResponse::State(state), None | Some(StateResponse::Error(_)), _) => {
                vec![SSE::Message::CompleteState(state.clone())]
            }
            // Error after a good update.  Send error.
            (StateResponse::Error(e), None | Some(StateResponse::State(_)), _) => {
                vec![SSE::Message::Error(e.clone())]
            }
            // Error after an error.  Only send update if errors differ.
            (StateResponse::Error(e), Some(StateResponse::Error(olde)), _) => {
                if e == olde {
                    vec![]
                } else {
                    vec![SSE::Message::Error(e.clone())]
                }
            }
            // Last time was a good update, but delta support is not requested.  Send full sync.
            (
                StateResponse::State(SOK {
                    rollouts: new_rollouts,
                    rollout_engine_states: new_rollout_engine_states,
                }),
                Some(StateResponse::State(SOK { .. })),
                false,
            ) => vec![
                (SSE::Message::CompleteState(SOK {
                    rollouts: new_rollouts.clone(),
                    rollout_engine_states: new_rollout_engine_states.clone(),
                })),
            ],
            // Last time was a good update and sync is enabled.  Send differential sync.
            (
                StateResponse::State(SOK {
                    rollouts: new_rollouts,
                    rollout_engine_states: new_rollout_engine_states,
                }),
                Some(StateResponse::State(SOK {
                    rollouts: old_rollouts,
                    rollout_engine_states: old_rollout_engine_states,
                })),
                true,
            ) => {
                let new_names = new_rollouts
                    .iter()
                    .map(|r| r.key())
                    .collect::<HashSet<String>>();
                let old_rollouts_map = old_rollouts.iter().map(|r| (r.key(), r)).collect::<HashMap<
                    String,
                    &Rollout,
                >>(
                );
                let updated = new_rollouts
                    .iter()
                    .filter_map(|r| match old_rollouts_map.get(&r.key()) {
                        None => Some(r.clone()),
                        Some(old_rollout) => match r.update_count != old_rollout.update_count {
                            true => Some(r.clone()),
                            false => None,
                        },
                    })
                    .collect::<VecDeque<Rollout>>();
                let deleted = old_rollouts
                    .iter()
                    .filter_map(|r| match new_names.contains(&r.key()) {
                        true => None,
                        false => Some(DeletedRollout {
                            kind: r.kind(),
                            name: r.name.clone(),
                        }),
                    })
                    .collect::<VecDeque<DeletedRollout>>();
                let mut ret = vec![];
                if !updated.is_empty() || !deleted.is_empty() {
                    ret.push(SSE::Message::RolloutsDelta(SSE::RolloutsDelta {
                        updated,
                        deleted,
                    }))
                }
                if new_rollout_engine_states != old_rollout_engine_states {
                    ret.push(SSE::Message::RolloutEngineStatesUpdate(
                        new_rollout_engine_states.clone(),
                    ))
                }
                ret
            }
        }
    }

    pub fn compat_convert_v2_sse_state_to_v1(
        self: &Arc<Self>,
        message: SSE::Message,
        last_engine_state: &mut v1::RolloutEngineState,
    ) -> v1::DeltaState {
        match message {
            SSE::Message::CompleteState(SOK {
                rollouts,
                rollout_engine_states,
            }) => {
                let rollouts: VecDeque<v1::Rollout> = rollouts
                    .into_iter()
                    .filter_map(|r| v1::Rollout::try_from(r).ok())
                    .collect();
                let engine_state = v1::RolloutEngineState::from(rollout_engine_states);
                *last_engine_state = engine_state.clone();
                v1::DeltaState::full(&engine_state, &rollouts)
            }
            SSE::Message::Error(SError { code, message }) => {
                v1::DeltaState::error(&(code, message))
            }
            SSE::Message::RolloutsDelta(SSE::RolloutsDelta { updated, deleted }) => {
                let updated: VecDeque<v1::Rollout> = updated
                    .into_iter()
                    .filter_map(|r| v1::Rollout::try_from(r).ok())
                    .collect();
                let deleted: VecDeque<String> = deleted
                    .into_iter()
                    .filter(|r| r.kind == "rollout_ic_os_to_mainnet_subnets")
                    .map(|r| r.name)
                    .collect();
                v1::DeltaState::partial(&last_engine_state.clone(), &updated, &deleted)
            }
            SSE::Message::RolloutEngineStatesUpdate(rollout_engine_states) => {
                let engine_state = match rollout_engine_states.is_empty() {
                    true => last_engine_state.clone(),
                    false => {
                        // This depends on the delta message
                        // returning all rollout engine states,
                        // not just a few.  Thankfully, the
                        // function which computes this message
                        // does just that.
                        let x = v1::RolloutEngineState::from(rollout_engine_states);
                        *last_engine_state = x.clone();
                        x
                    }
                };
                let updated = VecDeque::new();
                let deleted = VecDeque::new();
                v1::DeltaState::partial(&engine_state, &updated, &deleted)
            }
        }
    }

    pub fn compat_stream_state(
        self: Arc<Self>,
        delta_support: bool,
    ) -> Sse<impl Stream<Item = Result<sse::Event, Infallible>>> {
        let mut subscription = self.state_syncer.subscribe_to_state_updates();

        let stream = {
            try_stream! {
                // Set up something that will be dropped (thus log) when SSE is disconnected.
                let disconnection_guard = DisconnectionGuard::default();

                // Set an initial message to diff the first broadcast message against.
                let mut last_rollout_data: Option<CurrentState> = None;
                let mut last_engine_state: v1::RolloutEngineState = v1::RolloutEngineState::Active;

                loop {
                    let current_rollout_data: CurrentState = subscription.borrow_and_update().clone();
                    let messages: Vec<SSE::Message> = self.clone().produce_sse_messages(&current_rollout_data, &last_rollout_data, delta_support);
                    for message in messages.into_iter() {
                        let mm = self.compat_convert_v2_sse_state_to_v1(message, &mut last_engine_state);
                        yield sse::Event::default().json_data(&mm).unwrap()
                    }
                    last_rollout_data = Some(current_rollout_data);
                    if subscription.changed().await.is_err() {
                        break;
                    }
                }

                // Drop the disconnection guard to log the message that the client disconnected.
                drop(disconnection_guard);
            }
        };

        Sse::new(stream).keep_alive(
            axum::response::sse::KeepAlive::new()
                .interval(Duration::from_secs(5))
                .text("keepalive"),
        )
    }

    pub fn stream_state(
        self: Arc<Self>,
        delta_support: bool,
    ) -> Sse<impl Stream<Item = Result<sse::Event, Infallible>>> {
        let mut subscription = self.state_syncer.subscribe_to_state_updates();

        let stream = {
            try_stream! {
                // Set up something that will be dropped (thus log) when SSE is disconnected.
                let disconnection_guard = DisconnectionGuard::default();

                // Set an initial message to diff the first broadcast message against.
                let mut last_rollout_data: Option<CurrentState> = None;

                loop {
                    let current_rollout_data: CurrentState = subscription.borrow_and_update().clone();
                    let messages = self.clone().produce_sse_messages(&current_rollout_data, &last_rollout_data, delta_support);
                    for message in messages.iter() {
                        match message {
                            SSE::Message::CompleteState(sok) => {
                                yield sse::Event::default().event("State").json_data(sok).unwrap();
                            }
                            SSE::Message::Error(serr) => {
                                yield sse::Event::default().event("Error").json_data(serr).unwrap();
                            }
                            SSE::Message::RolloutsDelta(sdelta) => {
                                yield sse::Event::default().event("RolloutsDelta").json_data(sdelta).unwrap();
                            }
                            SSE::Message::RolloutEngineStatesUpdate(sdelta) => {
                                yield sse::Event::default().event("RolloutEngineStates").json_data(sdelta).unwrap();
                            }
                        }
                    }
                    last_rollout_data = Some(current_rollout_data);
                    if subscription.changed().await.is_err() {
                        break;
                    }
                }

                // Drop the disconnection guard to log the message that the client disconnected.
                drop(disconnection_guard);
            }
        };

        Sse::new(stream).keep_alive(
            axum::response::sse::KeepAlive::new()
                .interval(Duration::from_secs(5))
                .text("keepalive"),
        )
    }

    fn v1_api(self: Arc<Self>) -> Router {
        let rollouts_handler_ref = self.clone();
        let engine_state_handler_ref = self.clone();
        let compat_sse_handler_ref = self.clone();
        let sse_handler_ref = self.clone();
        Router::new()
            .route(
                "/rollouts",
                get(move || async move { rollouts_handler_ref.get_rollout_data().await }),
            )
            .route(
                "/engine_state",
                get(move || async move { engine_state_handler_ref.get_engine_state().await }),
            )
            .route(
                "/rollouts/sse",
                get(move |options: Query<SseHandlerParameters>| {
                    let options: SseHandlerParameters = options.0;
                    async move {
                        compat_sse_handler_ref
                            .compat_stream_state(options.incremental.unwrap_or_default())
                    }
                }),
            )
            .route(
                "/sse/rollouts_view",
                get(move || async move { sse_handler_ref.compat_stream_state(true) }),
            )
    }

    fn v2_api(self: Arc<Self>) -> Router {
        let state_handler_ref = self.clone();
        let sse_handler_ref = self.clone();
        Router::new()
            .route(
                "/state",
                get(move || async move { state_handler_ref.get_state().await }),
            )
            .route(
                "/sse",
                get(move |options: Query<SseHandlerParameters>| {
                    let options: SseHandlerParameters = options.0;
                    async move { sse_handler_ref.stream_state(options.incremental.unwrap_or(true)) }
                }),
            )
    }

    fn unstable_api(self: Arc<Self>) -> Router {
        let cached_data_handler_ref = self.clone();
        let get_dag_run_handler_ref = self.clone();

        Router::new()
            .route(
                "/cache",
                get(move || async move { cached_data_handler_ref.get_cache().await }),
            )
            .route(
                "/dags",
                get(move || async move { "Result is great".to_string() }),
            )
            .route(
                "/dags/:dag_id/dag_runs/:dag_run_id",
                get(
                    move |Path((dag_id, dag_run_id)): Path<(String, String)>| async move {
                        get_dag_run_handler_ref
                            .get_dag_run(dag_id.as_str(), dag_run_id.as_str())
                            .await
                    },
                ),
            )
    }

    pub fn routes(self: Arc<Self>) -> Router {
        Router::new()
            .nest("/api/v1", self.clone().v1_api())
            .nest("/api/v2", self.clone().v2_api())
            .nest("/api/unstable", self.unstable_api())
    }
}
