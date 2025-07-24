use crate::live_state::HostOsRolloutBatchStateRequestError;
use crate::live_state::{AirflowStateSyncer, Live, SyncCycleState};
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
use rollout_dashboard::types::{unstable, v1, v2};
use serde::{Deserialize, Deserializer, de};
use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::Infallible;
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

macro_rules! http_error {
    ($code:expr) => {{
        Err((code, format!("{}", code)))
    }};
    ($code:expr, $str:expr) => {{
        Err(($code, format!("{}", $str)))
    }};
    ($code:expr, $str:expr, $($arg:tt)*) => {{
        Err(($code, format!($str, $($arg)*)))
    }};
}

macro_rules! bad_request {
    ($($arg:tt)*) => {{ http_error!(StatusCode::BAD_REQUEST, $($arg)*) }};
}

macro_rules! internal_server_error {
    ($($arg:tt)*) => {{ http_error!(StatusCode::INTERNAL_SERVER_ERROR, $($arg)*) }};
}

macro_rules! not_found {
    ($($arg:tt)*) => {{ http_error!(StatusCode::NOT_FOUND, $($arg)*) }};
}

macro_rules! no_content {
    ($($arg:tt)*) => {{ http_error!(StatusCode::NO_CONTENT, "") }};
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

macro_rules! handle {
    ($self:ident, $ident:ident, $block:block) => {{
        let $ident = $self.clone();
        move || async move { $block }
    }};
    ($self:ident, $ident:ident, |$parms:ident($($arg:tt)*): $type:ty|, $block:block) => {{
        let $ident = $self.clone();
        move |$parms($($arg)*): $type| async move { $block }
    }};
    ($self:ident, $ident:ident, |$parms:ident: $type:ty|, $block:block) => {{
        let $ident = $self.clone();
        move |$parms: $type| async move { $block }
    }};
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

    /// Given an initial state and a final state, produce
    /// zero or more SSE messages representing the state changes.
    ///
    /// When rollout engine states change, and delta support
    /// is enabled, all rollout states are returned in the delta
    /// message, not just one.
    fn produce_sse_messages(
        self: Arc<Self>,
        current_rollout_data: &SyncCycleState,
        last_rollout_data: &SyncCycleState,
        delta_support: bool,
    ) -> Vec<v2::sse::Message> {
        match (current_rollout_data, last_rollout_data, delta_support) {
            // First sync or error before.  Send full sync.
            (SyncCycleState::Initial, _, _) => {
                vec![]
            }
            (
                SyncCycleState::State(state),
                SyncCycleState::Initial | SyncCycleState::Error(_),
                _,
            ) => {
                vec![v2::sse::Message::CompleteState(state.clone())]
            }
            // Error after a good update.  Send error.
            (SyncCycleState::Error(e), SyncCycleState::Initial | SyncCycleState::State(_), _) => {
                vec![v2::sse::Message::Error(e.clone().into())]
            }
            // Error after an error.  Only send update if errors differ.
            (SyncCycleState::Error(e), SyncCycleState::Error(olde), _) => {
                let olde: v2::Error = olde.clone().into();
                let e: v2::Error = e.clone().into();
                if e == olde {
                    vec![]
                } else {
                    vec![v2::sse::Message::Error(e)]
                }
            }
            // Last time was a good update, but delta support is not requested.  Send full sync.
            (
                SyncCycleState::State(v2::State {
                    rollouts: new_rollouts,
                    rollout_engine_states: new_rollout_engine_states,
                }),
                SyncCycleState::State(_),
                false,
            ) => vec![
                (v2::sse::Message::CompleteState(v2::State {
                    rollouts: new_rollouts.clone(),
                    rollout_engine_states: new_rollout_engine_states.clone(),
                })),
            ],
            // Last time was a good update and sync is enabled.  Send differential sync.
            (
                SyncCycleState::State(v2::State {
                    rollouts: new_rollouts,
                    rollout_engine_states: new_rollout_engine_states,
                }),
                SyncCycleState::State(v2::State {
                    rollouts: old_rollouts,
                    rollout_engine_states: old_rollout_engine_states,
                }),
                true,
            ) => {
                let new_names = new_rollouts
                    .iter()
                    .map(|r| r.key())
                    .collect::<HashSet<String>>();
                let old_rollouts_map = old_rollouts.iter().map(|r| (r.key(), r)).collect::<HashMap<
                    String,
                    &v2::Rollout,
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
                    .collect::<VecDeque<v2::Rollout>>();
                let deleted = old_rollouts
                    .iter()
                    .filter_map(|r| match new_names.contains(&r.key()) {
                        true => None,
                        false => Some(v2::DeletedRollout {
                            kind: r.kind(),
                            name: r.name.clone(),
                        }),
                    })
                    .collect::<VecDeque<v2::DeletedRollout>>();
                let mut ret = vec![];
                if !updated.is_empty() || !deleted.is_empty() {
                    ret.push(v2::sse::Message::RolloutsDelta(v2::sse::RolloutsDelta {
                        updated,
                        deleted,
                    }))
                }
                if new_rollout_engine_states != old_rollout_engine_states {
                    ret.push(v2::sse::Message::RolloutEngineStatesUpdate(
                        new_rollout_engine_states.clone(),
                    ))
                }
                ret
            }
        }
    }

    fn compat_convert_v2_sse_state_to_v1(
        self: &Arc<Self>,
        message: v2::sse::Message,
        last_engine_state: &mut v1::RolloutEngineState,
    ) -> v1::DeltaState {
        match message {
            v2::sse::Message::CompleteState(v2::State {
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
            v2::sse::Message::Error(v2::Error { code, message }) => {
                v1::DeltaState::error(&(code, message))
            }
            v2::sse::Message::RolloutsDelta(v2::sse::RolloutsDelta { updated, deleted }) => {
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
            v2::sse::Message::RolloutEngineStatesUpdate(rollout_engine_states) => {
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

    /* V1 API */

    // #[debug_handler]
    async fn get_rollout_data(&self) -> Result<Json<VecDeque<v1::Rollout>>, (StatusCode, String)> {
        match self.state_syncer.get_cycle_state().await {
            SyncCycleState::Initial => no_content!(),
            SyncCycleState::State(s) => Ok(Json(
                s.rollouts
                    .into_iter()
                    .filter_map(|r| r.try_into().ok())
                    .collect(),
            )),
            SyncCycleState::Error(err) => Err(err.into()),
        }
    }

    // #[debug_handler]
    async fn get_engine_state(&self) -> Result<Json<v1::RolloutEngineState>, (StatusCode, String)> {
        match self.state_syncer.get_cycle_state().await {
            SyncCycleState::Initial => no_content!(),
            SyncCycleState::State(s) => Ok(Json(s.rollout_engine_states.into())),
            SyncCycleState::Error(err) => Err(err.into()),
        }
    }

    pub fn stream_state_v1(
        self: Arc<Self>,
        delta_support: bool,
    ) -> Sse<impl Stream<Item = Result<sse::Event, Infallible>>> {
        let mut subscription = self.state_syncer.subscribe_to_state_updates();

        let stream = {
            try_stream! {
                // Set up something that will be dropped (thus log) when SSE is disconnected.
                let disconnection_guard = DisconnectionGuard::default();

                // Set an initial message to diff the first broadcast message against.
                let mut last_rollout_data: SyncCycleState = SyncCycleState::Initial;
                let mut last_engine_state: v1::RolloutEngineState = v1::RolloutEngineState::Active;

                loop {
                    let current_rollout_data: SyncCycleState = subscription.borrow_and_update().clone();
                    let messages: Vec<v2::sse::Message> = self.clone().produce_sse_messages(&current_rollout_data, &last_rollout_data, delta_support);
                    for message in messages.into_iter() {
                        let mm = self.compat_convert_v2_sse_state_to_v1(message, &mut last_engine_state);
                        yield sse::Event::default().json_data(&mm).unwrap()
                    }
                    last_rollout_data = current_rollout_data;
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

    /* V2 API */

    async fn get_state(&self) -> Result<Json<v2::State>, (StatusCode, String)> {
        match self.state_syncer.get_cycle_state().await {
            SyncCycleState::Initial => no_content!(),
            SyncCycleState::State(s) => Ok(Json(s)),
            SyncCycleState::Error(e) => Err(e.into()),
        }
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
                let mut last_rollout_data = SyncCycleState::Initial;

                loop {
                    let current_rollout_data: SyncCycleState = subscription.borrow_and_update().clone();
                    let messages = self.clone().produce_sse_messages(&current_rollout_data, &last_rollout_data, delta_support);
                    for message in messages.iter() {
                        match message {
                            v2::sse::Message::CompleteState(sok) => {
                                yield sse::Event::default().event("State").json_data(sok).unwrap();
                            }
                            v2::sse::Message::Error(serr) => {
                                yield sse::Event::default().event("Error").json_data(serr).unwrap();
                            }
                            v2::sse::Message::RolloutsDelta(sdelta) => {
                                yield sse::Event::default().event("RolloutsDelta").json_data(sdelta).unwrap();
                            }
                            v2::sse::Message::RolloutEngineStatesUpdate(sdelta) => {
                                yield sse::Event::default().event("RolloutEngineStates").json_data(sdelta).unwrap();
                            }
                        }
                    }
                    last_rollout_data = current_rollout_data;
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

    /* Unstable API */

    async fn get_cache(
        &self,
    ) -> Result<Json<Vec<unstable::FlowCacheResponse>>, (StatusCode, String)> {
        Ok(Json(self.state_syncer.get_cache().await))
    }

    async fn get_internal_state(&self) -> Result<Json<SyncCycleState>, Infallible> {
        Ok(Json(self.state_syncer.get_cycle_state().await))
    }

    async fn get_dag_run(
        &self,
        dag_id: &str,
        dag_run_id: &str,
    ) -> Result<Json<unstable::DagRunsResponseItem>, (StatusCode, String)> {
        Ok(Json(
            match self.airflow_api.dag_run(dag_id, dag_run_id).await {
                Ok(val) => val,
                Err(e) => return Err(e.into()),
            },
        ))
    }

    async fn get_hostos_rollout_batch_state(
        &self,
        dag_run_id: &str,
        stage_name: &str,
        batch_number: usize,
    ) -> Result<Json<unstable::HostOsRolloutBatchStateResponse>, (StatusCode, String)> {
        match self
            .state_syncer
            .get_hostos_rollout_batch_state(dag_run_id, stage_name, batch_number)
            .await
        {
            Err(HostOsRolloutBatchStateRequestError::NotYetSynced) => no_content!(),
            Err(HostOsRolloutBatchStateRequestError::NoDataForDagRun) => {
                not_found!("Supplied DAG run ID not found.")
            }

            Err(HostOsRolloutBatchStateRequestError::NoDataForBatchNumber) => {
                not_found!("No data for supplied batch number.")
            }
            Err(HostOsRolloutBatchStateRequestError::InvalidDagID) => {
                bad_request!("Invalid DAG ID.")
            }
            Err(HostOsRolloutBatchStateRequestError::InvalidDagRunID) => {
                bad_request!("Invalid DAG run ID.")
            }
            Err(HostOsRolloutBatchStateRequestError::InvalidStageName) => {
                bad_request!("Invalid stage name.")
            }
            Err(HostOsRolloutBatchStateRequestError::InvalidBatchNumber) => {
                bad_request!("Invalid batch number.")
            }
            Err(HostOsRolloutBatchStateRequestError::InvalidPlanData) => {
                internal_server_error!("Invalid plan data found for batch.")
            }
            Err(HostOsRolloutBatchStateRequestError::RolloutDataGatherError(err)) => {
                Err(err.into())
            }
            Ok(data) => Ok(Json(data)),
        }
    }

    pub fn routes(self: Arc<Self>) -> Router {
        Router::new()
            .nest("/api/v1", Router::new()
                // V1 API
                .route(
                    "/rollouts",
                    get(handle!(self, s, { s.get_rollout_data().await })),
                )
                .route(
                    "/engine_state",
                    get(handle!(self, s, { s.get_engine_state().await })),
                )
                .route(
                    "/rollouts/sse",
                    get(handle!(self, s, |options: Query<SseHandlerParameters>|, {
                        let options: SseHandlerParameters = options.0;
                        s.stream_state_v1(options.incremental.unwrap_or_default())
                    })),
                )
                .route(
                    "/sse/rollouts_view",
                    get(handle!(self, s, { s.stream_state_v1(true) })),
                )
            )
            .nest("/api/v2", Router::new()
                // V2 API
                .route("/state", get(handle!(self, s, { s.get_state().await })))
                .route(
                    "/sse",
                    get(handle!(self, s, |options: Query<SseHandlerParameters>|, {
                        let options: SseHandlerParameters = options.0;
                        s.stream_state(options.incremental.unwrap_or(true))
                    })),
                )
            )
            .nest(
                // unstable API
                "/api/unstable",
                Router::new().route(
                    "/cache2",
                    get(handle!(self, s, { s.get_cache().await} ))
                )
                .route(
                    "/cache",
                    get(handle!(self, s, { s.get_cache().await }))
                )
                .route(
                    "/internal_state",
                    get(handle!(self, s, { s.get_internal_state().await }))
                )
                .route(
                    "/dags/:dag_id/dag_runs/:dag_run_id",
                    get(handle!(self, s, |Path((dag_id, dag_run_id)): Path<(String, String)>|, {
                        s
                            .get_dag_run(dag_id.as_str(), dag_run_id.as_str())
                            .await
                    }))
                )
                .route(
                    "/rollouts/rollout_ic_os_to_mainnet_nodes/:dag_run_id/stages/:stage_name/batches/:batch_number",
                    get(handle!(self, s, |Path((dag_run_id, stage_name, batch_number)): Path<(String, String, usize)>|, {
                        s
                            .get_hostos_rollout_batch_state(dag_run_id.as_str(), stage_name.as_str(), batch_number)
                            .await
                    }))
                )
            )
    }
}
