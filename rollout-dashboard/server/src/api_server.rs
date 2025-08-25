use crate::live_state::SuccessfulSyncCycleState;
use crate::live_state::{
    AirflowStateSyncer, Live, Parser, RolloutState, RolloutStates, SyncCycleState, SyncerState,
};
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
use serde::{Deserialize, Deserializer, Serialize, de};
use std::collections::{HashSet, VecDeque};
use std::convert::Infallible;
use std::fmt;
use std::iter::Iterator;
use std::num::NonZero;
use std::str::FromStr;
use std::sync::Arc;
use tokio::time::Duration;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::WatchStream;

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

macro_rules! no_content {
    ($($arg:tt)*) => {{ http_error!(StatusCode::NO_CONTENT, "") }};
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

macro_rules! sse {
    ($ex:tt) => {
        Sse::new({
            try_stream! {
                // Set up something that will be dropped (thus log) when SSE is disconnected.
                let guard = DisconnectionGuard::default();
                $ex
                // Drop the guard, logging the disconnection.
                drop(guard)
            }
        })
        .keep_alive(
            axum::response::sse::KeepAlive::new()
                .interval(Duration::from_secs(5))
                .text("keepalive"),
        )
    };
}

#[derive(Deserialize)]
struct SseHandlerParameters {
    #[serde(default, deserialize_with = "empty_value_as_true")]
    incremental: Option<bool>,
}

pub(crate) struct ApiServer {
    state_syncer: Arc<AirflowStateSyncer<Live>>,
    airflow_api: Arc<AirflowClient>,
    unstable_api_enabled: bool,
}

macro_rules! handle {
    ($self:ident, $ident:ident, $block:block) => {{
        let $ident = $self.clone();
        move || async move { $block }
    }};
    ($self:ident, $ident:ident, |$parms:ident($($arg:tt)*): $type:ty, $parms2:ident($($arg2:tt)*): $type2:ty|, $block:block) => {{
        let $ident = $self.clone();
        move |$parms($($arg)*): $type, $parms2($($arg2)*): $type2| async move { $block }
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

#[derive(Serialize)]
struct InternalState {
    cycle_state: SyncCycleState,
    rollout_states: RolloutStates,
}

impl ApiServer {
    pub fn new(
        state_syncer: Arc<AirflowStateSyncer<Live>>,
        airflow_api: Arc<AirflowClient>,
        enable_unstable_api: bool,
    ) -> Self {
        Self {
            state_syncer,
            airflow_api,
            unstable_api_enabled: enable_unstable_api,
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
                vec![v2::sse::Message::Error(v2::Error {
                    code: StatusCode::NO_CONTENT,
                    message: "Backend has not yet fetched rollouts".to_string(),
                    permanent: false,
                })]
            }
            (
                SyncCycleState::Successful(state),
                SyncCycleState::Initial | SyncCycleState::Error(_),
                _,
            ) => {
                vec![v2::sse::Message::CompleteState(state.clone().into())]
            }
            // Error after a good update.  Send error.
            (
                SyncCycleState::Error(e),
                SyncCycleState::Initial | SyncCycleState::Successful(_),
                _,
            ) => {
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
            (SyncCycleState::Successful(state), SyncCycleState::Successful(_), false) => {
                vec![(v2::sse::Message::CompleteState(state.clone().into()))]
            }
            // Last time was a good update and sync is enabled.  Send differential sync.
            (
                SyncCycleState::Successful(SuccessfulSyncCycleState {
                    rollouts: new_rollouts,
                    rollout_engine_states: new_rollout_engine_states,
                }),
                SyncCycleState::Successful(SuccessfulSyncCycleState {
                    rollouts: old_rollouts,
                    rollout_engine_states: old_rollout_engine_states,
                }),
                true,
            ) => {
                let new_names = new_rollouts
                    .clone()
                    .into_iter()
                    .map(|(k, _)| k)
                    .collect::<HashSet<(v2::DagID, v2::DagRunID)>>();
                let updated = new_rollouts
                    .clone()
                    .into_iter()
                    .filter_map(|(k, r)| match old_rollouts.get(&k) {
                        None => Some(r.clone()),
                        Some(old_rollout) => match r.update_count != old_rollout.update_count {
                            true => Some(r.clone()),
                            false => None,
                        },
                    })
                    .collect::<VecDeque<v2::Rollout>>();
                let deleted = old_rollouts
                    .clone()
                    .into_iter()
                    .filter_map(|(k, r)| match new_names.contains(&k) {
                        true => None,
                        false => Some(v2::DeletedRollout {
                            kind: r.kind().to_string(),
                            name: r.name.to_string(),
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
            v2::sse::Message::Error(v2::Error {
                code,
                message,
                permanent: _,
            }) => v1::DeltaState::error(&(code, message)),
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
        match &self.state_syncer.get_state().await.cycle_state {
            SyncCycleState::Initial => no_content!(),
            SyncCycleState::Successful(s) => Ok(Json(
                s.rollouts
                    .clone()
                    .into_iter()
                    .filter_map(|(_, r)| r.try_into().ok())
                    .collect(),
            )),
            SyncCycleState::Error(err) => Err(err.clone().into()),
        }
    }

    // #[debug_handler]
    async fn get_engine_state(&self) -> Result<Json<v1::RolloutEngineState>, (StatusCode, String)> {
        match &self.state_syncer.get_state().await.cycle_state {
            SyncCycleState::Initial => no_content!(),
            SyncCycleState::Successful(s) => Ok(Json(s.rollout_engine_states.clone().into())),
            SyncCycleState::Error(err) => Err(err.clone().into()),
        }
    }

    async fn stream_state_v1(
        self: Arc<Self>,
        delta_support: bool,
    ) -> Sse<impl Stream<Item = Result<sse::Event, Infallible>>> {
        let mut watcher = WatchStream::new(self.state_syncer.subscribe_to_syncer_updates());
        // Set an initial message to diff the first broadcast message against.
        let mut last_rollout_data: SyncCycleState = SyncCycleState::Initial;
        let mut last_engine_state: v1::RolloutEngineState = v1::RolloutEngineState::Active;

        sse!({
            // Set up something that will be dropped (thus log) when SSE is disconnected.
            while let Some(new) = watcher.next().await {
                let current_rollout_data = new.cycle_state.clone();
                let messages: Vec<v2::sse::Message> = self.clone().produce_sse_messages(
                    &current_rollout_data,
                    &last_rollout_data,
                    delta_support,
                );
                for message in messages.into_iter() {
                    let mm =
                        self.compat_convert_v2_sse_state_to_v1(message, &mut last_engine_state);
                    yield sse::Event::default().json_data(&mm).unwrap()
                }
                last_rollout_data = current_rollout_data;
            }
        })
    }

    /* V2 API */

    async fn get_state(&self) -> Result<Json<v2::State>, (StatusCode, String)> {
        match &self.state_syncer.get_state().await.cycle_state {
            SyncCycleState::Initial => no_content!(),
            SyncCycleState::Successful(s) => Ok(Json(s.clone().into())),
            SyncCycleState::Error(e) => Err(e.clone().into()),
        }
    }

    fn stream_state(
        self: Arc<Self>,
        delta_support: bool,
    ) -> Sse<impl Stream<Item = Result<sse::Event, Infallible>>> {
        // Set up something that will be dropped (thus log) when SSE is disconnected.
        // Stream status updates.
        let mut stream = WatchStream::new(self.state_syncer.subscribe_to_syncer_updates());
        // Set an initial message to diff the first broadcast message against.
        let mut last_rollout_data = SyncCycleState::Initial;

        sse!({
            while let Some(new) = stream.next().await {
                let current_rollout_data: SyncCycleState = new.cycle_state.clone();
                let messages = self.clone().produce_sse_messages(
                    &current_rollout_data,
                    &last_rollout_data,
                    delta_support,
                );
                for message in messages.iter() {
                    let (interrupt, event) = message.into();
                    yield event;
                    if interrupt {
                        break;
                    }
                }
                last_rollout_data = current_rollout_data;
            }
        })
    }

    /* Unstable API */

    async fn get_cache(
        &self,
    ) -> Result<Json<Vec<unstable::FlowCacheResponse>>, (StatusCode, String)> {
        let cache = self.state_syncer.get_state().await;
        let mut result: Vec<unstable::FlowCacheResponse> = cache
            .rollout_states
            .iter()
            .map(|(_, v)| (v).into())
            .collect();
        drop(cache);
        result.sort_by_key(|v| v.dispatch_time);
        result.reverse();
        Ok(Json(result))
    }

    async fn get_internal_state(&self) -> Result<Json<InternalState>, Infallible> {
        let state = self.state_syncer.get_state().await;
        let cycle_state = state.cycle_state.clone();
        let rollout_states = state.rollout_states.clone();
        let internal_state = InternalState {
            cycle_state,
            rollout_states,
        };
        Ok(Json(internal_state))
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

    async fn get_xcom_entry(
        &self,
        dag_id: &str,
        dag_run_id: &str,
        task_instance_id: &str,
        map_index: Option<usize>,
        xcom_key: &str,
    ) -> Result<Json<unstable::XComEntryResponse>, (StatusCode, String)> {
        Ok(Json(
            match self
                .airflow_api
                .xcom_entry(dag_id, dag_run_id, task_instance_id, map_index, xcom_key)
                .await
            {
                Ok(val) => val,
                Err(e) => return Err(e.into()),
            },
        ))
    }

    fn get_hostos_rollout_batch(
        &self,
        dag_run_id: &str,
        stage_name: &str,
        batch_number: usize,
        syncer_state: Arc<SyncerState>,
    ) -> Result<v2::hostos::BatchResponse, v2::hostos::BatchError> {
        if !(1..=100).contains(&batch_number) {
            return Err(v2::hostos::BatchError::InvalidBatchNumber);
        }
        let nonzero_batch_number = NonZero::new(batch_number).unwrap();
        let stage = v2::hostos::StageName::from_str(stage_name)
            .map_err(|_| v2::hostos::BatchError::InvalidStageName)?;

        let dag_id = v2::DagID::RolloutIcOsToMainnetNodes;
        let dag_run_id = v2::DagRunID::from_str(dag_run_id)
            .map_err(|_| v2::hostos::BatchError::InvalidDagRunID)?;

        match (
            syncer_state
                .rollout_states
                .get(&(dag_id.clone(), dag_run_id.clone())),
            match &syncer_state.cycle_state {
                SyncCycleState::Initial => Err(v2::hostos::BatchError::NotYetSynced),
                SyncCycleState::Error(e) => Err(v2::hostos::BatchError::RolloutDataGatherError(
                    e.to_string(),
                )),
                SyncCycleState::Successful(SuccessfulSyncCycleState { rollouts, .. }) => {
                    Ok(rollouts)
                }
            }?
            .get(&(dag_id, dag_run_id)),
        ) {
            (
                Some(RolloutState {
                    parser: Parser::Nodes(parser),
                    ..
                }),
                Some(v2::Rollout {
                    kind: v2::RolloutKind::RolloutIcOsToMainnetNodes(kind),
                    ..
                }),
            ) => {
                let rollout_finished = matches!(
                    kind.state,
                    v2::hostos::State::Complete | v2::hostos::State::Failed
                );
                let stage = match (stage, &parser.stages) {
                    (v2::hostos::StageName::Canary, Some(stages)) => Ok(&stages.canary),
                    (v2::hostos::StageName::Main, Some(stages)) => Ok(&stages.main),
                    (v2::hostos::StageName::Unassigned, Some(stages)) => Ok(&stages.unassigned),
                    (v2::hostos::StageName::Stragglers, Some(stages)) => Ok(&stages.stragglers),
                    (_, None) => Err(if rollout_finished {
                        v2::hostos::BatchError::NoPlanData
                    } else {
                        v2::hostos::BatchError::NoPlanDataYet
                    }),
                }?;
                match stage.get(&nonzero_batch_number) {
                    Some(n) => Ok(n.clone()),
                    None => Err(if rollout_finished {
                        v2::hostos::BatchError::NoSuchBatch
                    } else {
                        v2::hostos::BatchError::NoBatchDataYet
                    }),
                }
            }
            (Some(_), _) | (_, Some(_)) => Err(v2::hostos::BatchError::WrongDagRunKind),
            _ => Err(v2::hostos::BatchError::NoSuchDagRun),
        }
    }

    async fn stream_get_hostos_rollout_batch(
        self: Arc<Self>,
        dag_run_id: String,
        stage_name: String,
        batch_number: usize,
    ) -> Sse<impl Stream<Item = Result<sse::Event, Infallible>>> {
        #[derive(PartialEq, Eq)]
        struct HostOsBatchResult(Result<v2::hostos::BatchResponse, v2::hostos::BatchError>);

        #[allow(clippy::from_over_into)]
        impl Into<HostOsBatchResult> for Result<v2::hostos::BatchResponse, v2::hostos::BatchError> {
            fn into(self) -> HostOsBatchResult {
                HostOsBatchResult(self)
            }
        }

        impl From<&HostOsBatchResult> for (bool, axum::response::sse::Event) {
            /// Transforms a Message into a pair (bool, SSE event).
            /// If the boolean is true, the caller running the SSE stream must interrupt
            /// the connection after sending the event.
            fn from(m: &HostOsBatchResult) -> (bool, axum::response::sse::Event) {
                match &m.0 {
                    Ok(sok) => (false, sok.into()),
                    Err(e) => (e.permanent(), e.into()),
                }
            }
        }

        // Stream status updates.
        let mut stream = WatchStream::new(self.state_syncer.subscribe_to_syncer_updates());
        // Set an initial message to diff the first broadcast message against.
        let mut last = HostOsBatchResult(Err(v2::hostos::BatchError::NotYetSynced));

        sse!({
            while let Some(new) = stream.next().await {
                let current = HostOsBatchResult(self.get_hostos_rollout_batch(
                    &dag_run_id,
                    &stage_name,
                    batch_number,
                    new.clone(),
                ));
                if current != last {
                    let (interrupt, event): (bool, sse::Event) = (&current).into();
                    yield event;
                    if interrupt {
                        break;
                    }
                    last = current;
                }
            }
        })
    }

    pub fn routes(self: Arc<Self>) -> Router {
        #[derive(Deserialize)]
        struct QMapIndex {
            map_index: Option<usize>,
        }

        let mut router = Router::new()
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
                        s.stream_state_v1(options.incremental.unwrap_or_default()).await
                    })),
                )
                .route(
                    "/sse/rollouts_view",
                    get(handle!(self, s, { s.stream_state_v1(true).await })),
                )
            )
            .nest("/api/v2", Router::new()
                // V2 API
                .route("/state", get(handle!(self, s, { s.get_state().await })))
                .route(
                    "/sse",
                    get(handle!(self, s, |options: Query<SseHandlerParameters>|, {
                        s.stream_state(options.incremental.unwrap_or(true))
                    })),
                )
                .route(
                    "/rollouts/rollout_ic_os_to_mainnet_nodes/{dag_run_id}/stages/{stage_name}/batches/{batch_number}",
                    get(handle!(self, s, |Path((dag_run_id, stage_name, batch_number)): Path<(String, String, usize)>|, {
                        let cycle_state = s.state_syncer.get_state().await;
                        match s
                            .get_hostos_rollout_batch(dag_run_id.as_str(), stage_name.as_str(), batch_number, cycle_state) {
                                Ok(k) => Ok(Json(k)),
                                Err(e) => {
                                    Err(<(StatusCode, String)>::from(&e))
                                }
                            }
                    }))
                )
                .route(
                    "/rollouts/rollout_ic_os_to_mainnet_nodes/{dag_run_id}/stages/{stage_name}/batches/{batch_number}/sse",
                    get(handle!(self, s, |Path((dag_run_id, stage_name, batch_number)): Path<(String, String, usize)>|, {
                        s.stream_get_hostos_rollout_batch(dag_run_id, stage_name, batch_number).await
                    }))
                )
            );
        if self.unstable_api_enabled {
            router = router
            .nest(
                // unstable API
                "/api/unstable",
                Router::new().route(
                    "/cache",
                    get(handle!(self, s, { s.get_cache().await }))
                )
                .route(
                    "/internal_state",
                    get(handle!(self, s, { s.get_internal_state().await }))
                )
                .route(
                    "/dags/{dag_id}/dag_runs/{dag_run_id}",
                    get(handle!(self, s, |Path((dag_id, dag_run_id)): Path<(String, String)>|, {
                        s
                            .get_dag_run(dag_id.as_str(), dag_run_id.as_str())
                            .await
                    }))
                )
                .route(
                    "/dags/{dag_id}/dag_runs/{dag_run_id}/task_instances/{task_id}/xcom_entries/{xcom_key}",
                    get(handle!(self, s, |Path((dag_id,dag_run_id,task_instance_id,xcom_key)): Path<(String,String,String,String)>, Query(query): Query<QMapIndex>|, {
                        s.get_xcom_entry(dag_id.as_str(),dag_run_id.as_str(),task_instance_id.as_str(),query.map_index,xcom_key.as_str())
                        .await
                    }))
                )
            )
        }
        router
    }
}
