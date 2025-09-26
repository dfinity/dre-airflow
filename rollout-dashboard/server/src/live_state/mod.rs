use super::airflow_client::{
    AirflowClient, AirflowError, DagRunsResponseItem, DagsQueryFilter, TaskInstanceRequestFilters,
    TaskInstancesResponseItem,
};
use super::types::v2::{Rollout, RolloutEngineState, RolloutKind};
use super::types::{unstable, v2, v2::DagID, v2::DagRunID};
use chrono::{DateTime, Utc};
use derive_more::IntoIterator;
use futures::future::join_all;
use futures::join;
use indexmap::IndexMap;
use log::{debug, error, info};
use reqwest::StatusCode;
use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};
use std::cmp::max;
use std::collections::HashMap;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::fmt::{self, Display};
use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;
use std::{vec, vec::Vec};
use tokio::sync::Mutex;
use tokio::sync::watch::{self, Sender};
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep};
use tokio::{select, spawn};

mod api_boundary_nodes_rollout;
mod guestos_rollout;
mod hostos_rollout;
mod log_inspector;
mod plan;
mod python;
mod task_sorter;

// Lotsa tasks in the HostOS rollout.
const TASK_INSTANCE_LIST_LIMIT: usize = 750;
const LOG_TARGET: &str = "live_state";

/// Compares two Option(DateTimes) and returns the latest one if
/// both are Some(_), else returns the one defined if one is Some(_),
/// else returns None.
fn max_option_date<T>(d1: Option<DateTime<T>>, d2: Option<DateTime<T>>) -> Option<DateTime<T>>
where
    T: chrono::TimeZone,
{
    match (d1, d2) {
        (Some(d1), Some(d2)) => Some(max(d1, d2)),
        (Some(d1), None) => Some(d1),
        (None, Some(d2)) => Some(d2),
        (None, None) => None,
    }
}

/// Problem gathering rollout data.
///
/// For purposes of REST requests, this is serialized as a reply with an
/// error HTTP status code and a short explanatory message.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub enum RolloutDataGatherError {
    /// Problem communicating with Airflow.
    AirflowError(AirflowError),
    /// Unexpected cyclic dependency in task data served by Airflow.
    CyclicDependency(task_sorter::CyclicDependencyError),
}

impl Display for RolloutDataGatherError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Cannot gather rollout data: {}",
            match self {
                Self::AirflowError(e) => e.to_string(),
                Self::CyclicDependency(e) => e.to_string(),
            }
        )
    }
}

impl Clone for RolloutDataGatherError {
    fn clone(&self) -> Self {
        match self {
            Self::AirflowError(e) => Self::AirflowError(e.clone()),
            Self::CyclicDependency(e) => Self::CyclicDependency(e.clone()),
        }
    }
}

impl From<AirflowError> for RolloutDataGatherError {
    fn from(err: AirflowError) -> Self {
        Self::AirflowError(err)
    }
}

impl From<task_sorter::CyclicDependencyError> for RolloutDataGatherError {
    fn from(err: task_sorter::CyclicDependencyError) -> Self {
        Self::CyclicDependency(err)
    }
}

impl From<RolloutDataGatherError> for (reqwest::StatusCode, String) {
    fn from(f: RolloutDataGatherError) -> Self {
        match f {
            RolloutDataGatherError::AirflowError(e) => e.into(),
            RolloutDataGatherError::CyclicDependency(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, format!("{f}"))
            }
        }
    }
}

impl From<RolloutDataGatherError> for v2::Error {
    fn from(f: RolloutDataGatherError) -> v2::Error {
        let (code, message) = f.into();
        v2::Error {
            code,
            message,
            permanent: false,
        }
    }
}

#[derive(Clone, Serialize, Debug)]
pub struct SuccessfulSyncCycleState {
    pub rollouts: Rollouts,
    pub rollout_engine_states: v2::RolloutEngineStates,
}

impl From<SuccessfulSyncCycleState> for v2::State {
    fn from(s: SuccessfulSyncCycleState) -> v2::State {
        v2::State {
            rollouts: s.rollouts.0.into_values().collect(),
            rollout_engine_states: s.rollout_engine_states,
        }
    }
}

#[derive(Clone, Serialize, Debug)]
pub enum SyncCycleState {
    /// The dashboard is loading data from Airflow after start.
    Initial,
    /// Rollout and engine data from the last update cycle.
    Successful(SuccessfulSyncCycleState),
    /// An error occurred gathering data from Airflow during the
    /// last update cycle.
    Error(RolloutDataGatherError),
}

#[derive(Clone, Serialize)]
#[allow(clippy::large_enum_variant)]
pub(super) enum Parser {
    Subnets(guestos_rollout::Parser),
    ApiBoundaryNodes(api_boundary_nodes_rollout::Parser),
    Nodes(hostos_rollout::Parser),
}

impl Parser {
    fn new(dag_id: DagID) -> Self {
        match dag_id {
            DagID::RolloutIcOsToMainnetSubnets => Parser::Subnets(guestos_rollout::Parser::new()),
            DagID::RolloutIcOsToMainnetApiBoundaryNodes => {
                Parser::ApiBoundaryNodes(api_boundary_nodes_rollout::Parser::new())
            }
            DagID::RolloutIcOsToMainnetNodes => Parser::Nodes(hostos_rollout::Parser::new()),
        }
    }

    fn valid_dag_ids() -> Vec<DagID> {
        vec![
            DagID::RolloutIcOsToMainnetSubnets,
            DagID::RolloutIcOsToMainnetApiBoundaryNodes,
            DagID::RolloutIcOsToMainnetNodes,
        ]
    }

    async fn reparse(
        &mut self,
        dag_run: &DagRunsResponseItem,
        airflow_api: Arc<AirflowClient>,
        linearized_tasks: Vec<TaskInstancesResponseItem>,
    ) -> Result<RolloutKind, RolloutDataGatherError> {
        match self {
            Self::Subnets(r) => r.reparse(dag_run, airflow_api, linearized_tasks).await,
            Self::ApiBoundaryNodes(r) => r.reparse(dag_run, airflow_api, linearized_tasks).await,
            Self::Nodes(r) => r.reparse(dag_run, airflow_api, linearized_tasks).await,
        }
    }
}

#[derive(Clone, Serialize)]
pub(super) struct RolloutState {
    dag_id: DagID,
    dag_run_id: DagRunID,
    pub(super) parser: Parser,
    #[serde(skip_serializing)]
    task_instances: IndexMap<String, HashMap<Option<usize>, TaskInstancesResponseItem>>,
    logical_date: DateTime<Utc>,
    note: Option<String>,
    last_update_time: Option<DateTime<Utc>>,
    update_count: usize,
}

impl From<&RolloutState> for unstable::FlowCacheResponse {
    fn from(s: &RolloutState) -> unstable::FlowCacheResponse {
        unstable::FlowCacheResponse {
            dag_id: s.dag_id.clone(),
            rollout_id: s.dag_run_id.clone(),
            dispatch_time: s.logical_date,
            linearized_task_instances: s
                .task_instances
                .iter()
                .flat_map(|(_, s)| s.values().cloned())
                .collect(),
            last_update_time: s.last_update_time,
            update_count: s.update_count,
        }
    }
}

impl RolloutState {
    async fn update(
        &mut self,
        dag_run: &DagRunsResponseItem,
        airflow_api: Arc<AirflowClient>,
        retrieved_task_instances: Vec<TaskInstancesResponseItem>,
        last_event_log_update: Option<DateTime<Utc>>,
        sorter: &task_sorter::TaskInstanceTopologicalSorter,
    ) -> Result<Rollout, RolloutDataGatherError> {
        // Merge tasks retrieved now with tasks retrieved before.
        let mut new_last_update_time =
            max_option_date(self.last_update_time, last_event_log_update);
        let mut updates: usize = 0;
        let retrieved_task_count = retrieved_task_instances.len();

        for task_instance in retrieved_task_instances.into_iter() {
            let task_instance_id = task_instance.task_id.clone();
            new_last_update_time =
                max_option_date(new_last_update_time, Some(task_instance.latest_date()));

            let by_name = self.task_instances.entry(task_instance_id).or_default();

            match by_name.entry(task_instance.map_index) {
                Vacant(entry) => {
                    entry.insert(task_instance);
                    updates += 1;
                }
                Occupied(mut entry) => {
                    if task_instance.latest_date() > entry.get().latest_date()
                        || task_instance.state != entry.get().state
                        || task_instance.note != entry.get().note
                    {
                        entry.insert(task_instance.clone());
                        updates += 1;
                    }
                }
            };
        }

        for (task_instance_id, tasks) in self.task_instances.iter_mut() {
            // Delete data on all unmapped tasks if a mapped task sibling is present.
            if tasks.len() > 1 {
                if let Occupied(_) = tasks.entry(None) {
                    debug!(
                        target: LOG_TARGET, "formerly unmapped task {} is now mapped",
                        task_instance_id
                    );
                    tasks.remove(&None);
                    updates += 1;
                }
            }
        }

        let linearized_tasks: Vec<TaskInstancesResponseItem> = sorter.sort_instances(
            self.task_instances
                .iter()
                .flat_map(|(_, tasks)| tasks.values().cloned()),
        );

        let tgt = &format!("{}::{}::{}", LOG_TARGET, dag_run.dag_id, "UpdaterState");

        if updates > 0 {
            debug!(target: tgt, "{}: {} of {} task instances updated after retrieving {} tasks.  Rollout has {} updates now.", dag_run.dag_run_id, updates, linearized_tasks.len(), retrieved_task_count, self.update_count + updates);
            self.update_count += updates;
        }

        // If the note of the rollout has changed, note that this has been updated.
        if self.note != dag_run.note {
            if self.note.is_some() {
                debug!(target: tgt, "{}: Rollout note has been updated.  Updating", dag_run.dag_run_id);
            }
            self.note.clone_from(&dag_run.note);
            self.update_count += 1;
        }

        let rollout = Rollout {
            name: DagRunID::from_str(dag_run.dag_run_id.as_str()).unwrap(),
            display_url: {
                let mut display_url = airflow_api
                    .url
                    .join(format!("/dags/{}/grid", dag_run.dag_id).as_str())
                    .unwrap();
                display_url
                    .query_pairs_mut()
                    .append_pair("dag_run_id", dag_run.dag_run_id.as_str());
                display_url.to_string()
            },
            note: dag_run.note.clone(),
            dispatch_time: dag_run.logical_date,
            last_scheduling_decision: dag_run.last_scheduling_decision,
            update_count: self.update_count,
            kind: self
                .parser
                .reparse(dag_run, airflow_api, linearized_tasks)
                .await?,
        };
        self.last_update_time = new_last_update_time;

        Ok(rollout)
    }
}

#[derive(Clone)]
pub(super) struct RolloutStates(HashMap<(DagID, DagRunID), RolloutState>);

// The following is necessary because the internal index map is not
// serializable to JSON.
impl Serialize for RolloutStates {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let themap: IndexMap<String, &RolloutState> = self
            .0
            .iter()
            .map(|(k, v)| (format!("{}/{}", k.0, k.1), v))
            .collect();
        let mut map = serializer.serialize_map(Some(themap.len()))?;
        for (k, v) in themap {
            map.serialize_entry(&k, v)?;
        }
        map.end()
    }
}

impl RolloutStates {
    fn clone_or_new(
        &self,
        dag_id: DagID,
        dag_run_id: DagRunID,
        logical_date: DateTime<Utc>,
    ) -> RolloutState {
        match self.0.get(&(dag_id.clone(), dag_run_id.clone())) {
            Some(updater_state) => match updater_state.logical_date == logical_date {
                // Same rollout being updated.
                true => RolloutState {
                    dag_id,
                    dag_run_id,
                    parser: updater_state.parser.clone(),
                    task_instances: updater_state.task_instances.clone(),
                    note: updater_state.note.clone(),
                    last_update_time: updater_state.last_update_time,
                    logical_date: updater_state.logical_date,
                    update_count: updater_state.update_count,
                },
                // Rollout redispatched with same name, we start blank.
                // we do preserve the update count.
                false => RolloutState {
                    dag_id: dag_id.clone(),
                    dag_run_id,
                    parser: Parser::new(dag_id),
                    task_instances: IndexMap::new(),
                    note: None,
                    last_update_time: None,
                    logical_date,
                    update_count: updater_state.update_count + 1,
                },
            },
            // Not found, let's create one!
            None => RolloutState {
                dag_id: dag_id.clone(),
                dag_run_id,
                parser: Parser::new(dag_id),
                task_instances: IndexMap::new(),
                note: None,
                last_update_time: None,
                logical_date,
                update_count: 0,
            },
        }
    }

    fn clear(&mut self) {
        self.0.drain();
    }

    fn update(&mut self, updater: RolloutState) {
        self.0.insert(
            (updater.dag_id.clone(), updater.dag_run_id.clone()),
            updater,
        );
    }

    pub(super) fn get(&self, v: &(DagID, DagRunID)) -> Option<&RolloutState> {
        self.0.get(v)
    }

    pub fn iter(&self) -> std::collections::hash_map::Iter<'_, (DagID, DagRunID), RolloutState> {
        self.0.iter()
    }
}

#[derive(Clone)]
pub struct SyncerState {
    /// Map from DAG ID and DAG run ID to updater.
    pub(super) rollout_states: RolloutStates,
    log_inspectors: HashMap<DagID, log_inspector::AirflowIncrementalLogInspector>,
    pub(super) cycle_state: SyncCycleState,
}

pub struct Initial;
pub struct Live;

pub struct AirflowStateSyncer<S> {
    airflow_api: Arc<AirflowClient>,
    syncer_state: Arc<Mutex<Arc<SyncerState>>>,
    stream_tx: Sender<Arc<SyncerState>>,
    refresh_interval: u64,
    max_rollouts: usize,
    #[allow(dead_code)]
    state: S,
}

impl AirflowStateSyncer<Initial> {
    pub fn new(
        airflow_api: Arc<AirflowClient>,
        max_rollouts: usize,
        refresh_interval: u64,
    ) -> Self {
        let init: SyncCycleState = SyncCycleState::Initial;
        let init_syncer_state = Arc::new(SyncerState {
            rollout_states: RolloutStates(HashMap::new()),
            log_inspectors: HashMap::new(),
            cycle_state: init,
        });
        let (stream_tx, _stream_rx) = watch::channel::<Arc<SyncerState>>(init_syncer_state.clone());
        Self {
            airflow_api,
            syncer_state: Arc::new(Mutex::new(init_syncer_state)),
            stream_tx,
            refresh_interval,
            max_rollouts,
            state: Initial,
        }
    }

    pub fn start_syncing(
        self,
        mut cancel_receiver: watch::Receiver<()>,
    ) -> (Arc<AirflowStateSyncer<Live>>, JoinHandle<()>) {
        let ret: Arc<AirflowStateSyncer<Live>> = Arc::new(AirflowStateSyncer {
            state: Live,
            airflow_api: self.airflow_api,
            syncer_state: self.syncer_state,
            stream_tx: self.stream_tx,
            refresh_interval: self.refresh_interval,
            max_rollouts: self.max_rollouts,
        });
        let looper = ret.clone();
        let background_loop_fut = spawn(async move {
            looper
                .periodically_sync_state(async move {
                    let _ = cancel_receiver.changed().await;
                })
                .await
        });
        (ret, background_loop_fut)
    }
}

#[derive(Clone, IntoIterator, Debug)]
pub struct Rollouts(IndexMap<(v2::DagID, v2::DagRunID), Rollout>);

impl Rollouts {
    pub fn get(&self, q: &(v2::DagID, v2::DagRunID)) -> Option<&Rollout> {
        self.0.get(q)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.len() == 0
    }
}

// The following is necessary because the internal index map is not
// serializable to JSON.
impl Serialize for Rollouts {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let themap: IndexMap<String, &Rollout> = self
            .0
            .iter()
            .map(|(k, v)| (format!("{}/{}", k.0, k.1), v))
            .collect();
        let mut map = serializer.serialize_map(Some(themap.len()))?;
        for (k, v) in themap {
            map.serialize_entry(&k, v)?;
        }
        map.end()
    }
}

impl AirflowStateSyncer<Live> {
    pub async fn get_state(&self) -> Arc<SyncerState> {
        self.syncer_state.lock().await.clone()
    }

    /// Create a channel that will get state updates as soon as they are available.
    /// This needs `periodically_sync_state` running in a coroutine.  That is
    /// statically ensured by the different type of this impl.
    pub fn subscribe_to_syncer_updates(&self) -> watch::Receiver<Arc<SyncerState>> {
        self.stream_tx.subscribe()
    }

    /// Retrieve all rollout data, using a cache to avoid
    /// re-fetching task instances not updated since last time.
    ///
    /// Returns a tuple of the the engine state data (keyed by name)
    /// and a vector of rollouts sorted by dispatch date/time.
    ///
    /// The rollout structure itself is updated on every call
    /// for every DAG run.  However, not every change in the DAG
    /// run is considered to be a meaningful change (causing a
    /// true return in the update flag).  Currently, only a change
    /// in the rollout note, the state of any of its tasks, or
    /// the rollout dispatch time are considered meaningful changes.
    /// This change is counted in the update_count member of the
    /// Rollout data structure.
    async fn update(
        &self,
        mut rollout_states: RolloutStates,
        mut log_inspectors: HashMap<DagID, log_inspector::AirflowIncrementalLogInspector>,
        max_rollouts: usize,
    ) -> Result<
        (
            v2::RolloutEngineStates,
            Rollouts,
            RolloutStates,
            HashMap<DagID, log_inspector::AirflowIncrementalLogInspector>,
        ),
        RolloutDataGatherError,
    > {
        debug!(target: LOG_TARGET, "Retrieving engine states.");

        // Retrieve the state of each DAG.
        let mut engine_states: v2::RolloutEngineStates = {
            let mut dags_response = self
                .airflow_api
                .dags(1000, 0, &DagsQueryFilter::default(), None)
                .await?;
            dags_response.dags.retain(|dag| {
                Parser::valid_dag_ids()
                    .iter()
                    .any(|s| s.to_string() == dag.dag_id)
            });
            dags_response
        }
        .into();

        for d in Parser::valid_dag_ids() {
            engine_states
                .entry(d.to_string())
                .or_insert(RolloutEngineState::Broken);
        }

        let rollout_state_futures: Vec<_> = join_all(
            Parser::valid_dag_ids()
                .into_iter()
                .map(|dag_id| {
                    let inspector = log_inspectors.entry(dag_id.clone())
                    .or_default().clone();
                    (
                        dag_id.clone(),
                        inspector,
                    )
                }).collect::<Vec<_>>()
                .into_iter().map(|(dag_id, log_inspector)|
                    {
                        async move {
                            debug!(target: &format!("{LOG_TARGET}::{dag_id}"), "Retrieving DAG runs, audit events and tasks.");
                            let dis = dag_id.to_string();

                            let (runs_result, audit_result, tasks_result) = join!(
                                // Retrieve the latest X DAG runs for the DAG we're operating with.
                                self.airflow_api
                                    .dag_runs(&dis, max_rollouts, 0, None, None),
                                // Call the log inspectors to determine which tasks need to be updated.
                                // Each log inspector knows about all updated tasks of all DAG runs for a DAG.
                                // Then insert any newly needed inspector into the in-flight inspectors map.
                                log_inspector
                                    .incrementally_detect_dag_updates(&self.airflow_api, &dag_id),
                                self.airflow_api.tasks(&dis),
                            );

                            let (dag_runs, (log_inspector, dag_run_updates_required), tasks) = (runs_result?.dag_runs, audit_result?, tasks_result?);

                            Ok((
                                dag_id,
                                dag_run_updates_required,
                                dag_runs,
                                // Fetch tasks of the DAG to later construct a topological sorter.
                                tasks,
                                log_inspector,
                            ))
                        }
                }),
        )
        .await
        .into_iter()
        .filter_map(|r: std::result::Result<_, AirflowError>| {
            // Here we handle the circumstance whereby a DAG itself went missing because it was broken.
            // Positive confirmation of this phenomenon will be available in the v2::RolloutEngineStates
            // returned at the end of this function.
            //
            // We handle this by filtering because, that way, at least we can collect information on
            // existing DAGs that are *not* broken.
            match r {
                Err(AirflowError::StatusCode(StatusCode::NOT_FOUND)) => {
                    error!(target: LOG_TARGET, "Airflow responded {} to request for DAG runs and tasks of some DAG -- DAG probably broken.", StatusCode::NOT_FOUND);
                    None
                }
                _ => Some(r),
            }
        })
        .collect::<Result<Vec<_>, AirflowError>>()?
        .into_iter()
        .map(
            |(
                dag_id,
                dag_run_updates_required,
                dag_runs,
                tasks,
                log_inspector,
            )| {
                // Update the log inspectors with the updated ones, then peel them off from the iterator.
                // But before peeling off the log inspector, copy its last event log update.
                let last_event_log_update = log_inspector.last_event_log_update;
                log_inspectors.insert(dag_id.clone(), log_inspector);

                Ok((
                    dag_id,
                    last_event_log_update,
                    dag_run_updates_required,
                    dag_runs,
                    task_sorter::TaskInstanceTopologicalSorter::new(tasks)?,
                ))
            },
        )
        .collect::<Result<Vec<_>, task_sorter::CyclicDependencyError>>()?
        .into_iter()
        .flat_map(
            // Map the prior into an iterator where each element corresponds to a DAG run and its update type.
            // Data of a DAG common to more than one DAG run is cloned for that DAG run and passed down.
            move |(dag_id, last_event_log_update, dag_run_update_types, dag_runs, sorter)| {
                dag_runs.into_iter().map(move |dag_run| {
                    let dag_run_id = DagRunID::from_str(&dag_run.dag_run_id).unwrap();
                    let update_type = dag_run_update_types.update_type(&dag_run_id);
                    (
                        dag_id.clone(),
                        dag_run_id,
                        last_event_log_update,
                        update_type,
                        dag_run,
                        sorter.clone(),
                    )
                })
            },
        )
        .map(|(dag_id, dag_run_id, last_event_log_update, dag_run_update_type, dag_run, sorter)| {
            // For each DAG run, create / clone its rollout state, then fetch all tasks, or tasks that have been updated.

            let mut rollout_state = rollout_states.clone_or_new(dag_id, dag_run_id, dag_run.logical_date);

            async move || {
                let tgt = &format!("{}::{}", LOG_TARGET, rollout_state.dag_id);
                let retrieved_task_instances: Vec<TaskInstancesResponseItem> = match (dag_run_update_type, rollout_state.last_update_time)     {
                    (log_inspector::DagRunUpdateType::AllTaskInstances, _) | (_, None)=> {
                        // We are retrieving all the tasks once again.
                        // Evacuate the existing task list.
                        rollout_state.task_instances = IndexMap::new();
                        debug!(target: tgt, "{}: collecting data about all task instances.", rollout_state.dag_run_id);
                        self.airflow_api
                            .task_instances(
                                &rollout_state.dag_id.to_string(),
                                &rollout_state.dag_run_id.to_string(),
                                TASK_INSTANCE_LIST_LIMIT,
                                0,
                                TaskInstanceRequestFilters::default(),
                            )
                            .await?.task_instances
                    }
                    (log_inspector::DagRunUpdateType::SomeTaskInstances(updated_task_instances), Some(lut)) => {
                        let updated_task_instances =
                            updated_task_instances.iter().cloned().collect::<Vec<_>>();
                        debug!(
                            target: tgt,
                            "{}: collecting data about task instances updated since {}{}.",
                            rollout_state.dag_run_id,
                            lut,
                            match updated_task_instances.is_empty() {
                                true => "".to_string(),
                                false => format!(" and a specific set of tasks too: {:?}", updated_task_instances),
                            }
                        );
                        [
                            self.airflow_api
                                .task_instances_batch(
                                    Some(vec![rollout_state.dag_id.to_string()]),
                                    Some(vec![rollout_state.dag_run_id.to_string()]),
                                    Some(updated_task_instances),
                                )
                                .await?.task_instances,
                            self.airflow_api
                                .task_instances(
                                    &rollout_state.dag_id.to_string(),
                                    &rollout_state.dag_run_id.to_string(),
                                    TASK_INSTANCE_LIST_LIMIT,
                                    0,
                                    TaskInstanceRequestFilters::default()
                                        .executed_on_or_after(Some(lut)),
                                )
                                .await?.task_instances,
                            self.airflow_api
                                .task_instances(
                                    &rollout_state.dag_id.to_string(),
                                    &rollout_state.dag_run_id.to_string(),
                                            TASK_INSTANCE_LIST_LIMIT,
                                    0,
                                    TaskInstanceRequestFilters::default()
                                        .updated_on_or_after(Some(lut)),
                                )
                                .await?.task_instances,
                            self.airflow_api
                                .task_instances(
                                    &rollout_state.dag_id.to_string(),
                                    &rollout_state.dag_run_id.to_string(),
                                            TASK_INSTANCE_LIST_LIMIT,
                                    0,
                                    TaskInstanceRequestFilters::default()
                                        .ended_on_or_after(Some(lut)),
                                )
                                .await?.task_instances,
                        ].concat()
                    }
                };
                Ok((
                    last_event_log_update,
                    dag_run,
                    sorter,
                    rollout_state,
                    retrieved_task_instances,
                ))
        }
        }()).collect();

        // Collect all the futures that have retrieved tasks and produced rollout states
        // as each state is updated with the retrieved tasks.  This happens concurrently.
        let updated_rollout_states: Vec<_> = join_all(join_all(rollout_state_futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, AirflowError>>()?
            .into_iter()
            .collect::<Vec<_>>().into_iter().map(
                |(last_event_log_update, dag_run, sorter, mut rollout_state, retrieved_task_instances)| {
                    async move || {
                        let rollout = rollout_state
                        .update(
                            &dag_run,
                            self.airflow_api.clone(),
                            retrieved_task_instances,
                            last_event_log_update,
                            &sorter,
                        ).await?;
                        Ok((rollout_state, rollout))
                    }
                }(),
            )
        ).await.into_iter()
        .collect::<Result<Vec<_>, RolloutDataGatherError>>()?;

        // The remaining updaters in the inflight updaters variable are updaters for
        // rollouts that have disappeared.  Remove them.
        rollout_states.clear();

        // Collect results.
        let mut rollouts: Vec<Rollout> = vec![];
        for (updater_state, rollout) in updated_rollout_states {
            rollouts.push(rollout);
            // Update the inflight updaters with the updated updaters that correspond
            // to all dag runs of all handled dags that Airflow sent us.
            rollout_states.update(updater_state);
        }

        // Sort rollouts by dispatch date (reversed).
        rollouts.sort_by_key(|rollout| -rollout.dispatch_time.timestamp());

        // Save the state of the log inspector after everything was successful.
        Ok((
            engine_states,
            Rollouts(
                rollouts
                    .into_iter()
                    .map(|r| ((r.kind(), r.name.clone()), r))
                    .collect(),
            ),
            rollout_states,
            log_inspectors,
        ))
    }

    async fn sync_state(&self, max_rollouts: usize) -> Arc<SyncerState> {
        let syncer_state = self.syncer_state.lock().await;
        let (rollout_states, log_inspectors) = (
            syncer_state.rollout_states.clone(),
            syncer_state.log_inspectors.clone(),
        );
        drop(syncer_state);

        match self
            .update(rollout_states.clone(), log_inspectors.clone(), max_rollouts)
            .await
        {
            Ok((engine_state, rollouts, rollout_states, log_inspectors)) => {
                let mut syncer_state = self.syncer_state.lock().await;
                *syncer_state = Arc::new(SyncerState {
                    log_inspectors,
                    rollout_states,
                    cycle_state: SyncCycleState::Successful(SuccessfulSyncCycleState {
                        rollout_engine_states: engine_state,
                        rollouts,
                    }),
                });
                syncer_state.clone()
            }
            Err(e) => {
                let mut syncer_state = self.syncer_state.lock().await;
                *syncer_state = Arc::new(SyncerState {
                    log_inspectors: syncer_state.log_inspectors.clone(),
                    rollout_states: syncer_state.rollout_states.clone(),
                    cycle_state: SyncCycleState::Error(e),
                });
                syncer_state.clone()
            }
        }
    }

    async fn periodically_sync_state<F>(self: Arc<Self>, cancel: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut errored = false;
        tokio::pin!(cancel);
        let tgt = &(LOG_TARGET.to_owned() + "::periodically_sync_state");

        loop {
            let loop_start_time: DateTime<Utc> = Utc::now();

            let d = select! {
                d = self.sync_state(self.max_rollouts) => {
                    match &d.cycle_state {
                        SyncCycleState::Initial => (),
                        SyncCycleState::Successful(SuccessfulSyncCycleState {rollouts, ..}) => {
                            let loop_delta_time = Utc::now() - loop_start_time;
                            info!(target: tgt, "{} rollouts collected after {}.  Sleeping for {} seconds.", rollouts.len(), loop_delta_time, self.refresh_interval);
                            if errored {
                                info!(target: tgt, "Successfully processed rollout data again after temporary error");
                                // Clear error flag.
                                errored = false;
                                // Ensure our data structure is overwritten by whatever data we obtained after the last loop.
                            }
                        }
                        SyncCycleState::Error(err) => {
                            error!(
                                target: tgt, "During sync_state within periodically_sync_state: {err}"
                            );
                            errored = true;
                        }
                    }
                    d
                },
                _ = &mut cancel => break,
            };

            let _ = self.stream_tx.send_replace(d.clone());

            select! {
                _ = sleep(Duration::from_secs(self.refresh_interval)) => (),
                _ = &mut cancel => break,
            }
        }
    }
}
