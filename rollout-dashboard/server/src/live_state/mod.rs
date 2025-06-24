use chrono::{DateTime, Utc};
use log::{debug, error, info, trace};
use regex::Regex;
use reqwest::StatusCode;
use rollout_dashboard::airflow_client::{
    AirflowClient, AirflowError, DagsQueryFilter, EventLogsResponseFilters,
    TaskInstancesResponseItem, TasksResponse, TasksResponseItem,
};
use rollout_dashboard::types::{unstable, v2};
use std::cmp::max;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::{HashMap, HashSet, VecDeque};
use std::error::Error;
use std::fmt::{self, Display};
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use std::{vec, vec::Vec};
use tokio::sync::watch::{self, Sender};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
use tokio::{select, spawn};
use topological_sort::TopologicalSort;

mod guestos_rollout;
mod python;

const TASK_INSTANCE_LIST_LIMIT: usize = 500;

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

#[derive(Debug)]
struct CyclicDependencyError {
    message: String,
}

impl Display for CyclicDependencyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

#[derive(Clone)]
struct TaskInstanceTopologicalSorter {
    sorted_tasks: Vec<Arc<TasksResponseItem>>,
}

impl TaskInstanceTopologicalSorter {
    fn new(r: TasksResponse) -> Result<Self, CyclicDependencyError> {
        let mut all_nodes: HashMap<String, Arc<TasksResponseItem>> = HashMap::new();
        let mut ts = TopologicalSort::<String>::new();

        for task in r.tasks.into_iter() {
            let taskid = task.task_id.clone();
            let downstream_taskids: Vec<String> = task.downstream_task_ids.to_vec();
            all_nodes.insert(taskid.clone(), Arc::new(task));
            for subtask in downstream_taskids.iter() {
                ts.add_dependency(taskid.clone(), subtask);
            }
        }

        let mut sorted_tasks = vec![];

        loop {
            let round = ts.pop_all();
            match round.is_empty() {
                true => {
                    if !ts.is_empty() {
                        return Err(CyclicDependencyError {
                            message: format!("cyclic dependencies: {:?}", ts),
                        });
                    }
                    break;
                }
                false => {
                    for taskid in round.iter() {
                        if let Some(all_nodes) = all_nodes.get(taskid) {
                            sorted_tasks.push(all_nodes.clone())
                        }
                    }
                }
            }
        }

        Ok(Self { sorted_tasks })
    }

    fn sort_instances<I>(&self, r: I) -> Vec<TaskInstancesResponseItem>
    where
        I: Iterator<Item = TaskInstancesResponseItem>,
    {
        let mut all_task_instances: HashMap<String, Vec<Rc<TaskInstancesResponseItem>>> =
            HashMap::new();

        for task_instance in r.into_iter() {
            let taskid = task_instance.task_id.clone();
            let mapindex = task_instance.map_index;
            let tasklist = all_task_instances.entry(taskid.clone()).or_default();
            let rctaskinstance = Rc::new(task_instance);
            match tasklist.binary_search_by(|probe| {
                match mapindex {
                    None => 0,
                    Some(i) => i + 1,
                }
                .cmp(&match probe.map_index {
                    None => 0,
                    Some(i) => i + 1,
                })
            }) {
                Ok(pos) => {
                    panic!(
                        "Task instance {} {:?} cannot be already in pos {}",
                        taskid, mapindex, pos
                    )
                } // element already in vector @ `pos`
                Err(pos) => tasklist.insert(pos, rctaskinstance),
            }
        }

        let mut sorted_task_instances: Vec<TaskInstancesResponseItem> = vec![];

        for task in self.sorted_tasks.iter() {
            let task_instances = match all_task_instances.get(&task.task_id) {
                Some(t) => t,
                // We could not find a task instance named after the task.
                // That can happen, so we ignore it.
                None => continue,
            };
            for ti in task_instances.iter() {
                let task = ti.as_ref();
                sorted_task_instances.push(task.clone());
            }
        }

        sorted_task_instances
    }
}

#[derive(Debug)]
enum RolloutDataGatherError {
    AirflowError(AirflowError),
    CyclicDependency(CyclicDependencyError),
}

impl From<AirflowError> for RolloutDataGatherError {
    fn from(err: AirflowError) -> Self {
        Self::AirflowError(err)
    }
}

impl From<CyclicDependencyError> for RolloutDataGatherError {
    fn from(err: CyclicDependencyError) -> Self {
        Self::CyclicDependency(err)
    }
}

#[derive(Debug, Clone)]
/// DAG run update type.
enum DagRunUpdateType {
    /// This DAG run needs all its task instances refreshed.
    AllTaskInstances,
    /// At least these tasks in this DAG run need to be
    /// refreshed, as well as incremental queries of tasks
    /// finished, updated, or started since last query.
    SomeTaskInstances(HashSet<String>),
}

/// Describe what kind of update each DAG run needs.
/// A DAG run listed in this DAG can have one of two update types,
/// defined in `DagRunUpdateType``.
struct DagRunUpdatesRequired {
    dag_runs: HashMap<String, DagRunUpdateType>,
}

impl DagRunUpdatesRequired {
    fn new() -> Self {
        Self {
            dag_runs: HashMap::new(),
        }
    }

    /// Returns the `DagRunUpdateType` for a given DAG run.
    /// If the DAG run is unknown to this function, then
    /// SomeTaskInstances(vec[]) is returned.
    fn update_type(&self, dag_run_id: &String) -> DagRunUpdateType {
        match self.dag_runs.get(dag_run_id) {
            None => DagRunUpdateType::SomeTaskInstances(HashSet::new()),
            Some(t) => t.clone(),
        }
    }
}

#[derive(Default)]
/// Inspects the Airflow log every time its incrementally_detect_dag_updates
/// function is called.
struct AirflowIncrementalLogInspector {
    last_event_log_update: Option<DateTime<Utc>>,
}

impl AirflowIncrementalLogInspector {
    /// Inspect changes to the log, and return a `DagRunUpdatesRequired`
    /// struct based on the contents of the log since its last inspection.
    async fn incrementally_detect_dag_updates(
        &self,
        airflow_api: &AirflowClient,
        dag_id: &str,
    ) -> Result<(Self, DagRunUpdatesRequired), AirflowError> {
        let mut task_instances_to_update_per_dag = DagRunUpdatesRequired::new();

        let mut last_event_log_update = self.last_event_log_update;

        if last_event_log_update.is_some() {
            // Construct a plan of what tasks will be queried, by using the
            // Airflow event log as a deciding factor.
            let event_logs = airflow_api
                .event_logs(
                    1000,
                    0,
                    &EventLogsResponseFilters {
                        after: last_event_log_update,
                        dag_id: Some(&dag_id.to_string()),
                        ..Default::default()
                    },
                    None,
                )
                .await?;

            // Process log events.
            for event in event_logs.event_logs.iter() {
                // Remember the date of the latest event.
                last_event_log_update = Some(event.when);
                // Ignore events with no dag ID or wrong dag ID.
                match &event.dag_id {
                    Some(d) => match *d == dag_id {
                        true => d,
                        false => continue,
                    },
                    None => continue,
                };
                // Ignore events that have no run ID.
                let event_run_id = match &event.run_id {
                    Some(r) => r,
                    None => continue,
                };
                // Ignore events that just change the DAG run note.  We already
                // retrieve the full DAG (not necessarily its tasks or instances),
                // so this event is not interesting.
                if event.event == "ui.set_dag_run_note" {
                    continue;
                }
                // Also ignore UI confirmation events to mark tasks as failed/success.
                if event.event == "confirm" {
                    continue;
                }
                // Also ignore UI clearing events of tasks not yet confirmed.
                if event.event == "clear" {
                    match &event.extra {
                        None => continue,
                        Some(extra) => {
                            let r = Regex::new(r".*.confirmed.: .true.*").unwrap();
                            if r.captures(extra.as_str()).is_none() {
                                // No confirmation.  We continue.
                                continue;
                            }
                            // We found it.  We won't continue.
                        }
                    }
                }

                // Under the following circumstances, the whole rollout has to be refreshed because
                // administrative action was taken to clear / fail / succeed tasks that may not in
                // fact appear listed in the log as such.
                let force_refresh_all_tasks = (event.event == "success" && event.extra.is_some())
                    || (event.event == "failed" && event.extra.is_some())
                    || (event.event == "clear" && event.extra.is_some());

                trace!(target: "rollout_data::log_inspector", "Processing event:\n{:#?}\n", event);

                match task_instances_to_update_per_dag
                    .dag_runs
                    .entry(event_run_id.to_string())
                {
                    // No entry.  Let's initialize it (all tasks if event has no run_id or forced, else the single task).
                    Vacant(ventry) => {
                        ventry.insert(match (&event.task_id, force_refresh_all_tasks) {
                (Some(t), false) => {
                    trace!(target: "rollout_data::log_inspector", "{}: initializing plan with a request to update task {}", event_run_id, t);
                    let mut init = HashSet::new();
                    init.insert(t.clone());
                    DagRunUpdateType::SomeTaskInstances(init)
                },
                _ => {
                    trace!(target: "rollout_data::log_inspector", "{}: initializing plan with a request to update all tasks", event_run_id);
                    DagRunUpdateType::AllTaskInstances
                },
            });
                    }
                    // There's an entry.  Update to all tasks if this event has no run_id.
                    Occupied(mut entry) => match (&event.task_id, force_refresh_all_tasks) {
                        (Some(t), false) => {
                            if let DagRunUpdateType::SomeTaskInstances(thevec) = entry.get_mut() {
                                let ts = t.to_string();
                                if !thevec.contains(&ts) {
                                    trace!(target: "rollout_data::log_inspector", "{}: adding task {} to plan", event_run_id, ts);
                                    thevec.insert(ts);
                                }
                            }
                        }
                        _ => {
                            if let DagRunUpdateType::SomeTaskInstances(_) = entry.get() {
                                trace!(target: "rollout_data::log_inspector", "{}: switching plan to request to update all tasks", event_run_id);
                                entry.insert(DagRunUpdateType::AllTaskInstances);
                            }
                        }
                    },
                }
            }

            // Now that we have a plan, we know what data to fetch from Airflow, minimizing the load on the server.
            for (k, v) in task_instances_to_update_per_dag.dag_runs.iter() {
                debug!(target: "rollout_data::log_inspector", "{}: tasks that will be updated: {}", k, match v {
                    DagRunUpdateType::AllTaskInstances => "all tasks".to_string(),
                    DagRunUpdateType::SomeTaskInstances(set_of_tasks) => set_of_tasks.iter().cloned().collect::<Vec<String>>().join(", "),
                });
            }
            if !event_logs.event_logs.is_empty()
                && !task_instances_to_update_per_dag.dag_runs.is_empty()
            {
                debug!(
                    target: "rollout_data::log_inspector", "Setting incremental refresh date to {:?}",
                    last_event_log_update
                )
            };
        } else {
            let event_logs = airflow_api
                .event_logs(
                    1,
                    0,
                    &EventLogsResponseFilters {
                        after: last_event_log_update,
                        dag_id: Some(&dag_id.to_string()),
                        ..Default::default()
                    },
                    Some("-event_log_id".to_string()),
                )
                .await?;
            for event in event_logs.event_logs.iter() {
                last_event_log_update = Some(event.when);
            }
            if !event_logs.event_logs.is_empty() {
                debug!(target: "rollout_data::log_inspector", "Setting initial refresh date to {:?}", last_event_log_update);
            }
        }

        Ok((
            Self {
                last_event_log_update,
            },
            task_instances_to_update_per_dag,
        ))
    }
}

trait FlowCacheViewable: Sized {
    fn get_task_instances(&self) -> Vec<TaskInstancesResponseItem>;
    fn get_flow_info(&self) -> (DateTime<Utc>, Option<DateTime<Utc>>, usize);
}

/// Contains either a list of rollouts ordered from newest to oldest,
/// dated from the last time it was successfully updated, or an HTTP
/// status code corresponding to -- and with -- a message for the last error.
pub type CurrentState = v2::StateResponse;

struct RolloutStateCache {
    /// Map from DAG run ID to task instance ID (with / without index)
    /// to task instance.
    by_dag_run: HashMap<String, guestos_rollout::RolloutInfoCache>,
    log_inspector: AirflowIncrementalLogInspector,
}

pub(crate) struct Initial;
pub(crate) struct Live;

pub(crate) struct AirflowStateSyncer<S> {
    airflow_api: Arc<AirflowClient>,
    cache: Arc<Mutex<RolloutStateCache>>,
    current_state: Arc<Mutex<CurrentState>>,
    stream_tx: Sender<CurrentState>,
    refresh_interval: u64,
    max_rollouts: usize,
    #[allow(dead_code)]
    state: S,
}

impl AirflowStateSyncer<Initial> {
    pub fn new(airflow_api: AirflowClient, max_rollouts: usize, refresh_interval: u64) -> Self {
        let init: CurrentState = v2::StateResponse::Error(v2::Error {
            code: StatusCode::NO_CONTENT,
            message: "".to_string(),
        });
        let (stream_tx, _stream_rx) = watch::channel::<CurrentState>(init.clone());
        Self {
            airflow_api: Arc::new(airflow_api),
            cache: Arc::new(Mutex::new(RolloutStateCache {
                by_dag_run: HashMap::new(),
                log_inspector: AirflowIncrementalLogInspector::default(),
            })),
            current_state: Arc::new(Mutex::new(init)),
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
            cache: self.cache,
            current_state: self.current_state,
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

impl AirflowStateSyncer<Live> {
    pub async fn get_current_state(&self) -> CurrentState {
        self.current_state.lock().await.clone()
    }

    pub async fn get_cache(&self) -> Vec<unstable::FlowCacheResponse> {
        let cache = self.cache.lock().await;
        let mut result: Vec<_> = cache
            .by_dag_run
            .iter()
            .map(|(k, v)| {
                let (dispatch_time, last_update_time, update_count) = v.get_flow_info();
                unstable::FlowCacheResponse {
                    rollout_id: k.clone(),
                    linearized_task_instances: v.get_task_instances(),
                    dispatch_time,
                    last_update_time,
                    update_count,
                }
            })
            .collect();
        drop(cache);
        result.sort_by_key(|v| v.dispatch_time);
        result.reverse();
        result
    }

    /// Create a channel that will get state updates as soon as they are available.
    /// This needs `periodically_sync_state` running in a coroutine.  That is
    /// statically ensured by the different type of this impl.
    pub fn subscribe_to_state_updates(&self) -> watch::Receiver<CurrentState> {
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
        max_rollouts: usize,
    ) -> Result<(v2::RolloutEngineStates, v2::Rollouts), RolloutDataGatherError> {
        let mut cache = self.cache.lock().await;
        let dag_id = "rollout_ic_os_to_mainnet_subnets";
        // FIXME once rollouts have been assembled of multiple types
        // they must be sorted by dispatch date/time. This is
        // because they already come sorted from DAG run query,
        // but obviously that is not correct.  Or maybe I will
        // simply just query more than one DAG run ID (single
        // query for both DAGs) and that solves the sort issue.

        // Query the log to see what has changed that isn't expressed in the
        // task updated, started or finished fields.
        let (updated_log_inspector, dag_run_update_types) = cache
            .log_inspector
            .incrementally_detect_dag_updates(&self.airflow_api, dag_id)
            .await?;

        // Retrieve the latest X DAG runs.
        let engine_states: v2::RolloutEngineStates = self
            .airflow_api
            .dags(
                1000,
                0,
                &DagsQueryFilter {
                    dag_id_pattern: Some(&dag_id.to_string()),
                },
                None,
            )
            .await?
            .into();

        // Retrieve the latest X DAG runs.
        let dag_runs = self
            .airflow_api
            .dag_runs(dag_id, max_rollouts, 0, None, None)
            .await?;

        let dag_tuples: Vec<_> = dag_runs
            .dag_runs
            .iter()
            .map(|dag_run| {
                (
                    dag_run,
                    dag_run_update_types.update_type(&dag_run.dag_run_id),
                )
            })
            .collect();

        // Get the tasks of the DAG, and assemble them into a topological
        // sorter, so we can process the task instances in the right order.
        // Note: perhaps if the number of tasks, or the names of tasks
        // have changed, we would want to reset the task instance cache
        // and re-request everything again.
        let sorter = TaskInstanceTopologicalSorter::new(self.airflow_api.tasks(dag_id).await?)?;

        let mut updaters: Vec<_> = dag_tuples
            .iter()
            .map(|(dag_run, update_type)| {
                let cache_key = dag_run.dag_id.clone() + &dag_run.dag_run_id;
                (
                    guestos_rollout::RolloutUpdater::new(
                        dag_run,
                        cache
                            .by_dag_run
                            .get(&cache_key)
                            .map(ToOwned::to_owned)
                            .unwrap_or(guestos_rollout::RolloutInfoCache::from(*dag_run)), // Add a default one to the cache if it is not found.
                    ),
                    update_type,
                    cache_key,
                )
            })
            .collect();

        let updateds = updaters
            .iter_mut()
            .map(|(updater, update_type, cache_key)| {
                let sorter = sorter.clone();
                (
                    updater.update(
                        &self.airflow_api,
                        sorter,
                        cache.log_inspector.last_event_log_update,
                        update_type,
                    ),
                    cache_key,
                )
            })
            .collect::<Vec<_>>();

        let mut res: v2::Rollouts = VecDeque::new();
        for fut in updateds.into_iter() {
            let (fut, cache_key) = fut;
            let (rollout, cache_entry) = fut.await?;
            cache.by_dag_run.insert(cache_key.clone(), cache_entry);
            res.push_back(rollout.into());
        }

        // Save the state of the log inspector after everything was successful.
        cache.log_inspector = updated_log_inspector;

        Ok((engine_states, res))
    }

    async fn sync_state(&self, max_rollouts: usize) -> CurrentState {
        match self.update(max_rollouts).await {
            Ok((engine_state, rollouts)) => v2::StateResponse::State(v2::State {
                rollout_engine_states: engine_state,
                rollouts,
            }),
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
                    RolloutDataGatherError::CyclicDependency(dep) => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("A cyclic dependency was found in the task graph: {:?}", dep),
                    ),
                };
                v2::StateResponse::Error(v2::Error {
                    code: res.0,
                    message: res.1,
                })
            }
        }
    }

    async fn periodically_sync_state<F>(self: Arc<Self>, cancel: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut errored = false;
        tokio::pin!(cancel);

        loop {
            let loop_start_time: DateTime<Utc> = Utc::now();

            let d = select! {
                d = self.sync_state(self.max_rollouts) => {
                    match &d {
                        v2::StateResponse::State(v2::State {rollouts, ..}) => {
                            let loop_delta_time = Utc::now() - loop_start_time;
                            info!(target: "rollout_data::state_syncer", "{} rollouts collected after {}.  Sleeping for {} seconds.", rollouts.len(), loop_delta_time, self.refresh_interval);
                            if errored {
                                info!(target: "rollout_data::state_syncer", "Successfully processed rollout data again after temporary error");
                                // Clear error flag.
                                errored = false;
                                // Ensure our data structure is overwritten by whatever data we obtained after the last loop.
                            }
                        }
                        v2::StateResponse::Error(v2::Error{ message, ..}) => {
                            error!(
                                target: "rollout_data::state_syncer", "After processing fetch_rollout_data: {}",
                               message
                            );
                            errored = true;
                        }
                    }
                    d
                },
                _ = &mut cancel => break,
            };

            let _ = self.stream_tx.send_replace(d.clone());
            let mut current_rollout_data = self.current_state.lock().await;
            *current_rollout_data = d;
            drop(current_rollout_data);

            select! {
                _ = sleep(Duration::from_secs(self.refresh_interval)) => (),
                _ = &mut cancel => break,
            }
        }
    }
}
