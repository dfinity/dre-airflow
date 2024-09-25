use crate::airflow_client::{
    AirflowClient, AirflowError, DagRunState, TaskInstanceRequestFilters, TaskInstanceState,
    TaskInstancesResponseItem, TasksResponse, TasksResponseItem,
};
use crate::python;
use chrono::{DateTime, Utc};
use futures::future::join_all;
use indexmap::IndexMap;
use lazy_static::lazy_static;
use log::{debug, trace, warn};
use regex::Regex;
use rollout_dashboard::types::{
    Batch, Rollout, RolloutState, Rollouts, Subnet, SubnetRolloutState,
};
use serde::Serialize;
use std::cmp::min;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::fmt::{self, Display};
use std::future::Future;
use std::num::ParseIntError;
use std::pin::Pin;
use std::rc::Rc;
use std::str::FromStr;
use std::sync::Arc;
use std::{vec, vec::Vec};
use tokio::sync::Mutex;
use topological_sort::TopologicalSort;

lazy_static! {
    // unwrap() is legitimate here because we know these cannot fail to compile.
    static ref SubnetGitRevisionRe: Regex = Regex::new("dfinity.ic_types.SubnetRolloutInstance.*@version=0[(]start_at=.*,subnet_id=([0-9-a-z-]+),git_revision=([0-9a-f]+)[)]").unwrap();
    static ref BatchIdentificationRe: Regex = Regex::new("batch_([0-9]+)[.](.+)").unwrap();
}

const TASK_INSTANCE_LIST_LIMIT: usize = 500;

#[derive(Debug)]
pub struct CyclicDependencyError {
    message: String,
}

impl Display for CyclicDependencyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

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

#[derive(Serialize, Debug)]
struct RolloutPlan {
    batches: IndexMap<usize, Batch>,
}

type PythonFormattedRolloutPlan = IndexMap<String, (String, Vec<String>)>;

#[derive(Debug)]
pub enum RolloutPlanParseError {
    UndecipherablePython(python::ErrorImpl),
    BadBatchNumber(ParseIntError),
    BadDateTime(chrono::format::ParseError),
    InvalidSubnet(String),
}

impl Display for RolloutPlanParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::UndecipherablePython(e) => {
                write!(f, "Invalid Python in rollout plan: {}", e)
            }
            Self::BadBatchNumber(e) => {
                write!(f, "Could not parse batch number in rollout plan: {}", e)
            }
            Self::BadDateTime(e) => {
                write!(f, "Could not parse date/time in rollout plan: {}", e)
            }
            Self::InvalidSubnet(e) => {
                write!(f, "Could not regex find subnets in {}", e)
            }
        }
    }
}

impl RolloutPlan {
    fn from_python_string(value: String) -> Result<Self, RolloutPlanParseError> {
        let mut res = RolloutPlan {
            batches: IndexMap::new(),
        };
        let python_string_plan: PythonFormattedRolloutPlan = match python::from_str(value.as_str())
        {
            Ok(s) => s,
            Err(e) => return Err(RolloutPlanParseError::UndecipherablePython(e)),
        };
        for (batch_number_str, (start_time_str, subnets)) in python_string_plan.iter() {
            let batch_number: usize = usize::from_str(batch_number_str)
                .map_err(RolloutPlanParseError::BadBatchNumber)?
                + 1;
            let start_time: DateTime<Utc> = match DateTime::parse_from_str(
                start_time_str.as_str(),
                "datetime.datetime@version=1(timestamp=%s%.f,tz=UTC)",
            ) {
                Ok(s) => Ok(s.with_timezone(&Utc)),
                Err(_e) => match DateTime::parse_from_str(
                    start_time_str.as_str(),
                    "datetime.datetime@version=2(timestamp=%s%.f,tz=(UTC,pendulum.tz.timezone.FixedTimezone,1,True))",
                ) {
                    Err(e) => Err(RolloutPlanParseError::BadDateTime(e)),
                    Ok(s) => Ok(s.with_timezone(&Utc)),
                },
            }?;

            let mut final_subnets: Vec<Subnet> = vec![];
            for subnet in subnets.iter() {
                final_subnets.push(match SubnetGitRevisionRe.captures(subnet) {
                    Some(capped) => Subnet {
                        subnet_id: capped[1].to_string(),
                        git_revision: capped[2].to_string(),
                        state: SubnetRolloutState::Unknown,
                        comment: "".to_string(),
                        display_url: "".to_string(),
                    },
                    None => return Err(RolloutPlanParseError::InvalidSubnet(subnet.clone())),
                });
            }
            let batch = Batch {
                planned_start_time: start_time,
                actual_start_time: None,
                end_time: None,
                subnets: final_subnets,
            };
            res.batches.insert(batch_number, batch);
        }
        Ok(res)
    }
}

#[derive(Debug)]
pub enum RolloutDataGatherError {
    AirflowError(AirflowError),
    CyclicDependency(CyclicDependencyError),
    RolloutPlanParseError(RolloutPlanParseError),
}

impl From<AirflowError> for RolloutDataGatherError {
    fn from(err: AirflowError) -> Self {
        Self::AirflowError(err)
    }
}

impl From<RolloutPlanParseError> for RolloutDataGatherError {
    fn from(err: RolloutPlanParseError) -> Self {
        Self::RolloutPlanParseError(err)
    }
}

impl From<CyclicDependencyError> for RolloutDataGatherError {
    fn from(err: CyclicDependencyError) -> Self {
        Self::CyclicDependency(err)
    }
}

enum ScheduleCache {
    Empty,
    Invalid,
    Valid(String),
}
struct RolloutDataCache {
    task_instances: HashMap<String, HashMap<Option<usize>, TaskInstancesResponseItem>>,
    dispatch_time: DateTime<Utc>,
    note: Option<String>,
    schedule: ScheduleCache,
    last_update_time: Option<DateTime<Utc>>,
}

struct RolloutApiCache {
    /// Map from DAG run ID to task instance ID (with / without index)
    /// to task instance.
    by_dag_run: HashMap<String, RolloutDataCache>,
}

fn format_some<N>(opt: Option<N>, prefix: &str, fallback: &str) -> String
where
    N: Display,
{
    match opt {
        None => fallback.to_string(),
        Some(v) => format!("{}{}", prefix, v),
    }
}

fn annotate_subnet_state(
    batch: &mut Batch,
    state: SubnetRolloutState,
    task_instance: &TaskInstancesResponseItem,
    base_url: &reqwest::Url,
    only_decrease: bool,
) -> SubnetRolloutState {
    for subnet in match task_instance.map_index {
        None => batch.subnets.iter_mut(),
        Some(index) => batch.subnets[index..=index].iter_mut(),
    } {
        let new_state = state.clone();
        if (only_decrease && new_state < subnet.state)
            || (!only_decrease && new_state != subnet.state)
        {
            trace!(target: "subnet_state", "{}: {} {:?} transition {} => {}   note: {}", task_instance.dag_run_id, task_instance.task_id, task_instance.map_index, subnet.state, new_state, subnet.comment);
            subnet.state = new_state.clone();
        } else {
            trace!(target: "subnet_state", "{}: {} {:?} NO transition {} => {}", task_instance.dag_run_id, task_instance.task_id, task_instance.map_index, subnet.state, new_state);
        }
        if new_state == subnet.state {
            subnet.comment = format!(
                "Task {}{} {}",
                task_instance.task_id,
                format_some(task_instance.map_index, ".", ""),
                format_some(
                    task_instance.state.clone(),
                    "in state ",
                    "has no known state"
                ),
            );
            subnet.display_url = {
                let mut url = base_url
                    .join(format!("/dags/{}/grid", task_instance.dag_id).as_str())
                    .unwrap();
                url.query_pairs_mut()
                    .append_pair("dag_run_id", &task_instance.dag_run_id);
                url.query_pairs_mut()
                    .append_pair("task_id", &task_instance.task_id);
                url.query_pairs_mut().append_pair("tab", "logs");
                if let Some(idx) = task_instance.map_index {
                    url.query_pairs_mut()
                        .append_pair("map_index", format!("{}", idx).as_str());
                };
                url.to_string()
            };
        };
    }
    state
}

#[derive(Clone)]
pub struct RolloutApi {
    airflow_api: Arc<AirflowClient>,
    cache: Arc<Mutex<RolloutApiCache>>,
}

impl RolloutApi {
    pub fn new(client: AirflowClient) -> Self {
        Self {
            airflow_api: Arc::new(client),
            cache: Arc::new(Mutex::new(RolloutApiCache {
                by_dag_run: HashMap::new(),
            })),
        }
    }

    /// Retrieve all rollout data, using a cache to avoid
    /// re-fetching task instances not updated since last time.
    ///
    /// Returns a tuple of the the rollout data and a flag
    /// indicating if the rollout data was updated since
    /// the last time.  The flag should be used by calling
    /// code to decide whether to send data to clients or not.
    ///
    /// The rollout structure itself is updated on every call
    /// for every DAG run.  However, not every change in the DAG
    /// run is considered to be a meaningful change (causing a
    /// true return in the update flag).  Currently, only a change
    /// in the rollout note, the state of any of its tasks, or
    /// the rollout dispatch time are considered meaningful changes.
    pub async fn get_rollout_data(
        &self,
        max_rollouts: usize,
    ) -> Result<(Rollouts, bool), RolloutDataGatherError> {
        let mut cache = self.cache.lock().await;

        let dag_id = "rollout_ic_os_to_mainnet_subnets";
        let dag_runs = self
            .airflow_api
            .dag_runs(dag_id, max_rollouts, 0, None, None)
            .await?;
        let tasks = self.airflow_api.tasks(dag_id).await?;
        // Note: perhaps if the number of tasks, or the names of tasks
        // have changed, we would want to reset the task instance cache
        // and re-request everything again.
        let sorter = TaskInstanceTopologicalSorter::new(tasks)?;

        let mut res: Rollouts = vec![];
        // Track if any rollout has had any meaningful changes.
        // Also see function documentation about meaningful changes.
        let mut meaningful_updates_to_any_rollout = false;

        for dag_run in dag_runs.dag_runs.iter() {
            let cache_entry = cache
                .by_dag_run
                .entry(dag_run.dag_run_id.clone())
                .or_insert(RolloutDataCache {
                    task_instances: HashMap::new(),
                    dispatch_time: dag_run.logical_date,
                    note: dag_run.note.clone(),
                    schedule: ScheduleCache::Empty,
                    last_update_time: None,
                });

            type TaskInstanceResponse = Result<Vec<TaskInstancesResponseItem>, AirflowError>;

            let last_update_time = cache_entry.last_update_time;
            let now = Utc::now();
            let requests: Vec<Pin<Box<dyn Future<Output = TaskInstanceResponse> + Send>>> = vec![
                Box::pin(async move {
                    match self
                        .airflow_api
                        .task_instances(
                            dag_id,
                            dag_run.dag_run_id.as_str(),
                            TASK_INSTANCE_LIST_LIMIT,
                            0,
                            TaskInstanceRequestFilters::default()
                                .executed_on_or_after(last_update_time),
                        )
                        .await
                    {
                        Ok(r) => Ok(r.task_instances),
                        Err(e) => Err(e),
                    }
                }),
                Box::pin(async move {
                    match last_update_time {
                        None => Ok(vec![]),
                        Some(_) => {
                            match self
                                .airflow_api
                                .task_instances(
                                    dag_id,
                                    dag_run.dag_run_id.as_str(),
                                    TASK_INSTANCE_LIST_LIMIT,
                                    0,
                                    TaskInstanceRequestFilters::default()
                                        .updated_on_or_after(last_update_time),
                                )
                                .await
                            {
                                Ok(r) => Ok(r.task_instances),
                                Err(e) => Err(e),
                            }
                        }
                    }
                }),
                Box::pin(async move {
                    match last_update_time {
                        None => Ok(vec![]),
                        Some(_) => {
                            match self
                                .airflow_api
                                .task_instances(
                                    dag_id,
                                    dag_run.dag_run_id.as_str(),
                                    TASK_INSTANCE_LIST_LIMIT,
                                    0,
                                    TaskInstanceRequestFilters::default()
                                        .ended_on_or_after(last_update_time),
                                )
                                .await
                            {
                                Ok(r) => Ok(r.task_instances),
                                Err(e) => Err(e),
                            }
                        }
                    }
                }),
            ];

            let results = join_all(requests).await;
            let mut all_task_instances: Vec<TaskInstancesResponseItem> = vec![];
            for r in results.into_iter() {
                all_task_instances.append(&mut r?)
            }

            debug!(
                target: "frontend_api", "{}: Undeduplicated tasks: {}",
                dag_run.dag_run_id, all_task_instances.len()
            );

            let rollout_had_changed_tasks = match all_task_instances.is_empty() {
                true => false,
                false => {
                    // At least one task has updated or finished.
                    // See function documentation about meaningful changes.
                    meaningful_updates_to_any_rollout = true;
                    // Now remember this rollout was updated.
                    true
                }
            };

            // If the note of the rollout has changed,
            // note that this has been updated.
            if cache_entry.note != dag_run.note {
                meaningful_updates_to_any_rollout = true;
                cache_entry.note.clone_from(&dag_run.note);
            }
            // Same for the dispatch time.
            if cache_entry.dispatch_time != dag_run.logical_date {
                meaningful_updates_to_any_rollout = true;
                cache_entry.dispatch_time = dag_run.logical_date;
            }

            let mut rollout = Rollout::new(
                dag_run.dag_run_id.to_string(),
                {
                    let mut display_url = self
                        .airflow_api
                        .as_ref()
                        .url
                        .join(format!("/dags/{}/grid", dag_run.dag_id).as_str())
                        .unwrap();
                    display_url
                        .query_pairs_mut()
                        .append_pair("dag_run_id", dag_run.dag_run_id.as_str());
                    display_url.to_string()
                },
                // Use recently-updated cache values here.
                // See function documentation about meaningful changes.
                cache_entry.note.clone(),
                cache_entry.dispatch_time,
                dag_run.last_scheduling_decision,
                dag_run.conf.clone(),
            );

            // Let's update the cache to incorporate the most up-to-date task instances.
            for task_instance in all_task_instances.into_iter() {
                let task_instance_id = task_instance.task_id.clone();
                if task_instance_id == "schedule" {
                    cache_entry.schedule = ScheduleCache::Invalid;
                }

                let by_name = cache_entry
                    .task_instances
                    .entry(task_instance_id)
                    .or_insert(HashMap::new());

                match by_name.entry(task_instance.map_index) {
                    Vacant(entry) => {
                        entry.insert(task_instance);
                    }
                    Occupied(mut entry) => {
                        if task_instance.latest_date() > entry.get().latest_date() {
                            entry.insert(task_instance.clone());
                        }
                    }
                };
            }

            for (task_instance_id, tasks) in cache_entry.task_instances.iter_mut() {
                // Delete data on all unmapped tasks if a mapped task sibling is present.
                if tasks.len() > 1 {
                    if let Occupied(_) = tasks.entry(None) {
                        debug!(
                            target: "frontend_api", "Formerly unmapped task {} is now mapped",
                            task_instance_id
                        );
                        tasks.remove(&None);
                    }
                }
            }

            let linearized_tasks: Vec<TaskInstancesResponseItem> = cache_entry
                .task_instances
                .iter()
                .flat_map(|(_, tasks)| tasks.iter().map(|(_, task)| task.clone()))
                .collect();

            debug!(
                target: "frontend_api", "{}: Total disambiguated tasks including locally cached ones: {}",
                dag_run.dag_run_id, linearized_tasks.len(),
            );

            // Now update rollout and batch state based on the obtained data.
            // What this process does is fairly straightforward:
            // * for each and every known up-to-date Airflow task in the cache
            //   (always processed in topological order),
            for task_instance in sorter.sort_instances(linearized_tasks.into_iter()) {
                // * deduce the rollout plan, if available,
                // * mark the rollout as having problems or errors depending on what
                //   the task state is, or as one of the various running states, if
                //   any  non-subnet-related task is running / pending.
                // * handle tasks corresponding to a batch/subnet in a special way
                //   (commented below in its pertinent section).
                if task_instance.task_id == "schedule" {
                    match task_instance.state {
                        Some(TaskInstanceState::Skipped) | Some(TaskInstanceState::Removed) => (),
                        Some(TaskInstanceState::UpForRetry)
                        | Some(TaskInstanceState::Restarting) => {
                            rollout.state = RolloutState::Problem;
                        }
                        Some(TaskInstanceState::Failed)
                        | Some(TaskInstanceState::UpstreamFailed) => {
                            rollout.state = RolloutState::Failed;
                        }
                        Some(TaskInstanceState::UpForReschedule)
                        | Some(TaskInstanceState::Running)
                        | Some(TaskInstanceState::Deferred)
                        | Some(TaskInstanceState::Queued)
                        | Some(TaskInstanceState::Scheduled)
                        | None => rollout.state = min(rollout.state, RolloutState::Preparing),
                        Some(TaskInstanceState::Success) => {
                            let schedule_string = match &cache_entry.schedule {
                                ScheduleCache::Valid(s) => s,
                                ScheduleCache::Invalid | ScheduleCache::Empty => {
                                    let value = self
                                        .airflow_api
                                        .xcom_entry(
                                            dag_id,
                                            dag_run.dag_run_id.as_str(),
                                            task_instance.task_id.as_str(),
                                            task_instance.map_index,
                                            "return_value",
                                        )
                                        .await;
                                    let schedule = match value {
                                        Ok(schedule) => {
                                            cache_entry.schedule =
                                                ScheduleCache::Valid(schedule.value.clone());
                                            schedule.value
                                        }
                                        Err(AirflowError::StatusCode(
                                            reqwest::StatusCode::NOT_FOUND,
                                        )) => {
                                            // There is no schedule to be found.
                                            // Or there was no schedule to be found last time
                                            // it was queried.
                                            cache_entry.schedule = ScheduleCache::Empty;
                                            continue;
                                        }
                                        Err(e) => {
                                            return Err(RolloutDataGatherError::AirflowError(e));
                                        }
                                    };
                                    &schedule.clone()
                                }
                            };
                            let schedule =
                                RolloutPlan::from_python_string(schedule_string.clone())?;
                            rollout.batches = schedule.batches;
                        }
                    }
                } else if task_instance.task_id == "wait_for_other_rollouts"
                    || task_instance.task_id == "wait_for_revision_to_be_elected"
                    || task_instance.task_id == "revisions"
                {
                    match task_instance.state {
                        Some(TaskInstanceState::Skipped) | Some(TaskInstanceState::Removed) => (),
                        Some(TaskInstanceState::UpForRetry)
                        | Some(TaskInstanceState::Restarting) => {
                            rollout.state = RolloutState::Problem;
                        }
                        Some(TaskInstanceState::Failed)
                        | Some(TaskInstanceState::UpstreamFailed) => {
                            rollout.state = RolloutState::Failed;
                        }
                        Some(TaskInstanceState::UpForReschedule)
                        | Some(TaskInstanceState::Running)
                        | Some(TaskInstanceState::Deferred)
                        | Some(TaskInstanceState::Queued)
                        | Some(TaskInstanceState::Scheduled)
                        | None => rollout.state = min(rollout.state, RolloutState::Waiting),
                        Some(TaskInstanceState::Success) => {}
                    }
                } else if let Some(captured) =
                    BatchIdentificationRe.captures(task_instance.task_id.as_str())
                {
                    // Handling of subnet state:
                    // * for each Airflow task that pertains to a rollout batch,
                    // * if its state in cache differs (or in some cases is higher) from the
                    //   corresponding subnet state, upgrade the subnet state to be the correct
                    //   state,
                    // * update the subnet link to the corresponding Airflow task if the
                    //   state of the task (after update) corresponds to the expected state,
                    // * update rollout state to problem / error depending on the task state.
                    trace!(target: "subnet_state", "{}: processing {} {:?} in state {:?}", task_instance.dag_run_id, task_instance.task_id, task_instance.map_index, task_instance.state);
                    let (batch, task_name) = (
                        // We get away with unwrap() here because we know we captured an integer.
                        match rollout
                            .batches
                            .get_mut(&usize::from_str(&captured[1]).unwrap())
                        {
                            Some(batch) => batch,
                            None => {
                                trace!(target: "subnet_state", "{}: no corresponding batch, continuing", task_instance.dag_run_id);
                                continue;
                            }
                        },
                        &captured[2],
                    );

                    macro_rules! trans_min {
                        ($input:expr) => {
                            annotate_subnet_state(
                                batch,
                                $input,
                                &task_instance,
                                &self.airflow_api.as_ref().url,
                                true,
                            )
                        };
                    }
                    macro_rules! trans_exact {
                        ($input:expr) => {
                            annotate_subnet_state(
                                batch,
                                $input,
                                &task_instance,
                                &self.airflow_api.as_ref().url,
                                false,
                            )
                        };
                    }

                    // FIXME: perhaps we want to destructure both the task name
                    // and the task state here.
                    match &task_instance.state {
                        None => {
                            if task_name == "collect_batch_subnets" {
                                trans_exact!(SubnetRolloutState::Pending);
                            } else {
                                trace!(target: "subnet_state", "{}: ignoring task instance {} {:?} with no state", task_instance.dag_run_id, task_instance.task_id, task_instance.map_index);
                            }
                        }
                        Some(state) => match state {
                            // https://stackoverflow.com/questions/53654302/tasks-are-moved-to-removed-state-in-airflow-when-they-are-queued-and-not-restore
                            // If a task is removed, we cannot decide rollout state based on it.
                            // https://stackoverflow.com/questions/77426996/skipping-a-task-in-airflow
                            // If a task is skipped, the next task (in state Running / Deferred)
                            // will pick up the slack for changing subnet state.
                            TaskInstanceState::Removed | TaskInstanceState::Skipped => {
                                trace!(target: "subnet_state", "{}: ignoring task instance {} {:?} in state {:?}", task_instance.dag_run_id, task_instance.task_id, task_instance.map_index, task_instance.state);
                            }
                            TaskInstanceState::UpForRetry | TaskInstanceState::Restarting => {
                                trans_min!(SubnetRolloutState::Error);
                                rollout.state = min(rollout.state, RolloutState::Problem)
                            }
                            TaskInstanceState::Failed => {
                                trans_min!(SubnetRolloutState::Error);
                                rollout.state = min(rollout.state, RolloutState::Failed)
                            }
                            TaskInstanceState::UpstreamFailed => {
                                trans_min!(SubnetRolloutState::PredecessorFailed);
                                rollout.state = min(rollout.state, RolloutState::Failed)
                            }
                            TaskInstanceState::UpForReschedule
                            | TaskInstanceState::Running
                            | TaskInstanceState::Deferred
                            | TaskInstanceState::Queued
                            | TaskInstanceState::Scheduled => {
                                match task_name {
                                    "collect_batch_subnets" => {
                                        trans_min!(SubnetRolloutState::Pending);
                                    }
                                    "wait_until_start_time" => {
                                        trans_min!(SubnetRolloutState::Waiting);
                                    }
                                    "wait_for_preconditions" => {
                                        trans_min!(SubnetRolloutState::Waiting);
                                    }
                                    "create_proposal_if_none_exists" => {
                                        trans_min!(SubnetRolloutState::Proposing);
                                    }
                                    "request_proposal_vote" => {
                                        // We ignore this one for the purposes of rollout state setup.
                                    }
                                    "wait_until_proposal_is_accepted" => {
                                        trans_min!(SubnetRolloutState::WaitingForElection);
                                    }
                                    "wait_for_replica_revision" => {
                                        trans_min!(SubnetRolloutState::WaitingForAdoption);
                                    }
                                    "wait_until_no_alerts" => {
                                        trans_min!(SubnetRolloutState::WaitingForAlertsGone);
                                    }
                                    "join" => {
                                        trans_min!(SubnetRolloutState::Complete);
                                    }
                                    &_ => {
                                        warn!(target: "subnet_state", "{}: no info on to handle task instance {} {:?} in state {:?}", task_instance.dag_run_id, task_instance.task_id, task_instance.map_index, task_instance.state);
                                    }
                                }
                                rollout.state = min(rollout.state, RolloutState::UpgradingSubnets)
                            }
                            TaskInstanceState::Success => match task_name {
                                // Tasks corresponding to a subnet that are in state Success
                                // require somewhat different handling than tasks in states
                                // Running et al.  For once, when a task is successful,
                                // the subnet state must be set to the *next* state it *would*
                                // have, if the /next/ task had /already begun executing/.
                                //
                                // To give an example: if `wait_until_start_time`` is Success,
                                // the subnet state is no longer "waiting until start time",
                                // but rather should be "creating proposal", even though
                                // perhaps `create_proposal_if_none_exists`` /has not yet run/
                                // because we know certainly that the
                                // `create_proposal_if_none_exists` task is /about to run/
                                // anyway.
                                //
                                // The same principle applies for all tasks -- if the current
                                // task is successful, we set the state of the subnet to the
                                // expected state that corresponds to the successor task.
                                //
                                // We could avoid encoding this type of knowledge here, by
                                // having a table of Airflow tasks vs. expected subnet states,
                                // and as a special case, on the task Success case, look up the
                                // successor task on the table to decide what subnet state to
                                // assign, but this would require a data structure different
                                // from the current (a vector of ordered task instances) to
                                // iterate over.  This refactor may happen in the future, and
                                // it will require extra tests to ensure that invariants have
                                // been preserved between this code (which works well) and
                                // the future rewrite.
                                "collect_batch_subnets" => {
                                    trans_min!(SubnetRolloutState::Waiting);
                                }
                                "wait_until_start_time" => {
                                    batch.actual_start_time = match task_instance.end_date {
                                        None => batch.actual_start_time,
                                        Some(end_date) => {
                                            if batch.actual_start_time.is_none() {
                                                Some(end_date)
                                            } else {
                                                let stime = batch.actual_start_time.unwrap();
                                                Some(min(stime, end_date))
                                            }
                                        }
                                    };
                                    trans_exact!(SubnetRolloutState::Waiting);
                                }
                                "wait_for_preconditions" => {
                                    trans_exact!(SubnetRolloutState::Proposing);
                                }
                                "create_proposal_if_none_exists" => {
                                    trans_exact!(SubnetRolloutState::WaitingForElection);
                                }
                                "request_proposal_vote" => {
                                    // We ignore this one for the purposes of rollout state setup.
                                }
                                "wait_until_proposal_is_accepted" => {
                                    trans_exact!(SubnetRolloutState::WaitingForAdoption);
                                }
                                "wait_for_replica_revision" => {
                                    trans_exact!(SubnetRolloutState::WaitingForAlertsGone);
                                }
                                "wait_until_no_alerts" => {
                                    trans_exact!(SubnetRolloutState::Complete);
                                }
                                "join" => {
                                    trans_exact!(SubnetRolloutState::Complete);
                                    batch.end_time = task_instance.end_date;
                                }
                                &_ => {
                                    warn!(target: "subnet_state", "{}: no info on how to handle task instance {} {:?} in state {:?}", task_instance.dag_run_id, task_instance.task_id, task_instance.map_index, task_instance.state);
                                }
                            },
                        },
                    }
                } else if task_instance.task_id == "upgrade_unassigned_nodes" {
                    match task_instance.state {
                        Some(TaskInstanceState::Skipped) | Some(TaskInstanceState::Removed) => (),
                        Some(TaskInstanceState::UpForRetry)
                        | Some(TaskInstanceState::Restarting) => {
                            rollout.state = RolloutState::Problem
                        }
                        Some(TaskInstanceState::Failed)
                        | Some(TaskInstanceState::UpstreamFailed) => {
                            rollout.state = RolloutState::Failed
                        }
                        Some(TaskInstanceState::UpForReschedule)
                        | Some(TaskInstanceState::Running)
                        | Some(TaskInstanceState::Deferred)
                        | Some(TaskInstanceState::Queued)
                        | Some(TaskInstanceState::Scheduled)
                        | Some(TaskInstanceState::Success)
                        | None => {
                            rollout.state =
                                min(rollout.state, RolloutState::UpgradingUnassignedNodes)
                        }
                    }
                } else {
                    warn!(target: "frontend_api", "{}: unknown task {}", task_instance.dag_run_id, task_instance.task_id)
                }
            }

            if let Some(state) = Some(&dag_run.state) {
                match state {
                    DagRunState::Success => rollout.state = RolloutState::Complete,
                    DagRunState::Failed => rollout.state = RolloutState::Failed,
                    _ => (),
                }
            }

            if rollout_had_changed_tasks {
                // We bump the cache entry's last update time, to only retrieve
                // tasks from this point in time on during subsequent retrievals.
                // We only do this at the end, in case any code above returns
                // early, to force a full state recalculation if there was a
                // failure or an early return.
                cache_entry.last_update_time = Some(now);
            }

            res.push(rollout);
        }

        Ok((res, meaningful_updates_to_any_rollout))
    }
}
