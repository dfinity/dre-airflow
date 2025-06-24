use super::{
    max_option_date, python, DagRunUpdateType, FlowCacheViewable, RolloutDataGatherError,
    TaskInstanceTopologicalSorter, TASK_INSTANCE_LIST_LIMIT,
};
use chrono::{DateTime, Utc};
use futures::future::join_all;
use indexmap::IndexMap;
use lazy_static::lazy_static;
use log::{debug, info, trace, warn};
use regex::Regex;
use rollout_dashboard::airflow_client::{
    AirflowClient, AirflowError, DagRunState, DagRunsResponseItem, TaskInstanceRequestFilters,
    TaskInstanceState, TaskInstancesResponseItem,
};
use rollout_dashboard::types::v2::{
    Batch, DAGInfo, RolloutIcOsToMainnetSubnets, RolloutIcOsToMainnetSubnetsState as State, Subnet,
    SubnetRolloutState as SubnetState,
};
use std::cmp::min;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::fmt::{self, Display};
use std::future::Future;
use std::num::ParseIntError;
use std::pin::Pin;
use std::str::FromStr;
use std::{vec, vec::Vec};

lazy_static! {
    // unwrap() is legitimate here because we know these cannot fail to compile.
    static ref SubnetGitRevisionRe: Regex = Regex::new("dfinity.ic_types.SubnetRolloutInstance.*@version=0[(]start_at=.*,subnet_id=([0-9-a-z-]+),git_revision=([0-9a-f]+)[)]").unwrap();
    static ref BatchIdentificationRe: Regex = Regex::new("batch_([0-9]+)[.](.+)").unwrap();
}

type PythonFormattedPlan = IndexMap<String, (String, Vec<String>)>;

type BatchMap = IndexMap<usize, Batch>;

#[derive(Clone)]
enum ScheduleStateForTask {
    Missing,
    Invalid,
    Valid { batch_map: BatchMap },
}

#[derive(Clone, Default)]
enum Schedule {
    #[default]
    Unretrieved,
    RetrievedAtTaskState {
        try_number: usize,
        latest_date: DateTime<Utc>,
        state: ScheduleStateForTask,
    },
}

enum ScheduleFreshness {
    UpToDate(BatchMap),
    Stale,
    Invalid,
}

impl Schedule {
    /// Retrieve whether the schedule is fresh based on the info of the schedule
    /// task passed, and if up to date, return the cache contents.  Return Stale if the
    /// cache needs updating, and Invalid if the cache is up to date but
    /// the contents are not valid for use.
    fn freshness(&self, schedule_task_instance: &TaskInstancesResponseItem) -> ScheduleFreshness {
        match self {
            Schedule::Unretrieved => ScheduleFreshness::Stale,
            Schedule::RetrievedAtTaskState {
                try_number: t,
                latest_date: l,
                state: kind,
            } => {
                if *t == schedule_task_instance.try_number
                    && *l == schedule_task_instance.latest_date()
                {
                    match &kind {
                        ScheduleStateForTask::Valid {
                            batch_map: cached_schedule,
                        } => ScheduleFreshness::UpToDate(cached_schedule.clone()),
                        ScheduleStateForTask::Missing | ScheduleStateForTask::Invalid { .. } => {
                            ScheduleFreshness::Invalid
                        }
                    }
                } else {
                    // Same schedule task has been updated.  Data may not be missing anymore.
                    ScheduleFreshness::Stale
                }
            }
        }
    }

    /// Update the cache entry.
    fn update(&mut self, task_instance: &TaskInstancesResponseItem, batches: &BatchMap) {
        *self = Self::RetrievedAtTaskState {
            try_number: task_instance.try_number,
            latest_date: task_instance.latest_date(),
            state: ScheduleStateForTask::Valid {
                batch_map: batches.clone(),
            },
        }
    }

    /// Update the cache entry with (possibly) an invalid value.
    fn invalidate(&mut self, task_instance: &TaskInstancesResponseItem, schedule: Option<String>) {
        *self = Self::RetrievedAtTaskState {
            try_number: task_instance.try_number,
            latest_date: task_instance.latest_date(),
            state: match schedule {
                None => ScheduleStateForTask::Missing,
                Some(_) => ScheduleStateForTask::Invalid,
            },
        }
    }
}

#[derive(Clone)]
pub(super) struct RolloutInfoCache {
    task_instances: HashMap<String, HashMap<Option<usize>, TaskInstancesResponseItem>>,
    dispatch_time: DateTime<Utc>,
    note: Option<String>,
    schedule: Schedule,
    last_update_time: Option<DateTime<Utc>>,
    update_count: usize,
}

impl From<&DagRunsResponseItem> for RolloutInfoCache {
    fn from(dag_run: &DagRunsResponseItem) -> Self {
        Self {
            task_instances: HashMap::new(),
            dispatch_time: dag_run.logical_date,
            note: dag_run.note.clone(),
            schedule: Schedule::default(),
            last_update_time: None,
            update_count: 0,
        }
    }
}

impl FlowCacheViewable for RolloutInfoCache {
    fn get_task_instances(&self) -> Vec<TaskInstancesResponseItem> {
        self.task_instances
            .iter()
            .flat_map(|(_, tasks)| tasks.iter().map(|(_, task)| task.clone()))
            .collect()
    }

    fn get_flow_info(&self) -> (DateTime<Utc>, Option<DateTime<Utc>>, usize) {
        (self.dispatch_time, self.last_update_time, self.update_count)
    }
}

#[derive(Debug)]
pub enum PlanParseError {
    UndecipherablePython(python::ErrorImpl),
    BadBatchNumber(ParseIntError),
    BadDateTime(chrono::format::ParseError),
    InvalidSubnet(String),
}

impl Display for PlanParseError {
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

#[derive(Debug)]
struct Plan {
    batches: BatchMap,
}

impl Plan {
    fn from_python_string(value: &str) -> Result<Self, PlanParseError> {
        let mut res = Plan {
            batches: IndexMap::new(),
        };
        let python_string_plan: PythonFormattedPlan = match python::from_str(value) {
            Ok(s) => s,
            Err(e) => return Err(PlanParseError::UndecipherablePython(e)),
        };
        for (batch_number_str, (start_time_str, subnets)) in python_string_plan.iter() {
            let batch_number: usize =
                usize::from_str(batch_number_str).map_err(PlanParseError::BadBatchNumber)? + 1;
            let start_time: DateTime<Utc> = match DateTime::parse_from_str(
                start_time_str.as_str(),
                "datetime.datetime@version=1(timestamp=%s%.f,tz=UTC)",
            ) {
                Ok(s) => Ok(s.with_timezone(&Utc)),
                Err(_e) => match DateTime::parse_from_str(
                    start_time_str.as_str(),
                    "datetime.datetime@version=2(timestamp=%s%.f,tz=(UTC,pendulum.tz.timezone.FixedTimezone,1,True))",
                ) {
                    Err(e) => Err(PlanParseError::BadDateTime(e)),
                    Ok(s) => Ok(s.with_timezone(&Utc)),
                },
            }?;

            let mut final_subnets: Vec<Subnet> = vec![];
            for subnet in subnets.iter() {
                final_subnets.push(match SubnetGitRevisionRe.captures(subnet) {
                    Some(capped) => Subnet {
                        subnet_id: capped[1].to_string(),
                        git_revision: capped[2].to_string(),
                        state: SubnetState::Unknown,
                        comment: "".to_string(),
                        display_url: "".to_string(),
                    },
                    None => return Err(PlanParseError::InvalidSubnet(subnet.clone())),
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
    state: SubnetState,
    task_instance: &TaskInstancesResponseItem,
    base_url: &reqwest::Url,
    only_decrease: bool,
) -> SubnetState {
    for subnet in match task_instance.map_index {
        None => batch.subnets.iter_mut(),
        Some(index) => batch.subnets[index..=index].iter_mut(),
    } {
        let new_state = state.clone();
        if (only_decrease && new_state < subnet.state)
            || (!only_decrease && new_state != subnet.state)
        {
            trace!(target: "rollout_data::annotate_subnet_state", "{}: {} {:?} transition {} => {}   note: {}", task_instance.dag_run_id, task_instance.task_id, task_instance.map_index, subnet.state, new_state, subnet.comment);
            subnet.state = new_state.clone();
        } else {
            trace!(target: "rollout_data::annotate_subnet_state", "{}: {} {:?} NO transition {} => {}", task_instance.dag_run_id, task_instance.task_id, task_instance.map_index, subnet.state, new_state);
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

pub(super) struct RolloutUpdater<'a> {
    dag_run: &'a DagRunsResponseItem,
    cache_entry: RolloutInfoCache,
}

impl<'a> RolloutUpdater<'_> {
    pub(super) fn new(
        dag_run: &'a DagRunsResponseItem,
        cache_entry: RolloutInfoCache,
    ) -> RolloutUpdater<'a> {
        RolloutUpdater {
            dag_run,
            cache_entry,
        }
    }
}

impl<'a> RolloutUpdater<'a> {
    pub(super) async fn update(
        &mut self,
        airflow_api: &AirflowClient,
        sorter: TaskInstanceTopologicalSorter,
        last_event_log_update: Option<DateTime<Utc>>,
        dag_run_update_type: &DagRunUpdateType,
    ) -> Result<(RolloutIcOsToMainnetSubnets, RolloutInfoCache), RolloutDataGatherError> {
        // If the note of the rollout has changed,
        // note that this has been updated.
        let cache_entry = &mut self.cache_entry;
        let dag_run = &self.dag_run;

        // Same for the dispatch time.
        // In this case, the rollout was restarted completely.
        // We must evict the task cache.
        if cache_entry.dispatch_time != dag_run.logical_date {
            cache_entry.dispatch_time = dag_run.logical_date;
            cache_entry.schedule = Schedule::default();
            cache_entry.task_instances = HashMap::new();
            cache_entry.update_count += 1;
        }

        if cache_entry.note != dag_run.note {
            cache_entry.note.clone_from(&dag_run.note);
            cache_entry.update_count += 1;
        }

        type TaskInstanceResponse = Result<Vec<TaskInstancesResponseItem>, AirflowError>;

        let last_update_time = cache_entry.last_update_time;
        let dag_id = dag_run.dag_id.as_str();
        let dag_run_id = dag_run.dag_run_id.as_str();

        let requests: Vec<Pin<Box<dyn Future<Output = TaskInstanceResponse> + Send>>> = match (
            dag_run_update_type,
            cache_entry.task_instances.is_empty(),
        ) {
            (_, true) | (DagRunUpdateType::AllTaskInstances, _) => {
                debug!(target:"rollout_data::get_rollout_data", "{}: collecting data about all task instances", dag_run.dag_run_id);
                vec![Box::pin(async move {
                    match airflow_api
                        .task_instances(
                            dag_id,
                            dag_run.dag_run_id.as_str(),
                            TASK_INSTANCE_LIST_LIMIT,
                            0,
                            TaskInstanceRequestFilters::default(),
                        )
                        .await
                    {
                        Ok(r) => Ok(r.task_instances),
                        Err(e) => Err(e),
                    }
                })]
            }
            (DagRunUpdateType::SomeTaskInstances(updated_task_instances), false) => {
                let updated_task_instances =
                    updated_task_instances.iter().cloned().collect::<Vec<_>>();
                debug!(target:"rollout_data::get_rollout_data", "{}: collecting data about task instances updated since {:?} and a specific set of tasks too: {:?}", dag_run_id, last_update_time, updated_task_instances);
                vec![
                    Box::pin(async move {
                        match airflow_api
                            .task_instances_batch(
                                Some(vec![dag_id.to_string()]),
                                Some(vec![dag_run_id.to_string()]),
                                Some(updated_task_instances),
                            )
                            .await
                        {
                            Ok(r) => Ok(r.task_instances),
                            Err(e) => Err(e),
                        }
                    }),
                    Box::pin(async move {
                        match airflow_api
                            .task_instances(
                                dag_id,
                                dag_run_id,
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
                                match airflow_api
                                    .task_instances(
                                        dag_id,
                                        dag_run_id,
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
                                match airflow_api
                                    .task_instances(
                                        dag_id,
                                        dag_run_id,
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
                ]
            }
        };

        let mut retrieved_task_instances: Vec<TaskInstancesResponseItem> = vec![];
        for r in join_all(requests).await.into_iter() {
            retrieved_task_instances.append(&mut r?)
        }

        debug!(
            target: "rollout_data::get_rollout_data", "{}: retrieved {} tasks",
            dag_run_id, retrieved_task_instances.len()
        );

        let mut rollout = RolloutIcOsToMainnetSubnets {
            dag_info: DAGInfo {
                name: dag_run.dag_run_id.to_string(),
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
                // Use recently-updated cache values here.
                // See function documentation about meaningful changes.
                note: cache_entry.note.clone(),
                dispatch_time: cache_entry.dispatch_time,
                last_scheduling_decision: dag_run.last_scheduling_decision,
                update_count: cache_entry.update_count,
            },
            state: State::Complete,
            batches: IndexMap::new(),
            conf: dag_run.conf.clone(),
        };

        // Let's update the cache to incorporate the most up-to-date task instances.
        let mut new_last_update_time = max_option_date(last_update_time, last_event_log_update);
        for task_instance in retrieved_task_instances.into_iter() {
            let task_instance_id = task_instance.task_id.clone();
            new_last_update_time =
                max_option_date(new_last_update_time, Some(task_instance.latest_date()));

            let by_name = cache_entry
                .task_instances
                .entry(task_instance_id)
                .or_default();

            match by_name.entry(task_instance.map_index) {
                Vacant(entry) => {
                    entry.insert(task_instance);
                    rollout.updated();
                }
                Occupied(mut entry) => {
                    if task_instance.latest_date() > entry.get().latest_date()
                        || task_instance.state != entry.get().state
                        || task_instance.note != entry.get().note
                    {
                        entry.insert(task_instance.clone());
                        rollout.updated();
                    }
                }
            };
        }

        for (task_instance_id, tasks) in cache_entry.task_instances.iter_mut() {
            // Delete data on all unmapped tasks if a mapped task sibling is present.
            if tasks.len() > 1 {
                if let Occupied(_) = tasks.entry(None) {
                    debug!(
                        target: "rollout_data::get_rollout_data", "formerly unmapped task {} is now mapped",
                        task_instance_id
                    );
                    tasks.remove(&None);
                    rollout.updated();
                }
            }
        }

        let linearized_tasks: Vec<TaskInstancesResponseItem> = cache_entry
            .task_instances
            .iter()
            .flat_map(|(_, tasks)| tasks.iter().map(|(_, task)| task.clone()))
            .collect();

        debug!(
            target: "rollout_data::get_rollout_data", "{}: total disambiguated tasks including locally cached ones: {}",
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
            trace!(
                target: "rollout_data::get_rollout_data", "Processing task {}.{:?} in state {:?}",
                task_instance.task_id, task_instance.map_index, task_instance.state,
            );
            if task_instance.task_id == "schedule" {
                match task_instance.state {
                    Some(TaskInstanceState::Skipped) | Some(TaskInstanceState::Removed) => (),
                    Some(TaskInstanceState::UpForRetry) | Some(TaskInstanceState::Restarting) => {
                        rollout.state = State::Problem;
                    }
                    Some(TaskInstanceState::Failed) | Some(TaskInstanceState::UpstreamFailed) => {
                        rollout.state = State::Failed;
                    }
                    Some(TaskInstanceState::UpForReschedule)
                    | Some(TaskInstanceState::Running)
                    | Some(TaskInstanceState::Deferred)
                    | Some(TaskInstanceState::Queued)
                    | Some(TaskInstanceState::Scheduled)
                    | None => rollout.state = min(rollout.state, State::Preparing),
                    Some(TaskInstanceState::Success) => {
                        rollout.batches = match cache_entry.schedule.freshness(&task_instance) {
                            ScheduleFreshness::UpToDate(cache) => cache,
                            ScheduleFreshness::Invalid => {
                                // Nothing has changed.  Stop processing this task.
                                continue;
                            }
                            ScheduleFreshness::Stale => {
                                // Same schedule task has been updated.  Data may not be missing anymore.
                                info!(target: "rollout_data::get_rollout_data", "{}: schedule task is outdated; requerying", dag_run.dag_run_id);
                                match airflow_api
                                    .xcom_entry(
                                        dag_id,
                                        dag_run.dag_run_id.as_str(),
                                        task_instance.task_id.as_str(),
                                        task_instance.map_index,
                                        "return_value",
                                    )
                                    .await
                                {
                                    Ok(schedule) => {
                                        match &Plan::from_python_string(&schedule.value) {
                                            Ok(plan) => {
                                                info!(target: "rollout_data::get_rollout_data", "{}: saving schedule cache", dag_run.dag_run_id);
                                                cache_entry
                                                    .schedule
                                                    .update(&task_instance, &plan.batches);
                                                plan.batches.clone()
                                            }
                                            Err(e) => {
                                                warn!(target: "rollout_data::get_rollout_data", "{}: could not parse schedule data: {}", dag_run.dag_run_id, e);
                                                cache_entry.schedule.invalidate(
                                                    &task_instance,
                                                    Some(schedule.value),
                                                );
                                                continue;
                                            }
                                        }
                                    }
                                    Err(AirflowError::StatusCode(
                                        reqwest::StatusCode::NOT_FOUND,
                                    )) => {
                                        // There is no schedule to be found.
                                        // Or there was no schedule to be found last time
                                        // it was queried.
                                        warn!(target: "rollout_data::get_rollout_data", "{}: no schedule despite schedule task finished", dag_run.dag_run_id);
                                        cache_entry.schedule.invalidate(&task_instance, None);
                                        continue;
                                    }
                                    Err(e) => {
                                        // In this case the dashboard will try to requery in the future again.
                                        return Err(RolloutDataGatherError::AirflowError(e));
                                    }
                                }
                            }
                        };
                    }
                }
            } else if task_instance.task_id == "wait_for_other_rollouts"
                || task_instance.task_id == "wait_for_revision_to_be_elected"
                || task_instance.task_id == "revisions"
            {
                match task_instance.state {
                    Some(TaskInstanceState::Skipped) | Some(TaskInstanceState::Removed) => (),
                    Some(TaskInstanceState::UpForRetry) | Some(TaskInstanceState::Restarting) => {
                        rollout.state = State::Problem;
                    }
                    Some(TaskInstanceState::Failed) | Some(TaskInstanceState::UpstreamFailed) => {
                        rollout.state = State::Failed;
                    }
                    Some(TaskInstanceState::UpForReschedule)
                    | Some(TaskInstanceState::Running)
                    | Some(TaskInstanceState::Deferred)
                    | Some(TaskInstanceState::Queued)
                    | Some(TaskInstanceState::Scheduled)
                    | None => rollout.state = min(rollout.state, State::Waiting),
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
                trace!(target: "rollout_data::get_rollout_data::subnet_state", "{}: processing {} {:?} in state {:?}", task_instance.dag_run_id, task_instance.task_id, task_instance.map_index, task_instance.state);
                let (batch, task_name) = (
                    // We get away with unwrap() here because we know we captured an integer.
                    match rollout
                        .batches
                        .get_mut(&usize::from_str(&captured[1]).unwrap())
                    {
                        Some(batch) => batch,
                        None => {
                            trace!(target: "rollout_data::get_rollout_data::subnet_state", "{}: no corresponding batch, continuing", task_instance.dag_run_id);
                            continue;
                        }
                    },
                    &captured[2],
                );

                macro_rules! trans_min {
                    ($input:expr) => {
                        annotate_subnet_state(batch, $input, &task_instance, &airflow_api.url, true)
                    };
                }
                macro_rules! trans_exact {
                    ($input:expr) => {
                        annotate_subnet_state(
                            batch,
                            $input,
                            &task_instance,
                            &airflow_api.url,
                            false,
                        )
                    };
                }

                match &task_instance.state {
                    None => {
                        if task_name == "collect_batch_subnets" {
                            trans_exact!(SubnetState::Pending);
                        } else {
                            trace!(target: "rollout_data::get_rollout_data::subnet_state", "{}: ignoring task instance {} {:?} with no state", task_instance.dag_run_id, task_instance.task_id, task_instance.map_index);
                        }
                    }
                    Some(state) => match state {
                        // https://stackoverflow.com/questions/53654302/tasks-are-moved-to-removed-state-in-airflow-when-they-are-queued-and-not-restore
                        // If a task is removed, we cannot decide rollout state based on it.
                        // https://stackoverflow.com/questions/77426996/skipping-a-task-in-airflow
                        // If a task is skipped, the next task (in state Running / Deferred)
                        // will pick up the slack for changing subnet state.
                        TaskInstanceState::Removed | TaskInstanceState::Skipped => {
                            trace!(target: "rollout_data::get_rollout_data::subnet_state", "{}: ignoring task instance {} {:?} in state {:?}", task_instance.dag_run_id, task_instance.task_id, task_instance.map_index, task_instance.state);
                        }
                        TaskInstanceState::UpForRetry | TaskInstanceState::Restarting => {
                            trans_min!(SubnetState::Error);
                            rollout.state = min(rollout.state, State::Problem)
                        }
                        TaskInstanceState::Failed => {
                            trans_min!(SubnetState::Error);
                            rollout.state = min(rollout.state, State::Failed)
                        }
                        TaskInstanceState::UpstreamFailed => {
                            trans_min!(SubnetState::PredecessorFailed);
                            rollout.state = min(rollout.state, State::Failed)
                        }
                        TaskInstanceState::UpForReschedule
                        | TaskInstanceState::Running
                        | TaskInstanceState::Deferred
                        | TaskInstanceState::Queued
                        | TaskInstanceState::Scheduled => {
                            match task_name {
                                "collect_batch_subnets" => {
                                    trans_min!(SubnetState::Pending);
                                }
                                "wait_until_start_time" => {
                                    trans_min!(SubnetState::Waiting);
                                }
                                "wait_for_preconditions" => {
                                    trans_min!(SubnetState::Waiting);
                                }
                                "create_proposal_if_none_exists" => {
                                    trans_min!(SubnetState::Proposing);
                                }
                                "request_proposal_vote" => {
                                    // We ignore this one for the purposes of rollout state setup.
                                }
                                "wait_until_proposal_is_accepted" => {
                                    trans_min!(SubnetState::WaitingForElection);
                                }
                                "wait_for_replica_revision" => {
                                    trans_min!(SubnetState::WaitingForAdoption);
                                }
                                "wait_until_no_alerts" => {
                                    trans_min!(SubnetState::WaitingForAlertsGone);
                                }
                                "join" => {
                                    trans_min!(SubnetState::Complete);
                                }
                                &_ => {
                                    warn!(target: "rollout_data::get_rollout_data::subnet_state", "{}: no info on to handle task instance {} {:?} in state {:?}", task_instance.dag_run_id, task_instance.task_id, task_instance.map_index, task_instance.state);
                                }
                            }
                            rollout.state = min(rollout.state, State::UpgradingSubnets)
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
                                trans_min!(SubnetState::Waiting);
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
                                trans_exact!(SubnetState::Waiting);
                            }
                            "wait_for_preconditions" => {
                                trans_exact!(SubnetState::Proposing);
                            }
                            "create_proposal_if_none_exists" => {
                                trans_exact!(SubnetState::WaitingForElection);
                            }
                            "request_proposal_vote" => {
                                // We ignore this one for the purposes of rollout state setup.
                            }
                            "wait_until_proposal_is_accepted" => {
                                trans_exact!(SubnetState::WaitingForAdoption);
                            }
                            "wait_for_replica_revision" => {
                                trans_exact!(SubnetState::WaitingForAlertsGone);
                            }
                            "wait_until_no_alerts" => {
                                trans_exact!(SubnetState::Complete);
                            }
                            "join" => {
                                trans_exact!(SubnetState::Complete);
                                batch.end_time = task_instance.end_date;
                            }
                            &_ => {
                                warn!(target: "rollout_data::get_rollout_data::subnet_state", "{}: no info on how to handle task instance {} {:?} in state {:?}", task_instance.dag_run_id, task_instance.task_id, task_instance.map_index, task_instance.state);
                            }
                        },
                    },
                }
            } else if task_instance.task_id == "upgrade_unassigned_nodes" {
                match task_instance.state {
                    Some(TaskInstanceState::Skipped) | Some(TaskInstanceState::Removed) => (),
                    Some(TaskInstanceState::UpForRetry) | Some(TaskInstanceState::Restarting) => {
                        rollout.state = State::Problem
                    }
                    Some(TaskInstanceState::Failed) | Some(TaskInstanceState::UpstreamFailed) => {
                        rollout.state = State::Failed
                    }
                    Some(TaskInstanceState::UpForReschedule)
                    | Some(TaskInstanceState::Running)
                    | Some(TaskInstanceState::Deferred)
                    | Some(TaskInstanceState::Queued)
                    | Some(TaskInstanceState::Scheduled)
                    | Some(TaskInstanceState::Success)
                    | None => rollout.state = min(rollout.state, State::UpgradingUnassignedNodes),
                }
            } else {
                warn!(target: "rollout_data::get_rollout_data::subnet_state", "{}: unknown task {}", task_instance.dag_run_id, task_instance.task_id)
            }
        }

        if let Some(state) = Some(&dag_run.state) {
            match state {
                DagRunState::Success => rollout.state = State::Complete,
                DagRunState::Failed => rollout.state = State::Failed,
                _ => (),
            }
        }

        // We bump the cache entry's last update time, to only retrieve
        // tasks from this point in time on during subsequent retrievals.
        // We only do this at the end, in case any code above returns
        // early, to force a full state recalculation if there was a
        // failure or an early return.
        cache_entry.last_update_time = new_last_update_time;
        cache_entry.update_count = rollout.dag_info.update_count;

        Ok((rollout, cache_entry.clone()))
    }
}
