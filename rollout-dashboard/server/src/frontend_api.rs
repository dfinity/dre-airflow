use crate::python;
use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use regex::Regex;
use serde::Serialize;
use std::collections::HashMap;
use std::fmt::{self, Display};
use std::num::ParseIntError;
use std::rc::Rc;
use std::str::FromStr;
use std::sync::Arc;
use std::{vec, vec::Vec};
use topological_sort::TopologicalSort;

use crate::airflow_client::{
    AirflowClient, AirflowError, DagRunState, TaskInstanceState, TaskInstancesResponse,
    TaskInstancesResponseItem, TasksResponse, TasksResponseItem,
};

lazy_static! {
    // unwrap() is legitimate here because we know these cannot fail to compile.
    static ref SubnetGitRevisionRe: Regex = Regex::new("dfinity.ic_types.SubnetRolloutInstance.*@version=0[(]start_at=.*,subnet_id=([0-9-a-z-]+),git_revision=([0-9a-f]+)[)]").unwrap();
    static ref BatchIdentificationRe: Regex = Regex::new("batch_([0-9]+)[.](.+)").unwrap();
}

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum SubnetRolloutState {
    Pending,
    Waiting,
    Proposing,
    WaitingForElection,
    WaitingForAdoption,
    WaitingForAlertsGone,
    Complete,
    Skipped,
    Error,
}

#[derive(Serialize, Debug, Clone)]
pub struct Subnet {
    pub subnet_id: String,
    pub git_revision: String,
    pub state: SubnetRolloutState,
}

#[derive(Serialize, Debug, Clone)]
pub struct Batch {
    pub planned_start_time: DateTime<Utc>,
    pub actual_start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub subnets: Vec<Subnet>,
}

impl Batch {
    fn set_subnet_state(&mut self, state: SubnetRolloutState, index: Option<usize>) {
        match index {
            None => {
                for subnet in self.subnets.iter_mut() {
                    subnet.state = state.clone()
                }
            }
            Some(index) => self.subnets[index].state = state,
        }
    }
}

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum RolloutState {
    Preparing,
    Waiting,
    UpgradingSubnets,
    UpgradingUnassignedNodes,
    Complete,
    Problem,
    Failed,
}

#[derive(Debug, Serialize, Clone)]
pub struct Rollout {
    pub name: String,
    pub note: Option<String>,
    pub state: RolloutState,
    pub dispatch_time: DateTime<Utc>,
    pub last_scheduling_decision: Option<DateTime<Utc>>,
    pub batches: HashMap<usize, Batch>,
    pub conf: HashMap<String, serde_json::Value>,
}

impl Rollout {
    fn new(
        name: String,
        note: Option<String>,
        dispatch_time: DateTime<Utc>,
        last_scheduling_decision: Option<DateTime<Utc>>,
        conf: HashMap<String, serde_json::Value>,
    ) -> Self {
        Self {
            name,
            note,
            state: RolloutState::Preparing,
            dispatch_time,
            last_scheduling_decision,
            batches: HashMap::new(),
            conf,
        }
    }
}

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

    fn sort_instances(&self, r: TaskInstancesResponse) -> Vec<TaskInstancesResponseItem> {
        let mut all_task_instances: HashMap<String, Vec<Rc<TaskInstancesResponseItem>>> =
            HashMap::new();

        for task_instance in r.task_instances.into_iter() {
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
    batches: HashMap<usize, Batch>,
}

type PythonFormattedRolloutPlan = HashMap<String, (String, Vec<String>)>;

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
            batches: HashMap::new(),
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
                        state: SubnetRolloutState::Pending,
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

#[derive(Clone)]
pub struct RolloutApi {
    airflow_api: Arc<AirflowClient>,
}

impl RolloutApi {
    pub fn new(client: AirflowClient) -> Self {
        Self {
            airflow_api: Arc::new(client),
        }
    }
}

impl RolloutApi {
    pub async fn get_rollout_data(&self) -> Result<Vec<Rollout>, RolloutDataGatherError> {
        let dag_id = "rollout_ic_os_to_mainnet_subnets";
        let dag_runs = self.airflow_api.dag_runs(dag_id, 20, 0).await?;
        let tasks = self.airflow_api.tasks(dag_id).await?;
        let sorter = TaskInstanceTopologicalSorter::new(tasks)?;

        let mut res: Vec<Rollout> = vec![];

        for dag_run in dag_runs.dag_runs.iter() {
            let task_instances = self
                .airflow_api
                .task_instances(dag_id, dag_run.dag_run_id.as_str(), 500, 0)
                .await?;
            let sorted_task_instances = sorter.sort_instances(task_instances);

            let mut rollout = Rollout::new(
                dag_run.dag_run_id.to_string(),
                dag_run.note.clone(),
                dag_run.logical_date,
                dag_run.last_scheduling_decision,
                dag_run.conf.clone(),
            );

            for task_instance in sorted_task_instances {
                if task_instance.task_id == "schedule" {
                    match task_instance.state {
                        Some(TaskInstanceState::Skipped)
                        | Some(TaskInstanceState::Removed)
                        | None => (),
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
                        | Some(TaskInstanceState::Scheduled) => {
                            rollout.state = RolloutState::Preparing
                        }
                        Some(TaskInstanceState::Success) => {
                            let schedule_xcom = match self
                                .airflow_api
                                .xcom_entry(
                                    dag_id,
                                    dag_run.dag_run_id.as_str(),
                                    task_instance.task_id.as_str(),
                                    task_instance.map_index,
                                    "return_value",
                                )
                                .await
                            {
                                Ok(schedule) => schedule,
                                Err(AirflowError::StatusCode(reqwest::StatusCode::NOT_FOUND)) => {
                                    rollout.state = RolloutState::Preparing;
                                    continue;
                                }
                                Err(e) => return Err(RolloutDataGatherError::AirflowError(e)),
                            };
                            let schedule =
                                RolloutPlan::from_python_string(schedule_xcom.value.clone())?;
                            rollout.batches = schedule.batches;
                            rollout.state = RolloutState::Waiting;
                        }
                    }
                } else if task_instance.task_id == "wait_for_other_rollouts"
                    || task_instance.task_id == "wait_for_revision_to_be_elected"
                {
                    match task_instance.state {
                        Some(TaskInstanceState::Skipped)
                        | Some(TaskInstanceState::Removed)
                        | None => (),
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
                        | Some(TaskInstanceState::Success) => rollout.state = RolloutState::Waiting,
                    }
                } else if let Some(captured) =
                    BatchIdentificationRe.captures(task_instance.task_id.as_str())
                {
                    let (batch, task_name) = (
                        // We get away with unwrap() here because we know we captured an integer.
                        match rollout
                            .batches
                            .get_mut(&usize::from_str(&captured[1]).unwrap())
                        {
                            Some(batch) => batch,
                            None => continue,
                        },
                        &captured[2],
                    );

                    match task_instance.state {
                        Some(TaskInstanceState::Skipped) | Some(TaskInstanceState::Removed) => {
                            batch.set_subnet_state(
                                SubnetRolloutState::Skipped,
                                task_instance.map_index,
                            );
                        }
                        Some(TaskInstanceState::UpForRetry)
                        | Some(TaskInstanceState::Restarting) => {
                            batch.set_subnet_state(
                                SubnetRolloutState::Error,
                                task_instance.map_index,
                            );
                            rollout.state = RolloutState::Problem;
                        }
                        Some(TaskInstanceState::Failed)
                        | Some(TaskInstanceState::UpstreamFailed) => {
                            batch.set_subnet_state(
                                SubnetRolloutState::Error,
                                task_instance.map_index,
                            );
                            rollout.state = RolloutState::Failed;
                        }
                        Some(TaskInstanceState::Success)
                        | Some(TaskInstanceState::UpForReschedule)
                        | Some(TaskInstanceState::Running)
                        | Some(TaskInstanceState::Deferred)
                        | Some(TaskInstanceState::Queued)
                        | Some(TaskInstanceState::Scheduled) => {
                            batch.set_subnet_state(
                                match task_name {
                                    "collect_batch_subnets" => SubnetRolloutState::Pending,
                                    "wait_until_start_time" => SubnetRolloutState::Waiting,
                                    "create_proposal_if_none_exists" | "request_proposal_vote" => {
                                        SubnetRolloutState::Proposing
                                    }
                                    "wait_until_proposal_is_accepted" => {
                                        SubnetRolloutState::WaitingForElection
                                    }
                                    "wait_for_replica_revision" => {
                                        SubnetRolloutState::WaitingForAdoption
                                    }
                                    "wait_until_no_alerts" => {
                                        SubnetRolloutState::WaitingForAlertsGone
                                    }
                                    "join" => match task_instance.state {
                                        Some(TaskInstanceState::Success) => {
                                            SubnetRolloutState::Complete
                                        }
                                        _ => SubnetRolloutState::WaitingForAlertsGone,
                                    },
                                    // Maybe here we just want to log error and continue for robustness?
                                    &_ => panic!("impossible task name {}", task_instance.task_id),
                                },
                                task_instance.map_index,
                            );
                            if let Some(TaskInstanceState::Success) = &task_instance.state {
                                match task_name {
                                    "wait_until_start_time" => match batch.actual_start_time {
                                        None => batch.actual_start_time = task_instance.end_date,
                                        Some(start_time) => match task_instance.end_date {
                                            None => (),
                                            Some(end_date) => {
                                                if start_time > end_date {
                                                    batch.actual_start_time =
                                                        task_instance.end_date;
                                                }
                                            }
                                        },
                                    },
                                    "join" => {
                                        batch.end_time = task_instance.end_date;
                                    }
                                    _ => (),
                                }
                            };
                            rollout.state = RolloutState::UpgradingSubnets;
                        }
                        None => (),
                    }
                } else if task_instance.task_id == "upgrade_unassigned_nodes" {
                    match task_instance.state {
                        Some(TaskInstanceState::Skipped)
                        | Some(TaskInstanceState::Removed)
                        | None => (),
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
                        | Some(TaskInstanceState::Success) => {
                            rollout.state = RolloutState::UpgradingUnassignedNodes
                        }
                    }
                }
            }

            if let Some(state) = Some(&dag_run.state) {
                match state {
                    DagRunState::Success => rollout.state = RolloutState::Complete,
                    DagRunState::Failed => rollout.state = RolloutState::Failed,
                    _ => (),
                }
            }

            res.push(rollout);
        }

        Ok(res)
    }
}
