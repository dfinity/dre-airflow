use super::super::airflow_client::{
    AirflowClient, DagRunState, DagRunsResponseItem, TaskInstanceState, TaskInstancesResponseItem,
};
use super::super::types::v2::RolloutKind;
use super::super::types::v2::guestos::{
    Batch, Rollout, StandardEngine, StandardEngineStep, StandardEngineStepState, State, Subnet,
    SubnetState,
};
use super::plan::{PlanQueryResult, fetch_xcom};
use super::{RolloutDataGatherError, plan::PlanCache, python};
use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use lazy_static::lazy_static;
use log::{trace, warn};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::cmp::min;
use std::fmt::{self, Display};
use std::num::ParseIntError;
use std::str::FromStr;
use std::sync::Arc;
use std::{vec, vec::Vec};

const LOG_TARGET: &str = "live_state::guestos_rollout";

lazy_static! {
    // unwrap() is legitimate here because we know these cannot fail to compile.
    static ref SubnetGitRevisionRe: Regex = Regex::new("dfinity.ic_types.SubnetRolloutInstance.*@version=0[(]start_at=.*,subnet_id=([0-9-a-z-]+),git_revision=([0-9a-f]+)[)]").unwrap();
    static ref BatchIdentificationRe: Regex = Regex::new("batch_([0-9]+)[.](.+)").unwrap();
    // Matches standard engine step tasks like "standard_engine.step_0.wait_until_start_time".
    static ref StandardEngineStepRe: Regex = Regex::new("standard_engine[.]step_([0-9]+)[.](.+)").unwrap();
}

type PythonFormattedPlan = IndexMap<String, (String, Vec<String>)>;

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
                write!(f, "Invalid Python in rollout plan: {e}")
            }
            Self::BadBatchNumber(e) => {
                write!(f, "Could not parse batch number in rollout plan: {e}")
            }
            Self::BadDateTime(e) => {
                write!(f, "Could not parse date/time in rollout plan: {e}")
            }
            Self::InvalidSubnet(e) => {
                write!(f, "Could not regex find subnets in {e}")
            }
        }
    }
}

type BatchMap = IndexMap<usize, Batch>;

#[derive(Debug, Clone, Serialize)]
struct Plan {
    batches: BatchMap,
}

impl FromStr for Plan {
    type Err = PlanParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
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

/// A single standard engine step as produced by the `standard_engine_schedule`
/// task (see plugins/operators/ic_os_rollout.py).  `start_at` is an ISO 8601
/// string and `deployment_progress` is a fraction in [0.0, 1.0].
#[derive(Debug, Clone, Deserialize, Serialize)]
struct StandardEngineStepPlan {
    start_at: String,
    deployment_progress: f64,
}

/// The full standard engine schedule: an ordered list of steps.
type StandardEnginePlan = Vec<StandardEngineStepPlan>;

/// Newtype so we can parse the whole plan out of the XCom string via `FromStr`
/// (the XCom is a Python-serialized list of dicts).
#[derive(Debug, Clone, Serialize)]
struct StandardEnginePlanWrapper {
    steps: StandardEnginePlan,
}

impl FromStr for StandardEnginePlanWrapper {
    type Err = PlanParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let steps: StandardEnginePlan =
            python::from_str(value).map_err(PlanParseError::UndecipherablePython)?;
        Ok(StandardEnginePlanWrapper { steps })
    }
}

fn format_some<N>(opt: Option<N>, prefix: &str, fallback: &str) -> String
where
    N: Display,
{
    match opt {
        None => fallback.to_string(),
        Some(v) => format!("{prefix}{v}"),
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
        let tgt = &(LOG_TARGET.to_owned() + "::annotate_subnet_state");
        let new_state = state.clone();
        if (only_decrease && new_state < subnet.state)
            || (!only_decrease && new_state != subnet.state)
        {
            trace!(target: tgt, "{}: {} {:?} transition {} => {}   note: {}", task_instance.dag_run_id, task_instance.task_id, task_instance.map_index, subnet.state, new_state, subnet.comment);
            subnet.state = new_state.clone();
        } else {
            trace!(target: tgt, "{}: {} {:?} NO transition {} => {}", task_instance.dag_run_id, task_instance.task_id, task_instance.map_index, subnet.state, new_state);
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
                        .append_pair("map_index", format!("{idx}").as_str());
                };
                url.to_string()
            };
        };
    }
    state
}

/// Update the state (and comment / link) of a single standard engine step.
///
/// When `only_decrease` is true, the state is only lowered (used for retryable
/// error / running states so a more advanced observed state is not clobbered by
/// an earlier task reprocessed out of order).
fn annotate_standard_engine_step_state(
    step: &mut StandardEngineStep,
    state: StandardEngineStepState,
    task_instance: &TaskInstancesResponseItem,
    base_url: &reqwest::Url,
    only_decrease: bool,
) {
    let tgt = &(LOG_TARGET.to_owned() + "::annotate_standard_engine_step_state");
    if (only_decrease && state < step.state) || (!only_decrease && state != step.state) {
        trace!(target: tgt, "{}: {} transition {} => {}", task_instance.dag_run_id, task_instance.task_id, step.state, state);
        step.state = state.clone();
    }
    if state == step.state {
        step.comment = format!(
            "Task {} {}",
            task_instance.task_id,
            format_some(
                task_instance.state.clone(),
                "in state ",
                "has no known state"
            ),
        );
        step.display_url = {
            let mut url = base_url
                .join(format!("/dags/{}/grid", task_instance.dag_id).as_str())
                .unwrap();
            url.query_pairs_mut()
                .append_pair("dag_run_id", &task_instance.dag_run_id);
            url.query_pairs_mut()
                .append_pair("task_id", &task_instance.task_id);
            url.query_pairs_mut().append_pair("tab", "logs");
            url.to_string()
        };
    }
    if let Some(end_date) = task_instance.end_date {
        step.end_time = Some(match step.end_time {
            None => end_date,
            Some(existing) => max(existing, end_date),
        });
    }
}

#[derive(Clone, Default, Serialize)]
pub(crate) struct Parser {
    schedule: PlanCache<Plan>,
    standard_engine_schedule: PlanCache<StandardEnginePlanWrapper>,
}

impl Parser {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) async fn reparse(
        &mut self,
        dag_run: &DagRunsResponseItem,
        airflow_api: Arc<AirflowClient>,
        linearized_tasks: Vec<TaskInstancesResponseItem>,
    ) -> Result<RolloutKind, RolloutDataGatherError> {
        let mut rollout = Rollout {
            state: State::Preparing,
            batches: IndexMap::new(),
            standard_engine: None,
            conf: dag_run.conf.clone(),
        };

        macro_rules! update_state_unless_problem {
            ($input:expr) => {
                match &rollout.state {
                    State::Problem | State::Failed => {}
                    _ => rollout.state = max(rollout.state, $input),
                }
            };
        }

        // Now update rollout and batch state based on the obtained data.
        // What this process does is fairly straightforward:
        // * for each and every known up-to-date Airflow task in the cache
        //   (always processed in topological order),
        for task_instance in linearized_tasks.into_iter() {
            let tgt = &format!("{LOG_TARGET}::subnet_state");

            // * deduce the rollout plan, if available,
            // * mark the rollout as having problems or errors depending on what
            //   the task state is, or as one of the various running states, if
            //   any  non-subnet-related task is running / pending.
            // * handle tasks corresponding to a batch/subnet in a special way
            //   (commented below in its pertinent section).
            trace!(
                target: LOG_TARGET, "Processing task {}.{:?} in state {:?}",
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
                    | Some(TaskInstanceState::Scheduled) => {
                        update_state_unless_problem!(State::Preparing)
                    }
                    Some(TaskInstanceState::Success) => {
                        rollout.batches = match self
                            .schedule
                            .get_from_str(
                                &task_instance,
                                fetch_xcom(
                                    airflow_api.clone(),
                                    dag_run.dag_id.as_str(),
                                    dag_run.dag_run_id.as_str(),
                                    task_instance.task_id.as_str(),
                                    task_instance.map_index,
                                    "return_value",
                                ),
                            )
                            .await
                        {
                            PlanQueryResult::Found(plan) => plan.batches,
                            PlanQueryResult::Invalid => continue,
                            PlanQueryResult::NotFound => continue,
                            PlanQueryResult::Error(e) => {
                                return Err(RolloutDataGatherError::AirflowError(e));
                            }
                        };
                        update_state_unless_problem!(State::Waiting)
                    }
                    None => {}
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
                    | Some(TaskInstanceState::Success)
                    | None => {}
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

                trace!(target: tgt, "{}: processing {} {:?} in state {:?}", task_instance.dag_run_id, task_instance.task_id, task_instance.map_index, task_instance.state);
                let (batch, task_name) = (
                    // We get away with unwrap() here because we know we captured an integer.
                    match rollout
                        .batches
                        .get_mut(&usize::from_str(&captured[1]).unwrap())
                    {
                        Some(batch) => batch,
                        None => {
                            trace!(target: tgt, "{}: no corresponding batch, continuing", task_instance.dag_run_id);
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
                            trace!(target: tgt, "{}: ignoring task instance {} {:?} with no state", task_instance.dag_run_id, task_instance.task_id, task_instance.map_index);
                        }
                    }
                    Some(state) => match state {
                        // https://stackoverflow.com/questions/53654302/tasks-are-moved-to-removed-state-in-airflow-when-they-are-queued-and-not-restore
                        // If a task is removed, we cannot decide rollout state based on it.
                        // https://stackoverflow.com/questions/77426996/skipping-a-task-in-airflow
                        // If a task is skipped, the next task (in state Running / Deferred)
                        // will pick up the slack for changing subnet state.
                        TaskInstanceState::Removed | TaskInstanceState::Skipped => {
                            trace!(target: tgt, "{}: ignoring task instance {} {:?} in state {:?}", task_instance.dag_run_id, task_instance.task_id, task_instance.map_index, task_instance.state);
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
                                    trans_min!(SubnetState::WaitingForAlertsGone);
                                }
                                &_ => {
                                    warn!(target: tgt, "{}: no info on to handle task instance {} {:?} in state {:?}", task_instance.dag_run_id, task_instance.task_id, task_instance.map_index, task_instance.state);
                                }
                            }
                            update_state_unless_problem!(State::UpgradingSubnets)
                        }
                        TaskInstanceState::Success => {
                            match task_name {
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
                                    // We don't have a state for when this task is completed,
                                    // but the join task is not yet.
                                    trans_exact!(SubnetState::WaitingForAlertsGone);
                                }
                                "join" => {
                                    trans_exact!(SubnetState::Complete);
                                    batch.end_time = task_instance.end_date;
                                }
                                &_ => {
                                    warn!(target: tgt, "{}: no info on how to handle task instance {} {:?} in state {:?}", task_instance.dag_run_id, task_instance.task_id, task_instance.map_index, task_instance.state);
                                }
                            };
                            update_state_unless_problem!(State::UpgradingSubnets)
                        }
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
                    | Some(TaskInstanceState::Success) => {
                        update_state_unless_problem!(State::UpgradingUnassignedNodes)
                    }
                    None => {}
                }
            } else if task_instance.task_id == "upgrade_cloud_engines" {
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
                    | Some(TaskInstanceState::Success) => {
                        update_state_unless_problem!(State::UpgradingCloudEngines)
                    }
                    None => {}
                }
            } else if task_instance.task_id == "standard_engine_schedule" {
                // This task produces the ordered list of deployment_progress
                // increments for the standard engine (Cloud Engines).  Once it
                // succeeds, its XCom holds the plan, which we materialize into
                // rollout.standard_engine.
                match task_instance.state {
                    Some(TaskInstanceState::Skipped) | Some(TaskInstanceState::Removed) => (),
                    Some(TaskInstanceState::UpForRetry) | Some(TaskInstanceState::Restarting) => {
                        rollout.state = State::Problem;
                    }
                    Some(TaskInstanceState::Failed) | Some(TaskInstanceState::UpstreamFailed) => {
                        rollout.state = State::Failed;
                    }
                    Some(TaskInstanceState::Success) => {
                        let plan = match self
                            .standard_engine_schedule
                            .get_from_str(
                                &task_instance,
                                fetch_xcom(
                                    airflow_api.clone(),
                                    dag_run.dag_id.as_str(),
                                    dag_run.dag_run_id.as_str(),
                                    task_instance.task_id.as_str(),
                                    task_instance.map_index,
                                    "return_value",
                                ),
                            )
                            .await
                        {
                            PlanQueryResult::Found(plan) => plan.steps,
                            PlanQueryResult::Invalid => continue,
                            PlanQueryResult::NotFound => continue,
                            PlanQueryResult::Error(e) => {
                                return Err(RolloutDataGatherError::AirflowError(e));
                            }
                        };
                        // Only bother reflecting the standard engine schedule if
                        // it is non-empty (an empty plan means the rollout does
                        // not manage the standard engine version this time).
                        if !plan.is_empty() {
                            let new_replica_version_id = rollout
                                .conf
                                .get("git_revision")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string();
                            let steps = plan
                                .into_iter()
                                .map(|step| StandardEngineStep {
                                    planned_start_time: DateTime::parse_from_rfc3339(
                                        &step.start_at,
                                    )
                                    .map(|d| d.with_timezone(&Utc))
                                    .unwrap_or_default(),
                                    actual_start_time: None,
                                    end_time: None,
                                    deployment_progress: step.deployment_progress,
                                    state: StandardEngineStepState::Pending,
                                    comment: "".to_string(),
                                    display_url: "".to_string(),
                                })
                                .collect();
                            rollout.standard_engine = Some(StandardEngine {
                                new_replica_version_id,
                                steps,
                            });
                        }
                    }
                    Some(TaskInstanceState::UpForReschedule)
                    | Some(TaskInstanceState::Running)
                    | Some(TaskInstanceState::Deferred)
                    | Some(TaskInstanceState::Queued)
                    | Some(TaskInstanceState::Scheduled)
                    | None => {}
                }
            } else if let Some(captured) =
                StandardEngineStepRe.captures(task_instance.task_id.as_str())
            {
                // A task belonging to a single standard engine step.  We update
                // the corresponding step's state (if the schedule has been
                // materialized already).
                let Some(standard_engine) = rollout.standard_engine.as_mut() else {
                    trace!(target: tgt, "{}: standard engine schedule not yet available, skipping {}", task_instance.dag_run_id, task_instance.task_id);
                    continue;
                };
                // unwrap() is safe: the regex captured an integer.
                let step_index = usize::from_str(&captured[1]).unwrap();
                let task_name = &captured[2];
                let Some(step) = standard_engine.steps.get_mut(step_index) else {
                    trace!(target: tgt, "{}: no corresponding standard engine step {}, continuing", task_instance.dag_run_id, step_index);
                    continue;
                };

                macro_rules! se_trans_min {
                    ($input:expr) => {
                        annotate_standard_engine_step_state(
                            step,
                            $input,
                            &task_instance,
                            &airflow_api.url,
                            true,
                        )
                    };
                }
                macro_rules! se_trans_exact {
                    ($input:expr) => {
                        annotate_standard_engine_step_state(
                            step,
                            $input,
                            &task_instance,
                            &airflow_api.url,
                            false,
                        )
                    };
                }

                match &task_instance.state {
                    None => {}
                    Some(state) => match state {
                        TaskInstanceState::Removed | TaskInstanceState::Skipped => {}
                        TaskInstanceState::UpForRetry | TaskInstanceState::Restarting => {
                            se_trans_min!(StandardEngineStepState::Error);
                            rollout.state = min(rollout.state, State::Problem);
                        }
                        TaskInstanceState::Failed => {
                            se_trans_min!(StandardEngineStepState::Error);
                            rollout.state = min(rollout.state, State::Failed);
                        }
                        TaskInstanceState::UpstreamFailed => {
                            se_trans_min!(StandardEngineStepState::PredecessorFailed);
                            rollout.state = min(rollout.state, State::Failed);
                        }
                        TaskInstanceState::UpForReschedule
                        | TaskInstanceState::Running
                        | TaskInstanceState::Deferred
                        | TaskInstanceState::Queued
                        | TaskInstanceState::Scheduled => {
                            match task_name.as_ref() {
                                "collect_step" => se_trans_min!(StandardEngineStepState::Pending),
                                "wait_until_start_time" => {
                                    se_trans_min!(StandardEngineStepState::Waiting)
                                }
                                "create_proposal_if_none_exists" => {
                                    se_trans_min!(StandardEngineStepState::Proposing)
                                }
                                "collect_upgraded_engines" | "wait_until_no_alerts" => {
                                    se_trans_min!(StandardEngineStepState::WaitingForAlertsGone)
                                }
                                "request_proposal_vote" | "join" => {}
                                _ => {}
                            }
                            update_state_unless_problem!(State::UpgradingCloudEngines);
                        }
                        TaskInstanceState::Success => {
                            match task_name.as_ref() {
                                "collect_step" => se_trans_min!(StandardEngineStepState::Waiting),
                                "wait_until_start_time" => {
                                    if step.actual_start_time.is_none() {
                                        step.actual_start_time = task_instance.end_date;
                                    }
                                    se_trans_exact!(StandardEngineStepState::Proposing);
                                }
                                "create_proposal_if_none_exists" => {
                                    se_trans_exact!(StandardEngineStepState::WaitingForAlertsGone)
                                }
                                "collect_upgraded_engines" => {
                                    se_trans_exact!(StandardEngineStepState::WaitingForAlertsGone)
                                }
                                "wait_until_no_alerts" => {
                                    se_trans_exact!(StandardEngineStepState::Complete)
                                }
                                "join" => se_trans_exact!(StandardEngineStepState::Complete),
                                "request_proposal_vote" => {}
                                _ => {}
                            }
                            update_state_unless_problem!(State::UpgradingCloudEngines);
                        }
                    },
                }
            } else {
                warn!(target: tgt, "{}: unknown task {}", task_instance.dag_run_id, task_instance.task_id)
            }
        }

        if let Some(state) = Some(&dag_run.state) {
            match state {
                DagRunState::Success => rollout.state = State::Complete,
                DagRunState::Failed => rollout.state = State::Failed,
                _ => (),
            }
        }

        Ok(RolloutKind::RolloutIcOsToMainnetSubnets(rollout))
    }
}
