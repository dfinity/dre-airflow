use super::{
    RolloutDataGatherError, plan::PlanCache, plan::PlanQueryResult, plan::fetch_xcom, python,
};
use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use lazy_static::lazy_static;
use log::{trace, warn};
use regex::Regex;
use rollout_dashboard::airflow_client::{
    AirflowClient, DagRunState, DagRunsResponseItem, TaskInstanceState, TaskInstancesResponseItem,
};
use rollout_dashboard::types::v2::{
    ApiBoundaryNode, ApiBoundaryNodesBatch, ApiBoundaryNodesBatchState as BatchState,
    RolloutIcOsToMainnetApiBoundaryNodes, RolloutIcOsToMainnetApiBoundaryNodesState as State,
    RolloutKind,
};
use std::cmp::min;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;
use std::vec::Vec;

const LOG_TARGET: &str = "live_state::api_boundary_nodes_rollout";

lazy_static! {
    // unwrap() is legitimate here because we know these cannot fail to compile.
    static ref BatchIdentificationRe: Regex = Regex::new("batch_([0-9]+)[.](.+)").unwrap();
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

fn annotate_batch_state(
    batch: &mut ApiBoundaryNodesBatch,
    state: BatchState,
    task_instance: &TaskInstancesResponseItem,
    base_url: &reqwest::Url,
    only_decrease: bool,
) -> BatchState {
    let tgt = &(LOG_TARGET.to_owned() + "::annotate_batch_state");
    let new_state = state.clone();
    if (only_decrease && new_state < batch.state) || (!only_decrease && new_state != batch.state) {
        trace!(target: tgt, "{}: {} {:?} transition {} => {}   note: {}", task_instance.dag_run_id, task_instance.task_id, task_instance.map_index, batch.state, new_state, batch.comment);
        batch.state = new_state.clone();
    } else {
        trace!(target: tgt, "{}: {} {:?} NO transition {} => {}", task_instance.dag_run_id, task_instance.task_id, task_instance.map_index, batch.state, new_state);
    }
    if new_state == batch.state {
        batch.comment = format!(
            "Task {} {}",
            task_instance.task_id,
            format_some(
                task_instance.state.clone(),
                "in state ",
                "has no known state"
            ),
        );
        batch.display_url = {
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
    state
}

#[derive(Debug, Clone)]
struct Plan {
    batches: BatchMap,
}

type PythonFormattedPlan = Vec<(String, Vec<String>)>;

impl FromStr for Plan {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut res = Plan {
            batches: IndexMap::new(),
        };
        let python_string_plan: PythonFormattedPlan = match python::from_str(value) {
            Ok(s) => s,
            Err(e) => return Err(format!("Could not decipher the Python string: {}", e)),
        };
        for (batch_number, (start_time_str, api_boundary_nodes)) in
            python_string_plan.into_iter().enumerate()
        {
            let start_time: DateTime<Utc> = match DateTime::parse_from_str(
                start_time_str.as_str(),
                "datetime.datetime@version=2(timestamp=%s%.f,tz=None)",
            ) {
                Ok(s) => s.with_timezone(&Utc),
                Err(e) => {
                    return Err(format!(
                        "Could not parse date/time {}: {}",
                        start_time_str, e
                    ));
                }
            };
            let batch = ApiBoundaryNodesBatch {
                planned_start_time: start_time,
                actual_start_time: None,
                end_time: None,
                api_boundary_nodes: api_boundary_nodes
                    .into_iter()
                    .map(|i| ApiBoundaryNode { node_id: i })
                    .collect(),
                state: BatchState::Unknown,
                comment: "".into(),
                display_url: "".into(),
            };
            res.batches.insert(batch_number + 1, batch);
        }
        Ok(res)
    }
}

type BatchMap = IndexMap<usize, ApiBoundaryNodesBatch>;

#[derive(Clone, Default)]
pub(super) struct Parser {
    schedule: PlanCache<Plan>,
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
        let mut rollout = RolloutIcOsToMainnetApiBoundaryNodes {
            state: State::Complete,
            batches: IndexMap::new(),
            conf: dag_run.conf.clone(),
        };

        // Now update rollout and batch state based on the obtained data.
        // What this process does is fairly straightforward:
        // * for each and every known up-to-date Airflow task in the cache
        //   (always processed in topological order),
        for task_instance in linearized_tasks.into_iter() {
            let tgt = &format!("{}::batch_state", LOG_TARGET);

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
                    | Some(TaskInstanceState::Scheduled)
                    | None => rollout.state = min(rollout.state, State::Preparing),
                    Some(TaskInstanceState::Success) => {
                        rollout.batches = match self
                            .schedule
                            .get(
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
                    }
                }
            } else if task_instance.task_id == "wait_for_other_rollouts"
                || task_instance.task_id == "wait_for_revision_to_be_elected"
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
                        annotate_batch_state(batch, $input, &task_instance, &airflow_api.url, true)
                    };
                }
                macro_rules! trans_exact {
                    ($input:expr) => {
                        annotate_batch_state(batch, $input, &task_instance, &airflow_api.url, false)
                    };
                }

                match &task_instance.state {
                    None => {
                        if task_name == "prepare" {
                            trans_exact!(BatchState::Pending);
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
                            trans_min!(BatchState::Error);
                            rollout.state = min(rollout.state, State::Problem)
                        }
                        TaskInstanceState::Failed => {
                            trans_min!(BatchState::Error);
                            rollout.state = min(rollout.state, State::Failed)
                        }
                        TaskInstanceState::UpstreamFailed => {
                            trans_min!(BatchState::PredecessorFailed);
                            rollout.state = min(rollout.state, State::Failed)
                        }
                        TaskInstanceState::UpForReschedule
                        | TaskInstanceState::Running
                        | TaskInstanceState::Deferred
                        | TaskInstanceState::Queued
                        | TaskInstanceState::Scheduled => {
                            match task_name {
                                "prepare" => {
                                    trans_min!(BatchState::Pending);
                                }
                                "wait_until_start_time" => {
                                    trans_min!(BatchState::Waiting);
                                }
                                "create_proposal_if_none_exists" => {
                                    trans_min!(BatchState::Proposing);
                                }
                                "request_proposal_vote" => {
                                    // We ignore this one for the purposes of rollout state setup.
                                }
                                "wait_until_proposal_is_accepted" => {
                                    trans_min!(BatchState::WaitingForElection);
                                }
                                "wait_for_revision_adoption" => {
                                    trans_min!(BatchState::WaitingForAdoption);
                                }
                                "wait_until_nodes_healthy" => {
                                    trans_min!(BatchState::WaitingUntilNodesHealthy);
                                }
                                "join" => {
                                    trans_min!(BatchState::Complete);
                                }
                                &_ => {
                                    warn!(target: tgt, "{}: no info on to handle task instance {} {:?} in state {:?}", task_instance.dag_run_id, task_instance.task_id, task_instance.map_index, task_instance.state);
                                }
                            }
                            rollout.state = min(rollout.state, State::UpgradingApiBoundaryNodes)
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
                            "prepare" => {
                                trans_min!(BatchState::Waiting);
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
                                trans_exact!(BatchState::Waiting);
                            }
                            "create_proposal_if_none_exists" => {
                                trans_exact!(BatchState::WaitingForElection);
                            }
                            "request_proposal_vote" => {
                                // We ignore this one for the purposes of rollout state setup.
                            }
                            "wait_until_proposal_is_accepted" => {
                                trans_exact!(BatchState::WaitingForAdoption);
                            }
                            "wait_for_revision_adoption" => {
                                trans_exact!(BatchState::WaitingUntilNodesHealthy);
                            }
                            "wait_until_nodes_healthy" => {
                                trans_exact!(BatchState::Complete);
                            }
                            "join" => {
                                trans_exact!(BatchState::Complete);
                                batch.end_time = task_instance.end_date;
                            }
                            &_ => {
                                warn!(target: tgt, "{}: no info on how to handle task instance {} {:?} in state {:?}", task_instance.dag_run_id, task_instance.task_id, task_instance.map_index, task_instance.state);
                            }
                        },
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

        Ok(RolloutKind::RolloutIcOsToMainnetApiBoundaryNodes(rollout))
    }
}
