use crate::live_state::python::PythonDateTime;

use super::plan::{PlanQueryResult, fetch_xcom};
use super::{RolloutDataGatherError, plan::PlanCache, python};
use indexmap::IndexMap;
use lazy_static::lazy_static;
use log::{trace, warn};
use regex::Regex;
use rollout_dashboard::airflow_client::{
    AirflowClient, DagRunState, DagRunsResponseItem, TaskInstanceState, TaskInstancesResponseItem,
};
use rollout_dashboard::types::v2::RolloutKind;
use rollout_dashboard::types::v2::hostos::{
    ActuallyTargetedNodes, BatchResponse, BatchState, NodeAlertStatuses, NodeFailureTolerance,
    NodeInfo, NodeSelectors, NodeUpgradeStatuses, Rollout, StageName, Stages as V2Stages, State,
};
use serde::{Deserialize, Serialize, Serializer};
use std::cmp::max;
use std::cmp::min;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{self, Display};
use std::num::NonZero;
use std::str::FromStr;
use std::sync::Arc;
use std::{vec, vec::Vec};
use tokio::sync::Mutex;

const LOG_TARGET: &str = "live_state::hostos_rollout";

lazy_static! {
    // unwrap() is legitimate here because we know these cannot fail to compile.
    static ref BatchIdentificationRe: Regex = Regex::new("(canary|main|unassigned|stragglers)_([0-9]+)[.](.+)").unwrap();
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ProvisionalHostOSPlanBatch {
    pub(super) nodes: Vec<NodeInfo>,
    pub(super) selectors: NodeSelectors,
    pub(super) start_at: PythonDateTime,
    pub(super) tolerance: Option<NodeFailureTolerance>,
}

fn make_a_planned_host_os_batch(
    stage: StageName,
    batch_number: NonZero<usize>,
    val: &ProvisionalHostOSPlanBatch,
    selectors: &NodeSelectors,
    tolerance: &Option<NodeFailureTolerance>,
) -> BatchResponse {
    BatchResponse {
        stage,
        batch_number,
        planned_start_time: val.start_at.clone().into(),
        actual_start_time: None,
        end_time: None,
        state: BatchState::Unknown,
        comment: "".to_string(),
        display_url: "".into(),
        selectors: Some(selectors.clone()),
        tolerance: tolerance.clone(),
        planned_nodes: val.nodes.clone(),
        actual_nodes: None,
        upgraded_nodes: None,
        alerting_nodes: None,
    }
}

/// This function is only called when a task instance that does not correspond
/// to any known batch appears.  This can happen when nodes are added mid-rollout,
/// and therefore the planning logic did not take them into account, but a late
/// batch in the rollout finds them and goes to town on them.
fn make_a_discovered_host_os_batch(
    stage: StageName,
    batch_number: NonZero<usize>,
    val: &TaskInstancesResponseItem,
) -> BatchResponse {
    BatchResponse {
        stage,
        batch_number,
        planned_start_time: val.earliest_date(),
        actual_start_time: None,
        end_time: None,
        state: BatchState::Unknown,
        comment: "".to_string(),
        display_url: "".into(),
        planned_nodes: vec![],
        actual_nodes: None,
        selectors: None,
        tolerance: None,
        upgraded_nodes: None,
        alerting_nodes: None,
    }
}

#[derive(Debug)]
pub enum PlanParseError {
    UndecipherablePython(python::ErrorImpl),
}

impl Display for PlanParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::UndecipherablePython(e) => {
                write!(f, "Invalid Python in plan: {} {:?}", e, e.source())
            }
        }
    }
}

#[derive(Debug, Deserialize, Clone, Serialize)]
struct ProvisionalHostOSPlan {
    canary: Option<Vec<ProvisionalHostOSPlanBatch>>,
    main: Option<Vec<ProvisionalHostOSPlanBatch>>,
    unassigned: Option<Vec<ProvisionalHostOSPlanBatch>>,
    stragglers: Option<Vec<ProvisionalHostOSPlanBatch>>,
}

impl FromStr for ProvisionalHostOSPlan {
    type Err = PlanParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let python_string_plan: ProvisionalHostOSPlan = match python::from_str(value) {
            Ok(s) => s,
            Err(e) => return Err(PlanParseError::UndecipherablePython(e)),
        };
        Ok(python_string_plan)
    }
}

#[derive(Clone, Default, Debug, Serialize)]
pub(crate) struct Stages {
    pub(crate) canary: IndexMap<NonZero<usize>, BatchResponse>,
    pub(crate) main: IndexMap<NonZero<usize>, BatchResponse>,
    pub(crate) unassigned: IndexMap<NonZero<usize>, BatchResponse>,
    pub(crate) stragglers: IndexMap<NonZero<usize>, BatchResponse>,
}

impl From<&Stages> for V2Stages {
    fn from(v: &Stages) -> V2Stages {
        V2Stages {
            canary: v.canary.iter().map(|(k, v)| (*k, v.into())).collect(),
            main: v.main.iter().map(|(k, v)| (*k, v.into())).collect(),
            unassigned: v.unassigned.iter().map(|(k, v)| (*k, v.into())).collect(),
            stragglers: v.stragglers.iter().map(|(k, v)| (*k, v.into())).collect(),
        }
    }
}

impl From<&ProvisionalHostOSPlan> for Stages {
    fn from(val: &ProvisionalHostOSPlan) -> Self {
        macro_rules! transform_to_stages {
            ($stage:expr, $stagename:expr, $b:expr) => {
                $stage
                    .clone()
                    .map(|z| {
                        z.iter()
                            .enumerate()
                            .map(|(n, b)| {
                                (
                                    NonZero::new(n + 1).unwrap(),
                                    make_a_planned_host_os_batch(
                                        $stagename,
                                        NonZero::new(n + 1).unwrap(),
                                        b,
                                        &b.selectors,
                                        &b.tolerance,
                                    ),
                                )
                            })
                            .collect()
                    })
                    .unwrap_or_default()
            };
        }
        Stages {
            canary: transform_to_stages!(val.canary, StageName::Canary, b),
            main: transform_to_stages!(val.main, StageName::Main, b),
            unassigned: transform_to_stages!(val.unassigned, StageName::Unassigned, b),
            stragglers: transform_to_stages!(val.stragglers, StageName::Stragglers, b),
        }
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

fn annotate_batch_state(
    batch: &mut BatchResponse,
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
                    .append_pair("map_index", format!("{idx}").as_str());
            };
            url.to_string()
        };
    };
    state
}

#[derive(Clone, Default, Serialize)]
struct BatchXcomData {
    actual_plans: PlanCache<ActuallyTargetedNodes>,
    nodes_upgrade_status: PlanCache<NodeUpgradeStatuses>,
    nodes_alert_status: PlanCache<NodeAlertStatuses>,
    selectors: PlanCache<NodeSelectors>,
    tolerance: PlanCache<NodeFailureTolerance>,
}

#[derive(Clone, Default)]
pub(crate) struct Parser {
    provisional_plan: PlanCache<ProvisionalHostOSPlan>,
    batch_xcoms: HashMap<(StageName, NonZero<usize>), BatchXcomData>,
    pub(crate) stages: Option<Stages>,
}

// The following is necessary because the internal index map is not
// serializable to JSON.
impl Serialize for Parser {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct SerializedParser {
            provisional_plan: PlanCache<ProvisionalHostOSPlan>,
            batch_xcoms: HashMap<String, BatchXcomData>,
            stages: Option<Stages>,
        }

        let p = SerializedParser {
            provisional_plan: self.provisional_plan.clone(),
            batch_xcoms: self
                .batch_xcoms
                .iter()
                .map(|(k, v)| (format!("{}/{}", k.0, k.1), v.clone()))
                .collect(),
            stages: self.stages.clone(),
        };

        p.serialize(serializer)
    }
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
            stages: None,
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

        macro_rules! retrieve_xcom_from_cache_or_server {
            ($member:expr, $task_instance:expr, $xcom_key:expr) => {
                match $member
                    .get(
                        &$task_instance,
                        fetch_xcom(
                            airflow_api.clone(),
                            dag_run.dag_id.as_str(),
                            dag_run.dag_run_id.as_str(),
                            $task_instance.task_id.as_str(),
                            $task_instance.map_index,
                            $xcom_key,
                        ),
                    )
                    .await
                {
                    PlanQueryResult::Found(plan) => Some(plan),
                    PlanQueryResult::Error(e) => {
                        return Err(RolloutDataGatherError::AirflowError(e));
                    }
                    PlanQueryResult::Invalid => None,
                    PlanQueryResult::NotFound => None,
                }
            };
        }

        let rollout_stages: Arc<Mutex<Option<Stages>>> = Arc::new(Mutex::new(None));

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
                        let provisional_plan = match retrieve_xcom_from_cache_or_server!(
                            self.provisional_plan,
                            task_instance,
                            "return_value"
                        ) {
                            Some(plan) => plan,
                            None => continue,
                        };
                        rollout_stages
                            .lock()
                            .await
                            .replace((&provisional_plan).into());
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
                let mut rs = rollout_stages.lock().await;
                let rollout_stages = match rs.as_mut() {
                    Some(s) => s,
                    None => continue, // No rollout plan, pointless to try to go through this section.
                };

                let (stage_name, stage_batch_number, task_name) = (
                    &captured[1],
                    str::parse::<NonZero<usize>>(&captured[2]).unwrap(),
                    &captured[3],
                );

                trace!(target: tgt, "{}: processing {} {:?} in state {:?}", task_instance.dag_run_id, task_instance.task_id, task_instance.map_index, task_instance.state);

                let (stage, stage_name_enum) = match StageName::from_str(stage_name) {
                    Ok(StageName::Canary) => (&mut rollout_stages.canary, StageName::Canary),
                    Ok(StageName::Main) => (&mut rollout_stages.main, StageName::Main),
                    Ok(StageName::Unassigned) => {
                        (&mut rollout_stages.unassigned, StageName::Unassigned)
                    }
                    Ok(StageName::Stragglers) => {
                        (&mut rollout_stages.stragglers, StageName::Stragglers)
                    }
                    Err(_) => continue,
                };

                let batch =
                    stage
                        .entry(stage_batch_number)
                        .or_insert(make_a_discovered_host_os_batch(
                            stage_name_enum.clone(),
                            stage_batch_number,
                            &task_instance,
                        ));
                let batch_xcom_data = self
                    .batch_xcoms
                    .entry((stage_name_enum.clone(), stage_batch_number))
                    .or_default();

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
                        if task_name == "plan" {
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
                        TaskInstanceState::Removed => {
                            trace!(target: tgt, "{}: ignoring task instance {} {:?} in state {:?}", task_instance.dag_run_id, task_instance.task_id, task_instance.map_index, task_instance.state);
                        }
                        TaskInstanceState::Skipped => {
                            trans_exact!(BatchState::Skipped);
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
                                "plan" => {
                                    trans_min!(BatchState::Pending);
                                }
                                "wait_until_start_time" => {
                                    trans_min!(BatchState::Waiting);
                                }
                                "collect_nodes" => {
                                    trans_min!(BatchState::DeterminingTargets);
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
                                    if *state == TaskInstanceState::UpForReschedule {
                                        batch.upgraded_nodes = retrieve_xcom_from_cache_or_server!(
                                            batch_xcom_data.nodes_upgrade_status,
                                            task_instance,
                                            "return_value"
                                        );
                                    }
                                    trans_min!(BatchState::WaitingForAdoption);
                                }
                                "wait_until_nodes_healthy" => {
                                    if *state == TaskInstanceState::UpForReschedule {
                                        batch.alerting_nodes = retrieve_xcom_from_cache_or_server!(
                                            batch_xcom_data.nodes_alert_status,
                                            task_instance,
                                            "return_value"
                                        );
                                    }
                                    trans_min!(BatchState::WaitingUntilNodesHealthy);
                                }
                                "join" => {
                                    trans_min!(BatchState::Complete);
                                }
                                &_ => {
                                    warn!(target: tgt, "{}: no info on to handle task instance {} {:?} in state {:?}", task_instance.dag_run_id, task_instance.task_id, task_instance.map_index, task_instance.state);
                                }
                            }
                            update_state_unless_problem!(match stage_name_enum {
                                StageName::Canary => State::Canary,
                                StageName::Main => State::Main,
                                StageName::Unassigned => State::Unassigned,
                                StageName::Stragglers => State::Stragglers,
                            })
                        }
                        TaskInstanceState::Success => {
                            match task_name {
                                "plan" => {
                                    if let Some(selectors) = retrieve_xcom_from_cache_or_server!(
                                        batch_xcom_data.selectors,
                                        task_instance,
                                        "selectors"
                                    ) {
                                        batch.selectors = Some(selectors.clone())
                                    };
                                    if let Some(tolerance) = retrieve_xcom_from_cache_or_server!(
                                        batch_xcom_data.tolerance,
                                        task_instance,
                                        "tolerance"
                                    ) {
                                        batch.tolerance = Some(tolerance.clone())
                                    };
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
                                    trans_exact!(BatchState::DeterminingTargets);
                                }
                                "collect_nodes" => {
                                    if let Some(plan) = retrieve_xcom_from_cache_or_server!(
                                        batch_xcom_data.actual_plans,
                                        task_instance,
                                        "nodes"
                                    ) {
                                        batch.actual_nodes = Some(plan.clone())
                                    };
                                    trans_exact!(BatchState::Proposing);
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
                                    batch.upgraded_nodes = retrieve_xcom_from_cache_or_server!(
                                        batch_xcom_data.nodes_upgrade_status,
                                        task_instance,
                                        "return_value"
                                    );
                                    trans_exact!(BatchState::WaitingUntilNodesHealthy);
                                }
                                "wait_until_nodes_healthy" => {
                                    // We don't have a state for when this task is completed,
                                    // but the join task is not yet.
                                    batch.alerting_nodes = retrieve_xcom_from_cache_or_server!(
                                        batch_xcom_data.nodes_alert_status,
                                        task_instance,
                                        "return_value"
                                    );
                                    trans_exact!(BatchState::WaitingUntilNodesHealthy);
                                }
                                "join" => {
                                    if batch.state != BatchState::Skipped {
                                        trans_exact!(BatchState::Complete);
                                    }
                                    batch.end_time = task_instance.end_date;
                                }
                                &_ => {
                                    warn!(target: tgt, "{}: no info on how to handle task instance {} {:?} in state {:?}", task_instance.dag_run_id, task_instance.task_id, task_instance.map_index, task_instance.state);
                                }
                            };
                            update_state_unless_problem!(match stage_name_enum {
                                StageName::Canary => State::Canary,
                                StageName::Main => State::Main,
                                StageName::Unassigned => State::Unassigned,
                                StageName::Stragglers => State::Stragglers,
                            })
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

        let final_rollout_stages = rollout_stages.lock().await.take();

        // Now remove all rollout stages not part of the original plan that do not
        // have any nodes to roll out to and weren't present in the original plan
        // (and are consequently and inevitably going to be skipped).
        fn keep_batches_planned_or_provisional_with_tasks(
            m: IndexMap<NonZero<usize>, BatchResponse>,
        ) -> IndexMap<NonZero<usize>, BatchResponse> {
            m.into_iter()
                .filter(|(_, v)| {
                    !(
                        // We drop the batch if it has no planned nodes
                        // AND (
                        //   it is either known that there are no actual nodes,
                        //   OR
                        //   no nodes have been assigned to the task yet
                        // )
                        //
                        // Basically if a batch has no planned work, and has
                        // has not yet done any actual work yet, it is dropped.
                        // If it has planned work, it is kept, even if it did
                        // later do no actual work.  If it did not plan any work
                        // but in the end did actual work, it is kept.
                        v.planned_nodes.is_empty()
                            && v.actual_nodes.as_ref().is_none_or(|nodes| nodes.is_empty())
                    )
                })
                .collect()
        }
        self.stages = final_rollout_stages.map(|s| Stages {
            canary: keep_batches_planned_or_provisional_with_tasks(s.canary),
            main: keep_batches_planned_or_provisional_with_tasks(s.main),
            unassigned: keep_batches_planned_or_provisional_with_tasks(s.unassigned),
            stragglers: keep_batches_planned_or_provisional_with_tasks(s.stragglers),
        });

        rollout.stages = self.stages.as_ref().map(|stages| V2Stages {
            canary: stages.canary.iter().map(|(k, v)| (*k, v.into())).collect(),
            main: stages.main.iter().map(|(k, v)| (*k, v.into())).collect(),
            unassigned: stages
                .unassigned
                .iter()
                .map(|(k, v)| (*k, v.into()))
                .collect(),
            stragglers: stages
                .stragglers
                .iter()
                .map(|(k, v)| (*k, v.into()))
                .collect(),
        });

        Ok(RolloutKind::RolloutIcOsToMainnetNodes(rollout))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use rollout_dashboard::types::v2::hostos::{
        NodeAssignment, NodeFailureTolerance, NodeOwner, NodeSelectors, NodeSpecifier, NodeStatus,
        NodesPerGroup,
    };

    use crate::live_state::hostos_rollout::ProvisionalHostOSPlan;

    #[test]

    fn parse_small_plan() {
        let pythonplan = include_str!("fixtures/small_python_structure.txt");
        let theplan = ProvisionalHostOSPlan::from_str(pythonplan).unwrap();
        let batch = &theplan.canary.unwrap()[1];
        let actual = &batch.selectors;
        let expected = NodeSelectors::NodeFilter {
            intersect: vec![NodeSelectors::NodeSpecifier(NodeSpecifier {
                assignment: Some(NodeAssignment::Unassigned),
                owner: Some(NodeOwner::Dfinity),
                nodes_per_group: Some(NodesPerGroup::Absolute(5)),
                group_by: None,
                status: Some(NodeStatus::Healthy),
                datacenter: None,
            })],
        };
        assert_eq!(expected, *actual);
    }

    #[test]
    fn parse_plan_with_tolerances() {
        let pythonplan = include_str!("fixtures/python_structure_with_tolerances.txt");
        let theplan = ProvisionalHostOSPlan::from_str(pythonplan).unwrap();
        let canary = &theplan.canary.unwrap();
        let batch = &canary[0];
        let actual = &batch.tolerance;
        assert_eq!(*actual, Some(NodeFailureTolerance::Absolute(1)));
        let batch = &canary[1];
        let actual = &batch.tolerance;
        assert_eq!(*actual, None);
    }

    #[test]
    fn parse_typoed_plan() {
        let pythonplan = include_str!("fixtures/typoed_python_structure.txt");
        if ProvisionalHostOSPlan::from_str(pythonplan).is_ok() {
            panic!("Did not error out despite having a bad datetime")
        }
    }

    #[test]
    fn parse_ginormous_plan() {
        let pythonplan = include_str!("fixtures/ginormous_python_structure.txt");
        let data = ProvisionalHostOSPlan::from_str(pythonplan).unwrap();
        let selectors = match &data.clone().canary.unwrap()[0].selectors {
            NodeSelectors::NodeFilter { intersect } => intersect.clone(),
            _ => panic!("The selector was not a filter."),
        };
        let spec = match &selectors[0] {
            NodeSelectors::NodeSpecifier(spec) => spec,
            _ => panic!("The subselector was not a specifier."),
        };
        assert_eq!(spec.assignment.clone().unwrap(), NodeAssignment::Unassigned);
    }
}
