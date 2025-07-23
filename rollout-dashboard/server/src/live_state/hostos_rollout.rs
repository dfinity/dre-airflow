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
use rollout_dashboard::types::unstable::{self, NodeInfo};
use rollout_dashboard::types::v2::RolloutKind;
use rollout_dashboard::types::v2::hostos::{Batch, BatchState, Node, Rollout, Stages, State};
use serde::Deserialize;
use std::cmp::max;
use std::cmp::min;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{self, Display};
use std::str::FromStr;
use std::sync::Arc;
use std::{vec, vec::Vec};
use tokio::sync::Mutex;

const LOG_TARGET: &str = "live_state::hostos_rollout";

lazy_static! {
    // unwrap() is legitimate here because we know these cannot fail to compile.
    static ref BatchIdentificationRe: Regex = Regex::new("(canary|main|unassigned|stragglers)_([0-9]+)[.](.+)").unwrap();
}

/*

This code will be used later to provide a detail view of the rollout.


#[derive(Debug, Deserialize, Clone, EnumString)]
enum NodeAssignment {
    #[serde(rename = "assigned")]
    Assigned,
    #[serde(rename = "unassigned")]
    Unassigned,
}

#[derive(Debug, Deserialize, Clone, EnumString)]
enum NodeOwner {
    #[serde(rename = "DFINITY")]
    Dfinity,
    #[serde(rename = "others")]
    Others,
}

fn deserialize_nodes_per_group<'de, D>(deserializer: D) -> Result<Option<NodesPerGroup>, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    struct CustomVisitor;

    impl<'de> serde::de::Visitor<'de> for CustomVisitor {
        type Value = Option<NodesPerGroup>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter
                .write_str("either a nonzero nonnegative integer or a float between 0.0 and 1.0")
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(None)
        }

        fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            if v < 0 {
                return Err(serde::de::Error::invalid_value(
                    serde::de::Unexpected::Signed(v),
                    &self,
                ));
            }
            Ok(Some(NodesPerGroup::Absolute(v.try_into().unwrap())))
        }

        fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(Some(NodesPerGroup::Absolute(v.try_into().unwrap())))
        }

        fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            if !(0.0..=1.0).contains(&v) {
                return Err(serde::de::Error::invalid_value(
                    serde::de::Unexpected::Float(v),
                    &self,
                ));
            }
            Ok(Some(NodesPerGroup::Ratio(v)))
        }
    }

    deserializer.deserialize_any(CustomVisitor)
}

#[derive(Debug, Deserialize, Clone)]
enum NodesPerGroup {
    Absolute(usize),
    Ratio(f64),
}

#[derive(Debug, Deserialize, Clone)]
enum GroupBy {
    #[serde(rename = "datacenter")]
    Datacenter,
    #[serde(rename = "subnet")]
    Subnet,
}

#[derive(Debug, Deserialize, Clone)]
struct Selector {
    assignment: Option<NodeAssignment>,
    owner: Option<NodeOwner>,
    #[serde(deserialize_with = "deserialize_nodes_per_group")]
    nodes_per_group: Option<NodesPerGroup>,
    group_by: Option<GroupBy>,
}

type Selectors = Vec<Selector>;

*/

#[derive(Debug, Deserialize, Clone)]
pub(super) struct ProvisionalPlanBatch {
    pub(super) nodes: Vec<NodeInfo>,
    // selectors: Selectors,
    pub(super) start_at: PythonDateTime,
}

impl From<ProvisionalPlanBatch> for unstable::ProvisionalPlanBatch {
    fn from(other: ProvisionalPlanBatch) -> unstable::ProvisionalPlanBatch {
        unstable::ProvisionalPlanBatch {
            nodes: other.nodes,
            start_at: other.start_at.into(),
        }
    }
}

impl From<&ProvisionalPlanBatch> for Batch {
    fn from(val: &ProvisionalPlanBatch) -> Self {
        Batch {
            planned_start_time: val.start_at.clone().into(),
            actual_start_time: None,
            end_time: None,
            state: BatchState::Unknown,
            comment: "".to_string(),
            display_url: "".into(),
            planned_nodes: val
                .nodes
                .iter()
                .map(|n| Node {
                    node_id: n.node_id.clone(),
                })
                .collect(),
            actual_nodes: None,
            present_in_provisional_plan: true,
        }
    }
}

/// This function is only called when a task instance that does not correspond
/// to any known batch appears.  This can happen when nodes are added mid-rollout,
/// and therefore the planning logic did not take them into account, but a late
/// batch in the rollout finds them and goes to town on them.
fn make_a_discovered_host_os_batch(val: &TaskInstancesResponseItem) -> Batch {
    Batch {
        planned_start_time: val.earliest_date(),
        actual_start_time: None,
        end_time: None,
        state: BatchState::Unknown,
        comment: "".to_string(),
        display_url: "".into(),
        planned_nodes: vec![],
        actual_nodes: None,
        present_in_provisional_plan: false,
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

#[derive(Debug, Deserialize, Clone)]
pub(super) struct ProvisionalHostOSPlan {
    pub(super) canary: Option<Vec<ProvisionalPlanBatch>>,
    pub(super) main: Option<Vec<ProvisionalPlanBatch>>,
    pub(super) unassigned: Option<Vec<ProvisionalPlanBatch>>,
    pub(super) stragglers: Option<Vec<ProvisionalPlanBatch>>,
}

#[derive(Clone, PartialEq, Hash, Eq)]
pub(super) enum StageName {
    Canary,
    Main,
    Unassigned,
    Stragglers,
}

pub struct InvalidStageName;

impl FromStr for StageName {
    type Err = InvalidStageName;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "canary" => Ok(StageName::Canary),
            "main" => Ok(StageName::Main),
            "unassigned" => Ok(StageName::Unassigned),
            "stragglers" => Ok(StageName::Stragglers),
            _ => Err(InvalidStageName {}),
        }
    }
}

impl ProvisionalHostOSPlan {
    pub(super) fn get_stage(&self, stage: StageName) -> &Option<Vec<ProvisionalPlanBatch>> {
        match stage {
            StageName::Canary => &self.canary,
            StageName::Main => &self.main,
            StageName::Unassigned => &self.unassigned,
            StageName::Stragglers => &self.stragglers,
        }
    }
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

impl From<&ProvisionalHostOSPlan> for Stages {
    fn from(val: &ProvisionalHostOSPlan) -> Self {
        Stages {
            canary: val
                .canary
                .clone()
                .map(|z| {
                    z.iter()
                        .enumerate()
                        .map(|(n, b)| (n + 1, b.into()))
                        .collect()
                })
                .unwrap_or_default(),
            main: val
                .main
                .clone()
                .map(|z| {
                    z.iter()
                        .enumerate()
                        .map(|(n, b)| (n + 1, b.into()))
                        .collect()
                })
                .unwrap_or_default(),
            unassigned: val
                .unassigned
                .clone()
                .map(|z| {
                    z.iter()
                        .enumerate()
                        .map(|(n, b)| (n + 1, b.into()))
                        .collect()
                })
                .unwrap_or_default(),
            stragglers: val
                .stragglers
                .clone()
                .map(|z| {
                    z.iter()
                        .enumerate()
                        .map(|(n, b)| (n + 1, b.into()))
                        .collect()
                })
                .unwrap_or_default(),
        }
    }
}

type ActualHostPlan = Vec<NodeInfo>;

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
    batch: &mut Batch,
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

#[derive(Clone, Default)]
pub(super) struct Parser {
    pub(super) provisional_plan: PlanCache<ProvisionalHostOSPlan>,
    pub(super) actual_plans: HashMap<(StageName, usize), PlanCache<ActualHostPlan>>,
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

        let rollout_stages: Arc<Mutex<Option<Stages>>> = Arc::new(Mutex::new(None));

        // Now update rollout and batch state based on the obtained data.
        // What this process does is fairly straightforward:
        // * for each and every known up-to-date Airflow task in the cache
        //   (always processed in topological order),
        for task_instance in linearized_tasks.into_iter() {
            let tgt = &format!("{}::subnet_state", LOG_TARGET);

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
                        let provisional_plan = match self
                            .provisional_plan
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
                            PlanQueryResult::Found(plan) => plan,
                            PlanQueryResult::Invalid => {
                                continue;
                            }
                            PlanQueryResult::NotFound => {
                                continue;
                            }
                            PlanQueryResult::Error(e) => {
                                return Err(RolloutDataGatherError::AirflowError(e));
                            }
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
                    str::parse::<usize>(&captured[2]).unwrap(),
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

                let batch = stage
                    .entry(stage_batch_number)
                    .or_insert(make_a_discovered_host_os_batch(&task_instance));

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
                                    let actual_plan = self
                                        .actual_plans
                                        .entry((stage_name_enum.clone(), stage_batch_number))
                                        .or_insert(PlanCache::Unretrieved);

                                    match actual_plan
                                        .get(
                                            &task_instance,
                                            fetch_xcom(
                                                airflow_api.clone(),
                                                dag_run.dag_id.as_str(),
                                                dag_run.dag_run_id.as_str(),
                                                task_instance.task_id.as_str(),
                                                task_instance.map_index,
                                                "nodes",
                                            ),
                                        )
                                        .await
                                    {
                                        PlanQueryResult::Found(plan) => {
                                            batch.actual_nodes = Some(
                                                plan.iter()
                                                    .map(|n| Node {
                                                        node_id: n.node_id.clone(),
                                                    })
                                                    .collect(),
                                            )
                                        }
                                        PlanQueryResult::Invalid => {}
                                        PlanQueryResult::NotFound => {}
                                        PlanQueryResult::Error(e) => {
                                            return Err(RolloutDataGatherError::AirflowError(e));
                                        }
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
                                    trans_exact!(BatchState::WaitingUntilNodesHealthy);
                                }
                                "wait_until_nodes_healthy" => {
                                    // We don't have a state for when this task is completed,
                                    // but the join task is not yet.
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
        fn keep_batches_planned_or_provisional_with_tasks(m: &mut IndexMap<usize, Batch>) {
            let _: IndexMap<_, _> = m
                .extract_if(.., |_, v| {
                    v.actual_nodes.clone().unwrap_or_default().is_empty()
                        && !v.present_in_provisional_plan
                })
                .collect();
        }
        rollout.stages = match final_rollout_stages {
            Some(mut stages) => {
                keep_batches_planned_or_provisional_with_tasks(&mut stages.canary);
                keep_batches_planned_or_provisional_with_tasks(&mut stages.main);
                keep_batches_planned_or_provisional_with_tasks(&mut stages.unassigned);
                keep_batches_planned_or_provisional_with_tasks(&mut stages.stragglers);
                Some(stages)
            }
            None => None,
        };

        Ok(RolloutKind::RolloutIcOsToMainnetNodes(rollout))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::live_state::hostos_rollout::ProvisionalHostOSPlan;

    #[test]

    fn parse_small_plan() {
        let pythonplan = include_str!("fixtures/small_python_structure.txt");
        let _ = ProvisionalHostOSPlan::from_str(pythonplan).unwrap();
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
        let _ = ProvisionalHostOSPlan::from_str(pythonplan).unwrap();
    }
}
