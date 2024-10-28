//! Contains useful types to deserialize the result of the API calls
//! made available by the rollout dashboard REST API.

use crate::airflow_client::{DagsResponse, DagsResponseItem};
use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use reqwest::StatusCode;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::vec::Vec;
use strum::Display;

#[derive(Serialize, Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Display, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Represents the rollout state of a subnet.
// Ordering matters here.
pub enum SubnetRolloutState {
    Error,
    PredecessorFailed,
    Pending,
    Waiting,
    Proposing,
    WaitingForElection,
    WaitingForAdoption,
    WaitingForAlertsGone,
    Complete,
    Unknown,
}

#[derive(Serialize, Debug, Clone, Deserialize)]
/// Represents a subnet to be upgraded as part of a batch in a rollout.
pub struct Subnet {
    /// Long-form subnet ID.
    pub subnet_id: String,
    /// Git revision of the IC OS GuestOS to deploy to the subnet.
    pub git_revision: String,
    pub state: SubnetRolloutState,
    /// Shows a comment for the subnet if it is available; else it contains an empty string.
    pub comment: String,
    /// Links to the specific task within Airflow that this subnet is currently performing; else it contains an empty string.
    pub display_url: String,
}

#[derive(Serialize, Debug, Clone, Deserialize)]
/// Represents a batch of subnets to upgrade.
pub struct Batch {
    /// The time the batch was programmed to start at.
    pub planned_start_time: DateTime<Utc>,
    /// The actual observed start time of the batch.
    pub actual_start_time: Option<DateTime<Utc>>,
    /// The time of the last action associated with this batch, if any.
    pub end_time: Option<DateTime<Utc>>,
    /// A list of subnets to be upgraded as part of this batch.
    pub subnets: Vec<Subnet>,
}
impl Batch {}

#[derive(Serialize, Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Represents the rollout state.
// Ordering matters here.
pub enum RolloutState {
    /// The rollout has failed or was abandoned by the operator.  It is not executing any longer.
    Failed,
    /// The rollout is experiencing a retryable issue.  It continues to execute.
    Problem,
    /// The rollout is in the planning stage.
    Preparing,
    /// The rollout is waiting until all preconditions have been met.
    Waiting,
    /// The rollout is upgrading subnets batch by batch.
    UpgradingSubnets,
    /// The rollout is upgrading unassigned nodes.
    UpgradingUnassignedNodes,
    /// The rollout has finished successfully or was marked as such by the operator.
    Complete,
}

#[derive(Debug, Serialize, Clone, Deserialize)]
/// Represents an IC OS rollout.
pub struct Rollout {
    /// Unique, enforced by Airflow, corresponds to DAG run ID.
    pub name: String,
    /// Link to the rollout screen in Airflow.
    pub display_url: String,
    /// Note set on the rollout by the operator.
    pub note: Option<String>,
    pub state: RolloutState,
    pub dispatch_time: DateTime<Utc>,
    /// Last scheduling decision.
    /// Due to the way the central rollout cache is updated, clients may not see
    /// an up-to-date value that corresponds to Airflow's last update time for
    /// the DAG run.  See documentation in function `get_rollout_data`.
    pub last_scheduling_decision: Option<DateTime<Utc>>,
    /// Associative array of `{batch ID -> Batch}` planned for the rollout.
    pub batches: IndexMap<usize, Batch>,
    /// Configuration associated to the rollout.
    pub conf: HashMap<String, serde_json::Value>,
    pub update_count: usize,
}

impl Rollout {
    pub fn new(
        name: String,
        display_url: String,
        note: Option<String>,
        dispatch_time: DateTime<Utc>,
        last_scheduling_decision: Option<DateTime<Utc>>,
        conf: HashMap<String, serde_json::Value>,
        update_count: usize,
    ) -> Self {
        Self {
            name,
            display_url,
            note,
            state: RolloutState::Complete,
            dispatch_time,
            last_scheduling_decision,
            batches: IndexMap::new(),
            conf,
            update_count,
        }
    }
}

/// List of rollouts.
///
/// The API call `/api/v1/rollouts` returns this in JSON format as its content,
/// when the information the rollout dashboard backend has collected is
/// complete and free of errors.
///
/// Rollouts are always returned in reverse chronological order -- the most
/// recent comes first, and the last item is the oldest rollout.
pub type Rollouts = Vec<Rollout>;

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
/// Rollout engine state.
///
/// The API call `/api/v1/engine_state` returns this in JSON format as its content.
/// If Airflow itself is malfunctioning, there is no guarantee that this will be up-to-date.
pub enum RolloutEngineState {
    Missing,
    Broken,
    Paused,
    Inactive,
    Active,
}

impl From<DagsResponse> for RolloutEngineState {
    fn from(resp: DagsResponse) -> Self {
        match resp.dags.len() {
            0 => RolloutEngineState::Missing,
            _ => match resp.dags[0] {
                DagsResponseItem {
                    has_import_errors: true,
                    ..
                } => Self::Broken,
                DagsResponseItem {
                    is_paused: true, ..
                } => Self::Paused,
                DagsResponseItem {
                    is_active: false, ..
                } => Self::Inactive,
                DagsResponseItem {
                    is_active: true,
                    is_paused: false,
                    has_import_errors: false,
                    ..
                } => Self::Active,
            },
        }
    }
}

#[derive(Serialize, Default)]
/// Incremental state update sent by Airflow via its SSE update endpoint.
///
/// To query this endpoint, use server-sent events client with URL
/// /api/v1/sse/rollouts_view or URL /api/v1/rollouts/sse (for compatibility
/// with old clients that do not support incremental updates).
///
/// By default, when invoked through /api/v1/rollouts/sse, only full updates
/// of rollouts (the rollouts list) will be filled, or possibly the error
/// field.  If /api/v1/sse/rollouts_view is used, or the query string parameter
/// `incremental` is specified (and not valued false) in /api/v1/rollouts/sse,
/// then the first update the client receives (and each update right after
/// the dashboard has sent an error to the client) will contain the full
/// rollouts list in the rollouts member, and subsequent updates will only
/// include the updated and deleted members, if there are any updated or
/// deleted rollouts.
pub struct RolloutsViewDelta {
    #[serde(skip_serializing_if = "Option::is_none")]
    rollouts: Option<VecDeque<Rollout>>,
    #[serde(skip_serializing_if = "VecDeque::is_empty")]
    updated: VecDeque<Rollout>,
    #[serde(skip_serializing_if = "VecDeque::is_empty")]
    deleted: VecDeque<String>,
    error: Option<(u16, String)>,
    #[serde(skip_serializing_if = "Option::is_none")]
    engine_state: Option<RolloutEngineState>,
}

impl RolloutsViewDelta {
    pub fn error(e: &(StatusCode, String)) -> Self {
        Self {
            error: Some((e.0.as_u16(), e.1.clone())),
            ..Default::default()
        }
    }
    pub fn full(engine_state: &RolloutEngineState, rollouts: &VecDeque<Rollout>) -> Self {
        Self {
            rollouts: Some(rollouts.clone()),
            engine_state: Some(engine_state.clone()),
            ..Default::default()
        }
    }
    pub fn partial(
        engine_state: &RolloutEngineState,
        updated: &VecDeque<Rollout>,
        deleted: &VecDeque<String>,
    ) -> Self {
        Self {
            updated: updated.clone(),
            deleted: deleted.clone(),
            engine_state: Some(engine_state.clone()),
            ..Default::default()
        }
    }
}
