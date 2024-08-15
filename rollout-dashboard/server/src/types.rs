//! Contains useful types to deserialize the result of the API calls
//! made available by the rollout dashboard REST API.

use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
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
}

impl Rollout {
    pub fn new(
        name: String,
        display_url: String,
        note: Option<String>,
        dispatch_time: DateTime<Utc>,
        last_scheduling_decision: Option<DateTime<Utc>>,
        conf: HashMap<String, serde_json::Value>,
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
