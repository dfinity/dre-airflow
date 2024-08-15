use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use serde::Serialize;
use std::collections::HashMap;
use std::vec::Vec;
use strum::Display;

#[derive(Serialize, Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Display)]
#[serde(rename_all = "snake_case")]
/// Represents the rollout state of a subnet.
/// Ordering matters here.
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
pub struct Subnet {
    pub subnet_id: String,
    pub git_revision: String,
    pub state: SubnetRolloutState,
    /// Shows a comment for the subnet if available, else empty string.
    pub comment: String,
    /// Shows a display URL if available, else empty string.
    pub display_url: String,
}

#[derive(Serialize, Debug, Clone, Deserialize)]
pub struct Batch {
    pub planned_start_time: DateTime<Utc>,
    pub actual_start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub subnets: Vec<Subnet>,
}
impl Batch {}

#[derive(Serialize, Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Represents the rollout state.  Ordering matters here.
pub enum RolloutState {
    Failed,
    Problem,
    Preparing,
    Waiting,
    UpgradingSubnets,
    UpgradingUnassignedNodes,
    Complete,
}

#[derive(Debug, Serialize, Clone, Deserialize)]
pub struct Rollout {
    /// name is unique, enforced by Airflow.
    pub name: String,
    /// Links to the rollout in Airflow.
    pub display_url: String,
    pub note: Option<String>,
    pub state: RolloutState,
    pub dispatch_time: DateTime<Utc>,
    /// Last scheduling decision.
    /// Due to the way the central rollout cache is updated, clients may not see
    /// an up-to-date value that corresponds to Airflow's last update time for
    /// the DAG run.  See documentation in get_rollout_data.
    pub last_scheduling_decision: Option<DateTime<Utc>>,
    pub batches: IndexMap<usize, Batch>,
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
