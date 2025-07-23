//! Contains useful types to deserialize the result of the API calls
//! made available by the rollout dashboard REST API.

pub mod v1 {
    use crate::airflow_client::{DagsResponse, DagsResponseItem};
    use chrono::{DateTime, Utc};
    use indexmap::IndexMap;
    use reqwest::StatusCode;
    use serde::Deserialize;
    use serde::Serialize;
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

    #[derive(Serialize, Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Deserialize, Default)]
    #[serde(rename_all = "snake_case")]
    /// Represents the rollout state.
    // Ordering matters here.
    pub enum RolloutState {
        /// The rollout has failed or was abandoned by the operator.  It is not executing any longer.
        #[default]
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
        pub conf: IndexMap<String, serde_json::Value>,
        pub update_count: usize,
    }

    impl Rollout {
        pub fn new(
            name: String,
            display_url: String,
            note: Option<String>,
            dispatch_time: DateTime<Utc>,
            last_scheduling_decision: Option<DateTime<Utc>>,
            conf: IndexMap<String, serde_json::Value>,
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

    impl From<super::v1::Rollout> for super::v2::Rollout {
        fn from(rollout: super::v1::Rollout) -> super::v2::Rollout {
            super::v2::Rollout {
                kind: super::v2::RolloutKind::RolloutIcOsToMainnetSubnets(
                    super::v2::guestos::Rollout {
                        state: rollout.state,
                        batches: rollout.batches,
                        conf: rollout.conf,
                    },
                ),
                display_url: rollout.display_url,
                dispatch_time: rollout.dispatch_time,
                last_scheduling_decision: rollout.last_scheduling_decision,
                note: rollout.note,
                update_count: rollout.update_count,
                name: rollout.name,
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
    pub struct DeltaState {
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

    impl DeltaState {
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
}

pub mod v2 {

    use crate::airflow_client::{DagsResponse, DagsResponseItem};
    use chrono::{DateTime, Utc};
    use indexmap::IndexMap;
    use reqwest::StatusCode;
    use serde::Serialize;
    use serde::Serializer;
    use std::collections::VecDeque;

    pub mod guestos {
        pub use super::super::v1::{
            Batch, RolloutState as State, Subnet, SubnetRolloutState as SubnetState,
        };
        use indexmap::IndexMap;
        use serde::Serialize;

        /// Represents a rollout of GuestOS to mainnet subnets.
        #[derive(Debug, Serialize, Clone, Default)]
        pub struct Rollout {
            pub state: State,
            /// Associative array of `{batch ID -> Batch}` planned for the rollout.
            pub batches: IndexMap<usize, Batch>,
            /// Configuration associated to the rollout.
            pub conf: IndexMap<String, serde_json::Value>,
        }
    }

    pub mod api_boundary_nodes {
        use chrono::{DateTime, Utc};
        use indexmap::IndexMap;
        use serde::Serialize;
        use std::vec::Vec;
        use strum::Display;

        #[derive(Serialize, Debug, Clone, PartialEq, PartialOrd, Eq, Ord)]
        #[serde(rename_all = "snake_case")]
        /// Represents the state of the rollout using this struct.
        // Ordering matters here.
        pub enum State {
            /// The rollout has failed or was abandoned by the operator.  It is not executing any longer.
            Failed,
            /// The rollout is experiencing a retryable issue.  It continues to execute.
            Problem,
            /// The rollout is in the planning stage.
            /// This is true if the rollout is still executing task
            /// schedule.
            Preparing,
            /// The rollout is waiting until all preconditions have been met.
            /// This is true if the task schedule is done, but the tasks
            /// wait_for_other_rollouts and wait_for_revision_to_be_elected
            /// are not finished yet.
            Waiting,
            /// The rollout is upgrading API boundary nodes batch by batch.
            UpgradingApiBoundaryNodes,
            /// The rollout has finished successfully or was marked as such by the operator.
            Complete,
        }

        /// Represents an API boundary node being targeted by the rollout.
        #[derive(Serialize, Debug, Clone)]
        pub struct Node {
            /// This is the node ID.
            pub node_id: String,
        }

        #[derive(Serialize, Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Display)]
        #[serde(rename_all = "snake_case")]
        /// Represents the rollout state of a subnet.
        // Ordering matters here.
        pub enum BatchState {
            Error,
            PredecessorFailed,
            Pending,
            Waiting,
            Proposing,
            WaitingForElection,
            WaitingForAdoption,
            WaitingUntilNodesHealthy,
            Complete,
            Unknown,
        }

        #[derive(Serialize, Debug, Clone)]
        /// Represents a batch of subnets to upgrade.
        pub struct Batch {
            /// The time the batch was programmed to start at.
            pub planned_start_time: DateTime<Utc>,
            /// The actual observed start time of the batch.
            pub actual_start_time: Option<DateTime<Utc>>,
            /// The time of the last action associated with this batch, if any.
            pub end_time: Option<DateTime<Utc>>,
            pub state: BatchState,
            /// Shows a comment for the batch if it is available; else it contains an empty string.
            pub comment: String,
            /// Links to the specific task within Airflow that this batch is currently performing; else it contains an empty string.
            pub display_url: String,
            /// A list of API boundary nodes to be upgraded as part of this batch.
            pub api_boundary_nodes: Vec<Node>,
        }

        /// Represents a rollout of GuestOS to mainnet API boundary nodes.
        #[derive(Debug, Serialize, Clone)]
        pub struct Rollout {
            pub state: State,
            /// Associative array of `{batch ID -> Batch}` planned for the rollout.
            pub batches: IndexMap<usize, Batch>,
            /// Configuration associated to the rollout.
            pub conf: IndexMap<String, serde_json::Value>,
        }
    }

    pub mod hostos {

        use chrono::{DateTime, Utc};
        use indexmap::IndexMap;
        use serde::Serialize;
        use strum::Display;

        #[derive(Serialize, Debug, Clone, PartialEq, PartialOrd, Eq, Ord)]
        #[serde(rename_all = "snake_case")]
        /// Represents the state of the rollout using this struct.
        // Ordering matters here.
        pub enum State {
            /// The rollout has failed or was abandoned by the operator.  It is not executing any longer.
            Failed,
            /// The rollout is experiencing a retryable issue.  It continues to execute.
            Problem,
            /// The rollout is in the planning stage.
            /// This is true if the rollout is still executing task
            /// schedule.
            Preparing,
            /// The rollout is waiting until all preconditions have been met.
            /// This is true if the task schedule is done, but the tasks
            /// wait_for_other_rollouts and wait_for_revision_to_be_elected
            /// are not finished yet.
            Waiting,
            /// The rollout is upgrading nodes in the canary stage.
            Canary,
            /// The rollout is upgrading nodes in the main stage.
            Main,
            /// The rollout is upgrading nodes in the unassigned stage.
            Unassigned,
            /// The rollout is updating stragglers.
            Stragglers,
            /// The rollout has finished successfully or was marked as such by the operator.
            Complete,
        }

        #[derive(Serialize, Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Display)]
        #[serde(rename_all = "snake_case")]
        /// Represents the rollout state of a subnet.
        // Ordering matters here.
        pub enum BatchState {
            Error,
            PredecessorFailed,
            Pending,
            Waiting,
            DeterminingTargets,
            Proposing,
            WaitingForElection,
            WaitingForAdoption,
            WaitingUntilNodesHealthy,
            Complete,
            /// The state a batch is when plan skips ahead (wait_until_start_time is skipped)
            /// or collect_nodes skips ahead (create_proposal_if_none_exists is skipped).
            Skipped,
            Unknown,
        }

        /// Represents a HostOS node being targeted by the rollout.
        #[derive(Serialize, Debug, Clone)]
        pub struct Node {
            /// This is the node ID.
            pub node_id: String,
        }

        #[derive(Serialize, Debug, Clone)]
        /// Represents a batch of subnets to upgrade.
        pub struct Batch {
            /// The time the batch was programmed to start at.
            pub planned_start_time: DateTime<Utc>,
            /// The actual observed start time of the batch.
            pub actual_start_time: Option<DateTime<Utc>>,
            /// The time of the last action associated with this batch, if any.
            pub end_time: Option<DateTime<Utc>>,
            pub state: BatchState,
            /// Shows a comment for the batch if it is available; else it contains an empty string.
            pub comment: String,
            /// Links to the specific task within Airflow that this batch is currently performing; else it contains an empty string.
            pub display_url: String,
            /// A count of the nodes planned to be upgraded as part of this batch.
            pub planned_nodes: Vec<Node>,
            /// A count of the nodes that actually were or are upgraded as part of this batch.
            /// Usually updated after collect_nodes has executed and has obtained a list of nodes.
            /// If that phase of the batch has yet to take place, this is usually null.
            pub actual_nodes: Option<Vec<Node>>,
            #[serde(skip_serializing)]
            /// Internal flag used to prune batches that haven't yet run, or have run but
            /// had no nodes assigned to them.
            pub present_in_provisional_plan: bool,
        }

        ///  Represents a particular stage in the HostOS rollout.
        #[derive(Debug, Serialize, Clone)]
        pub struct Stages {
            pub canary: IndexMap<usize, Batch>,
            pub main: IndexMap<usize, Batch>,
            pub unassigned: IndexMap<usize, Batch>,
            pub stragglers: IndexMap<usize, Batch>,
        }

        /// Represents a rollout of HostOS to nodes.
        #[derive(Debug, Serialize, Clone)]
        pub struct Rollout {
            pub state: State,
            /// Is None when the rollout has yet to compute a set of stages (rollout plan).
            pub stages: Option<Stages>,
            /// Configuration associated to the rollout.
            pub conf: IndexMap<String, serde_json::Value>,
        }
    }

    #[derive(Debug, Serialize, Clone)]
    #[serde(tag = "kind")]
    #[serde(rename_all = "snake_case")]
    #[allow(clippy::large_enum_variant)]
    pub enum RolloutKind {
        RolloutIcOsToMainnetSubnets(guestos::Rollout),
        RolloutIcOsToMainnetApiBoundaryNodes(api_boundary_nodes::Rollout),
        RolloutIcOsToMainnetNodes(hostos::Rollout),
    }
    /// Represents a generic rollout of any kind.
    #[derive(Debug, Serialize, Clone)]
    pub struct Rollout {
        /// Unique, enforced by Airflow, corresponds to DAG run ID.
        pub name: String,
        /// Link to the rollout screen in Airflow.
        pub display_url: String,
        /// Optional note a rollout can have added by the administrator.
        pub note: Option<String>,
        /// Time that the rollout was dispatched.
        pub dispatch_time: DateTime<Utc>,
        /// Last scheduling decision.
        /// Due to the way the central rollout cache is updated, clients may not see
        /// an up-to-date value that corresponds to Airflow's last update time for
        /// the DAG run.  See documentation in function `get_rollout_data`.
        pub last_scheduling_decision: Option<DateTime<Utc>>,
        /// Whenever a rollout has changed, this field is incremented.  This is used
        /// to determine which rollouts have changed in the SSE API.
        pub update_count: usize,
        #[serde(flatten)]
        /// Data about the specific type of rollout this is.  Tagged "kind" before
        /// serializing the specific data in question.
        pub kind: RolloutKind,
    }

    impl Rollout {
        pub fn kind(&self) -> String {
            match self.kind {
                RolloutKind::RolloutIcOsToMainnetSubnets(_) => {
                    "rollout_ic_os_to_mainnet_subnets".to_string()
                }
                RolloutKind::RolloutIcOsToMainnetApiBoundaryNodes(_) => {
                    "rollout_ic_os_to_mainnet_api_boundary_nodes".to_string()
                }
                RolloutKind::RolloutIcOsToMainnetNodes(_) => {
                    "rollout_ic_os_to_mainnet_nodes".to_string()
                }
            }
        }
        pub fn key(&self) -> String {
            self.kind() + &self.name
        }
    }

    impl TryFrom<super::v2::Rollout> for super::v1::Rollout {
        type Error = &'static str;
        fn try_from(rollout: super::v2::Rollout) -> Result<super::v1::Rollout, Self::Error> {
            let (state, batches, conf) = match &rollout.kind {
                RolloutKind::RolloutIcOsToMainnetSubnets(s) => {
                    (s.state.clone(), s.batches.clone(), s.conf.clone())
                }
                _ => Err("this rollout is incompatible with conversion")?,
            };

            Ok(super::v1::Rollout {
                name: rollout.name,
                display_url: rollout.display_url,
                update_count: rollout.update_count,
                note: rollout.note,
                dispatch_time: rollout.dispatch_time,
                last_scheduling_decision: rollout.last_scheduling_decision,
                state: state.clone(),
                batches: batches.clone(),
                conf: conf.clone(),
            })
        }
    }

    /// Represents a deleted rollout.  This message type is
    /// sent in SSE events to notify clients that a rollout
    /// has been removed from Airflow.
    #[derive(Serialize)]
    pub struct DeletedRollout {
        pub kind: String,
        pub name: String,
    }

    #[derive(Serialize, Debug, Clone, PartialEq, Eq)]
    #[serde(rename_all = "snake_case")]
    /// Represents a rollout engine state.  Each rollout type has its own
    /// engine, which can be in a number of different states.
    pub enum RolloutEngineState {
        Broken,
        Paused,
        Inactive,
        Active,
    }

    /// Represents one or more engine update states.  May be empty,
    /// but it usually contains the states of each rollout engine in a
    /// full status update, and the states changed in between incremental
    /// updates.  The key of the dictionary is the kind of the rollout,
    /// and the value is the state of that rollout's engine.
    pub type RolloutEngineStates = IndexMap<String, RolloutEngineState>;

    impl From<DagsResponse> for RolloutEngineStates {
        /// Returns an IndexMap of DAG engine states, sorted by
        /// DAG ID (the keys of the map).
        fn from(resp: DagsResponse) -> Self {
            let mut res: Vec<DagsResponseItem> = resp.dags.into_iter().collect();
            res.sort_by_key(|key| key.dag_id.clone());
            res.into_iter()
                .map(|dag| {
                    (
                        dag.dag_id.clone(),
                        match dag {
                            DagsResponseItem {
                                has_import_errors: true,
                                ..
                            } => RolloutEngineState::Broken,
                            DagsResponseItem {
                                is_paused: true, ..
                            } => RolloutEngineState::Paused,
                            DagsResponseItem {
                                is_active: false, ..
                            } => RolloutEngineState::Inactive,
                            DagsResponseItem {
                                is_active: true,
                                is_paused: false,
                                has_import_errors: false,
                                ..
                            } => RolloutEngineState::Active,
                        },
                    )
                })
                .collect()
        }
    }

    impl From<RolloutEngineStates> for super::v1::RolloutEngineState {
        fn from(val: RolloutEngineStates) -> Self {
            match val.get("rollout_ic_os_to_mainnet_subnets") {
                None => super::v1::RolloutEngineState::Missing,
                Some(RolloutEngineState::Active) => super::v1::RolloutEngineState::Active,
                Some(RolloutEngineState::Broken) => super::v1::RolloutEngineState::Broken,
                Some(RolloutEngineState::Paused) => super::v1::RolloutEngineState::Paused,
                Some(RolloutEngineState::Inactive) => super::v1::RolloutEngineState::Inactive,
            }
        }
    }

    pub type Rollouts = VecDeque<Rollout>;

    #[derive(Serialize, Clone, Default)]
    pub struct State {
        pub rollouts: Rollouts,
        pub rollout_engine_states: RolloutEngineStates,
    }

    pub fn serialize_status_code<S>(e: &StatusCode, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let code: u16 = (*e).into();
        s.serialize_u16(code)
    }

    /// State update error sent by the dashboard backend via its SSE update endpoint
    /// and by the /api/v2/state endpoint when there is an error.
    ///
    /// Sent when there has been an error updating the state from Airflow.
    /// After receiving this, the client should consider its internal state stale.
    #[derive(Serialize, Clone, PartialEq, Eq)]
    pub struct Error {
        // fixme does not need to be struct, or code and message shoudl be separate instead of a tuple
        #[serde(serialize_with = "serialize_status_code")]
        pub code: StatusCode,
        pub message: String,
    }

    /// The two variants of full state that can be sent by
    /// non-SSE endpoints, and also stored internally.
    /// Contains either a list of rollouts ordered from newest to oldest,
    /// dated from the last time it was successfully updated, or an HTTP
    /// status code corresponding to -- and with -- a message for the last error.
    #[derive(Serialize, Clone)]
    #[serde(tag = "untagged")]
    pub enum StateResponse {
        State(State),
        Error(Error),
    }

    pub mod sse {
        use super::{DeletedRollout, Error, Rollout, RolloutEngineStates, State};
        use serde::Serialize;
        use std::collections::VecDeque;

        #[derive(Serialize)]
        pub struct RolloutsDelta {
            pub updated: VecDeque<Rollout>,
            pub deleted: VecDeque<DeletedRollout>,
        }

        /// Represents a state update sent by the dashboard backend
        /// via its SSE update endpoint at the /api/v2/sse path.
        ///
        /// Messages are sent as SSE events, where the name of the event
        /// is the name of the struct embedded in each variant below,
        /// and the data field is a JSON serialization of the data.
        /// E.g. a CompleteState variant would look like this on the wire:
        ///
        ///   event: State
        ///   data: {"rollouts": ..., "rollout_engine_states": ...}
        #[derive(Serialize)]
        #[serde(tag = "untagged")]
        pub enum Message {
            /// Full state sent by the dashboard backend via its SSE update endpoint.
            ///
            /// In SSE use, only sent during initial update or after a state update error,
            /// in order to fully synchronize the client with the state of the world.
            CompleteState(State),
            /// Error state sent by the dashboard backend via its
            /// SSE update endpoint.
            ///
            /// Clients should invalidate their view of the world
            /// when an error is received.
            Error(Error),
            /// Incremental rollout info update sent by the dashboard backend via its SSE update endpoint.  Updated rollouts must
            /// be replaced in clients' local state, while deleted rollouts
            /// must be deleted fromm clients' local state.
            /// Only sent after a full state update.  Clients are meant to use this
            /// to synchronize their internal state to the state of the world that the
            /// servers see.
            RolloutsDelta(RolloutsDelta),
            /// Engine info update sent by the dashboard backend via its SSE update endpoint.
            /// Upon receipt, clients must replace their own local
            /// copy of the states of all engines with the update sent
            /// here.
            ///
            /// Only sent after a full state update.  Clients are meant to use this
            /// to synchronize their internal state to the state of the world that the
            /// servers see.
            RolloutEngineStatesUpdate(RolloutEngineStates),
        }
    }
}

pub mod unstable {
    use crate::airflow_client::TaskInstancesResponseItem;
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Serialize};

    #[derive(Serialize)]
    pub struct FlowCacheResponse {
        pub dag_id: String,
        pub rollout_id: String,
        pub dispatch_time: DateTime<Utc>,
        pub last_update_time: Option<DateTime<Utc>>,
        pub update_count: usize,
        pub linearized_task_instances: Vec<TaskInstancesResponseItem>,
    }

    use strum::EnumString;

    #[derive(Debug, Deserialize, Serialize, Clone, EnumString)]
    #[strum(serialize_all = "PascalCase")]
    enum NodeStatus {
        Healthy,
        Dead,
        Degraded,
    }

    #[derive(Debug, Deserialize, Serialize, Clone)]
    pub struct NodeInfo {
        pub node_id: String,
        pub node_provider_id: String,
        pub subnet_id: Option<String>,
        pub dc_id: String,
        pub status: String,
    }

    #[derive(Debug, Serialize, Clone)]
    pub struct ProvisionalPlanBatch {
        pub nodes: Vec<NodeInfo>,
        // selectors: Selectors,
        pub start_at: DateTime<Utc>,
    }

    #[derive(Debug, Serialize)]
    pub struct HostOsRolloutBatchStateResponse {
        pub provisional_plan: ProvisionalPlanBatch,
        pub actual_plan: Option<Vec<NodeInfo>>,
    }
}
