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
            super::v2::Rollout::RolloutIcOsToMainnetSubnets(
                super::v2::RolloutIcOsToMainnetSubnets {
                    dag_info: super::v2::DAGInfo {
                        name: rollout.name,
                        display_url: rollout.display_url,
                        dispatch_time: rollout.dispatch_time,
                        last_scheduling_decision: rollout.last_scheduling_decision,
                        note: rollout.note,
                        update_count: rollout.update_count,
                    },
                    state: rollout.state,
                    batches: rollout.batches,
                    conf: rollout.conf,
                },
            )
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
    pub use super::v1::{
        Batch, RolloutState as RolloutIcOsToMainnetSubnetsState, Subnet, SubnetRolloutState,
    };
    use crate::airflow_client::{DagsResponse, DagsResponseItem};
    use chrono::{DateTime, Utc};
    use indexmap::IndexMap;
    use reqwest::StatusCode;
    use serde::Serialize;
    use serde::Serializer;
    use std::collections::VecDeque;

    #[derive(Debug, Serialize, Clone)]
    /// Represents the common fields that all rollouts carry.
    pub struct DAGInfo {
        /// Unique, enforced by Airflow, corresponds to DAG run ID.
        pub name: String,
        /// Link to the rollout screen in Airflow.
        pub display_url: String,
        /// Note set on the rollout by the operator.
        pub note: Option<String>,
        pub dispatch_time: DateTime<Utc>,
        /// Last scheduling decision.
        /// Due to the way the central rollout cache is updated, clients may not see
        /// an up-to-date value that corresponds to Airflow's last update time for
        /// the DAG run.  See documentation in function `get_rollout_data`.
        pub last_scheduling_decision: Option<DateTime<Utc>>,
        /// Associative array of `{batch ID -> Batch}` planned for the rollout.
        pub update_count: usize,
    }

    /// Represents a rollout of GuestOS to mainnet subnets.
    #[derive(Debug, Serialize, Clone)]
    pub struct RolloutIcOsToMainnetSubnets {
        #[serde(flatten)]
        pub dag_info: DAGInfo,
        pub state: RolloutIcOsToMainnetSubnetsState,
        /// Associative array of `{batch ID -> Batch}` planned for the rollout.
        pub batches: IndexMap<usize, Batch>,
        /// Configuration associated to the rollout.
        pub conf: IndexMap<String, serde_json::Value>,
    }

    impl RolloutIcOsToMainnetSubnets {
        pub fn updated(&mut self) {
            self.dag_info.update_count += 1
        }
    }

    /// Represents a generic rollout of any kind.
    #[derive(Debug, Serialize, Clone)]
    #[serde(tag = "kind")]
    #[serde(rename_all = "snake_case")]
    pub enum Rollout {
        RolloutIcOsToMainnetSubnets(RolloutIcOsToMainnetSubnets),
    }

    impl Rollout {
        pub fn name(&self) -> String {
            match self {
                Self::RolloutIcOsToMainnetSubnets(s) => s.dag_info.name.clone(),
            }
        }
        pub fn display_url(&self) -> String {
            match self {
                Self::RolloutIcOsToMainnetSubnets(s) => s.dag_info.display_url.clone(),
            }
        }
        pub fn kind(&self) -> String {
            match self {
                Self::RolloutIcOsToMainnetSubnets(_) => {
                    "rollout_ic_os_to_mainnet_subnets".to_string()
                }
            }
        }
        pub fn update_count(&self) -> usize {
            match self {
                Self::RolloutIcOsToMainnetSubnets(s) => s.dag_info.update_count,
            }
        }
        pub fn key(&self) -> String {
            self.kind() + &self.name()
        }
    }

    impl From<RolloutIcOsToMainnetSubnets> for Rollout {
        fn from(f: RolloutIcOsToMainnetSubnets) -> Self {
            Rollout::RolloutIcOsToMainnetSubnets(f)
        }
    }

    impl TryFrom<super::v2::Rollout> for super::v1::Rollout {
        type Error = &'static str;
        fn try_from(rollout: super::v2::Rollout) -> Result<super::v1::Rollout, Self::Error> {
            let (state, batches, conf, dag_info, update_count) = match &rollout {
                Rollout::RolloutIcOsToMainnetSubnets(s) => (
                    s.state.clone(),
                    s.batches.clone(),
                    s.conf.clone(),
                    s.dag_info.clone(),
                    s.dag_info.update_count,
                ),
                _ => Err("this rollout is incompatible with conversion")?,
            };

            Ok(super::v1::Rollout {
                name: rollout.name(),
                display_url: rollout.display_url(),
                update_count,
                note: dag_info.note,
                dispatch_time: dag_info.dispatch_time,
                last_scheduling_decision: dag_info.last_scheduling_decision,
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

    fn serialize_status_code<S>(e: &StatusCode, s: S) -> Result<S::Ok, S::Error>
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
    use serde::Serialize;

    #[derive(Serialize)]
    pub struct FlowCacheResponse {
        pub rollout_id: String,
        pub dispatch_time: DateTime<Utc>,
        pub last_update_time: Option<DateTime<Utc>>,
        pub update_count: usize,
        pub linearized_task_instances: Vec<TaskInstancesResponseItem>,
    }
}
