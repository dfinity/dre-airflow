//! These are the types used to deserialize inputs and results of the API calls
//! made available by the rollout dashboard REST / SSE API.
//!
//! See [crate::api_server] for a listing of API endpoints.

/// Types used by version 1 of the API.
///
/// This is deprecated and should not be used.
pub mod v1 {
    use super::super::airflow_client::{DagsResponse, DagsResponseItem};
    use chrono::{DateTime, Utc};
    use indexmap::IndexMap;
    use reqwest::StatusCode;
    use serde::Deserialize;
    use serde::Serialize;
    use std::collections::VecDeque;
    use std::str::FromStr;
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
                name: super::v2::DagRunID::from_str(rollout.name.as_str()).unwrap(),
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

/// Types used by version 2 of the API.
pub mod v2 {
    use super::super::airflow_client::{DagsResponse, DagsResponseItem};
    use chrono::{DateTime, Utc};
    use indexmap::IndexMap;
    use reqwest::StatusCode;
    use serde::Deserialize;
    use serde::Deserializer;
    use serde::Serialize;
    use serde::Serializer;
    use serde::de;
    use std::collections::VecDeque;
    use std::convert::Infallible;
    use std::fmt;
    use std::fmt::Display;
    use std::str::FromStr;
    use strum::EnumString;

    /// Serde deserialization decorator to map empty Strings to None,
    fn empty_value_as_true<'de, D, T>(de: D) -> Result<Option<T>, D::Error>
    where
        D: Deserializer<'de>,
        T: FromStr,
        T::Err: fmt::Display,
    {
        let opt = Option::<String>::deserialize(de)?;
        match opt.as_deref() {
            Some("") | None => FromStr::from_str("true")
                .map_err(de::Error::custom)
                .map(Some),
            Some(s) => FromStr::from_str(s).map_err(de::Error::custom).map(Some),
        }
    }

    #[derive(Deserialize)]
    /// Query string parameters for the SSE endpoint.
    pub struct SseHandlerParameters {
        #[serde(default, deserialize_with = "empty_value_as_true")]
        /// Should events be incremental?  Or just full events every update?
        /// Defaults to true in API v2, false in API v1.
        pub incremental: Option<bool>,
    }

    /// Types used when dealing with GuestOS rollouts.
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

    /// Types used when dealing with API boundary node rollouts.
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

    /// Types used when dealing with HostOS rollouts.
    pub mod hostos {

        use chrono::{DateTime, Utc};
        use indexmap::IndexMap;
        use reqwest::StatusCode;
        use serde::{Deserialize, Serialize, Serializer};
        use std::{collections::HashMap, fmt, num::NonZero};
        use strum::{Display, EnumString};

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
        /// Represents the rollout state of a HostOS rollout batch.
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
            /// The state a batch is when plan skips ahead (`wait_until_start_time` is skipped)
            /// or `collect_nodes` skips ahead (`create_proposal_if_none_exists` is skipped).
            Skipped,
            Unknown,
        }

        /// Represents a HostOS node being targeted by the rollout.
        #[derive(Serialize, Debug, Clone)]
        pub struct Node {
            /// This is the node ID.
            pub node_id: String,
        }

        impl From<NodeInfo> for Node {
            fn from(n: NodeInfo) -> Node {
                Node { node_id: n.node_id }
            }
        }

        #[derive(Debug, Serialize, Deserialize, Clone, EnumString, PartialEq, Eq)]
        pub enum NodeAssignment {
            #[serde(rename = "assigned")]
            Assigned,
            #[serde(rename = "unassigned")]
            Unassigned,
            #[serde(rename = "API boundary")]
            ApiBoundary,
        }

        #[derive(Debug, Serialize, Deserialize, Clone, EnumString, PartialEq, Eq)]
        pub enum NodeOwner {
            #[serde(rename = "DFINITY")]
            Dfinity,
            #[serde(rename = "others")]
            Others,
        }

        struct NaturalNumberOrRatioVisitor;

        enum NaturalNumberOrRatio {
            Absolute(usize),
            Ratio(ordered_float::OrderedFloat<f64>),
        }

        impl<'de> serde::de::Visitor<'de> for NaturalNumberOrRatioVisitor {
            type Value = NaturalNumberOrRatio;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str(
                    "either a nonzero nonnegative integer or a float between 0.0 and 1.0",
                )
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
                Ok(Self::Value::Absolute(v.try_into().unwrap()))
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Self::Value::Absolute(v.try_into().unwrap()))
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
                Ok(Self::Value::Ratio(ordered_float::OrderedFloat(v)))
            }
        }

        #[derive(Debug, Clone, PartialEq, Eq)]
        pub enum NodesPerGroup {
            Absolute(usize),
            Ratio(ordered_float::OrderedFloat<f64>),
        }

        impl From<NaturalNumberOrRatio> for NodesPerGroup {
            fn from(value: NaturalNumberOrRatio) -> Self {
                match value {
                    NaturalNumberOrRatio::Absolute(v) => Self::Absolute(v),
                    NaturalNumberOrRatio::Ratio(v) => Self::Ratio(v),
                }
            }
        }

        impl<'de> Deserialize<'de> for NodesPerGroup {
            fn deserialize<D>(deserializer: D) -> Result<NodesPerGroup, D::Error>
            where
                D: serde::de::Deserializer<'de>,
            {
                let nnor = deserializer.deserialize_any(NaturalNumberOrRatioVisitor)?;
                Ok(nnor.into())
            }
        }

        impl Serialize for NodesPerGroup {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                match self {
                    Self::Absolute(n) => serializer.serialize_u64(*n as u64),
                    Self::Ratio(n) => serializer.serialize_str(format!("{}%", n * 100.0).as_str()),
                }
            }
        }

        #[derive(Debug, Clone, PartialEq, Eq)]
        pub enum NodeFailureTolerance {
            Absolute(usize),
            Ratio(ordered_float::OrderedFloat<f64>),
        }

        impl From<NaturalNumberOrRatio> for NodeFailureTolerance {
            fn from(value: NaturalNumberOrRatio) -> Self {
                match value {
                    NaturalNumberOrRatio::Absolute(v) => Self::Absolute(v),
                    NaturalNumberOrRatio::Ratio(v) => Self::Ratio(v),
                }
            }
        }

        impl<'de> Deserialize<'de> for NodeFailureTolerance {
            fn deserialize<D>(deserializer: D) -> Result<NodeFailureTolerance, D::Error>
            where
                D: serde::de::Deserializer<'de>,
            {
                let nnor = deserializer.deserialize_any(NaturalNumberOrRatioVisitor)?;
                Ok(nnor.into())
            }
        }

        impl Serialize for NodeFailureTolerance {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                match self {
                    Self::Absolute(n) => serializer.serialize_u64(*n as u64),
                    Self::Ratio(n) => serializer.serialize_str(format!("{}%", n * 100.0).as_str()),
                }
            }
        }

        #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
        pub enum GroupBy {
            #[serde(rename = "datacenter")]
            Datacenter,
            #[serde(rename = "subnet")]
            Subnet,
        }

        #[derive(Debug, Deserialize, Serialize, Clone, EnumString, PartialEq, Eq)]
        #[strum(serialize_all = "PascalCase")]
        pub enum NodeStatus {
            Healthy,
            Degraded,
            Dead,
        }

        #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Default)]
        pub struct NodeSpecifier {
            pub assignment: Option<NodeAssignment>,
            pub owner: Option<NodeOwner>,
            pub nodes_per_group: Option<NodesPerGroup>,
            pub group_by: Option<GroupBy>,
            pub status: Option<NodeStatus>,
            pub datacenter: Option<String>,
            /// Filter nodes to only those in subnets with healthy node count exceeding
            pub subnet_healthy_threshold: Option<NodesPerGroup>,
        }

        #[cfg(test)]
        impl NodeSpecifier {
            fn unassigned(self) -> Self {
                Self {
                    assignment: Some(NodeAssignment::Unassigned),
                    ..self
                }
            }

            fn dfinity(self) -> Self {
                Self {
                    owner: Some(NodeOwner::Dfinity),
                    ..self
                }
            }

            fn number(self, n: usize) -> Self {
                Self {
                    nodes_per_group: Some(NodesPerGroup::Absolute(n)),
                    ..self
                }
            }

            fn healthy(self) -> Self {
                Self {
                    status: Some(NodeStatus::Healthy),
                    ..self
                }
            }

            fn datacenter(self, dc_id: &str) -> Self {
                Self {
                    datacenter: Some(dc_id.to_string()),
                    ..self
                }
            }
        }

        #[derive(Deserialize, Debug)]
        #[serde(untagged)]
        pub enum NodeSelectorsOrListOfNodeSelectors {
            NodeFilter { intersect: Vec<NodeSelectors> },
            NodeAggregator { join: Vec<NodeSelectors> },
            NodeComplement { not: Box<NodeSelectors> },
            NodeSpecifier(NodeSpecifier),
            ListOfSelectors(Vec<NodeSpecifier>),
        }

        impl From<NodeSelectorsOrListOfNodeSelectors> for NodeSelectors {
            fn from(s: NodeSelectorsOrListOfNodeSelectors) -> NodeSelectors {
                match s {
                    NodeSelectorsOrListOfNodeSelectors::NodeComplement { not: m } => {
                        NodeSelectors::NodeComplement { not: m }
                    }
                    NodeSelectorsOrListOfNodeSelectors::ListOfSelectors(m) => {
                        NodeSelectors::NodeFilter {
                            intersect: m.into_iter().map(NodeSelectors::NodeSpecifier).collect(),
                        }
                    }
                    NodeSelectorsOrListOfNodeSelectors::NodeAggregator { join: m } => {
                        NodeSelectors::NodeAggregator { join: m }
                    }
                    NodeSelectorsOrListOfNodeSelectors::NodeFilter { intersect: m } => {
                        NodeSelectors::NodeFilter { intersect: m }
                    }
                    NodeSelectorsOrListOfNodeSelectors::NodeSpecifier(m) => {
                        NodeSelectors::NodeSpecifier(m)
                    }
                }
            }
        }

        #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
        #[serde(untagged)]
        #[serde(from = "NodeSelectorsOrListOfNodeSelectors")]
        pub enum NodeSelectors {
            NodeFilter { intersect: Vec<NodeSelectors> },
            NodeAggregator { join: Vec<NodeSelectors> },
            NodeComplement { not: Box<NodeSelectors> },
            NodeSpecifier(NodeSpecifier),
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
            /// Current state of this batch.
            pub state: BatchState,
            /// Shows a comment for the batch if it is available; else it contains an empty string.
            pub comment: String,
            /// Links to the specific task within Airflow that this batch is currently performing; else it contains an empty string.
            pub display_url: String,
            /// A count of the nodes planned to be upgraded as part of this batch.
            pub planned_nodes: usize,
            /// A list of selectors used to select which nodes to target for upgrade.
            /// This is None if the selectors are not yet known.
            pub selectors: Option<NodeSelectors>,
            /// A count of the nodes that actually were or are upgraded as part of this batch.
            /// Usually updated after collect_nodes has executed and has obtained a list of nodes.
            /// If that phase of the batch has yet to take place, this is usually null.
            pub actual_nodes: Option<usize>,
        }

        impl From<&BatchResponse> for Batch {
            fn from(other: &BatchResponse) -> Batch {
                Batch {
                    planned_start_time: other.planned_start_time,
                    actual_start_time: other.actual_start_time,
                    end_time: other.end_time,
                    state: other.state.clone(),
                    comment: other.comment.clone(),
                    display_url: other.display_url.clone(),
                    planned_nodes: other.planned_nodes.len(),
                    selectors: other.selectors.clone(),
                    actual_nodes: other.actual_nodes.clone().map(|ns| ns.len()),
                }
            }
        }

        ///  Represents a particular stage in the HostOS rollout.
        #[derive(Debug, Serialize, Clone)]
        pub struct Stages {
            pub canary: IndexMap<NonZero<usize>, Batch>,
            pub main: IndexMap<NonZero<usize>, Batch>,
            pub unassigned: IndexMap<NonZero<usize>, Batch>,
            pub stragglers: IndexMap<NonZero<usize>, Batch>,
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

        /// Represents the full information for a HostOS node.
        #[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
        pub struct NodeInfo {
            pub node_id: String,
            pub node_provider_id: String,
            pub assignment: Option<String>, // either a subnet ID or "API boundary"
            pub dc_id: String,
            pub status: String,
        }

        /// List of nodes actually targeted by a running batch.
        pub type ActuallyTargetedNodes = Vec<NodeInfo>;

        /// Stage of the HostOS rollout.
        #[derive(Clone, PartialEq, Hash, Eq, EnumString, Serialize, Debug, Display)]
        #[strum(serialize_all = "lowercase")]
        pub enum StageName {
            #[serde(rename = "canary")]
            /// Serialized as `canary`.
            Canary,
            #[serde(rename = "main")]
            /// Serialized as `main`.
            Main,
            #[serde(rename = "unassigned")]
            Unassigned,
            /// Serialized as `unassigned`.
            #[serde(rename = "stragglers")]
            /// Serialized as `stragglers`.
            Stragglers,
        }

        /// Upgrade status of a node.
        #[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
        pub enum NodeUpgradeStatus {
            AWOL,
            #[serde(rename = "pending")]
            Pending,
            #[serde(rename = "upgraded")]
            Upgraded,
        }

        /// Upgrade statuses of a list of nodes.
        pub type NodeUpgradeStatuses = HashMap<String, NodeUpgradeStatus>;

        /// Alert status of a node.
        #[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
        pub enum NodeAlertStatus {
            #[serde(rename = "unknown")]
            Unknown,
            #[serde(rename = "OK")]
            OK,
            #[serde(rename = "alerting")]
            Alerting,
        }

        /// Alert statuses of a list of nodes.
        pub type NodeAlertStatuses = HashMap<String, NodeAlertStatus>;

        #[derive(Serialize, Debug, Clone, PartialEq, Eq)]
        /// Represents a batch of subnets to upgrade, its state, and other
        /// important information (either known or pending) at its current
        /// state.
        pub struct BatchResponse {
            /// The name of the HostOS rollout stage.
            pub stage: StageName,
            pub batch_number: NonZero<usize>,
            /// The time the batch was programmed to start at.
            pub planned_start_time: DateTime<Utc>,
            /// The actual observed start time of the batch.
            pub actual_start_time: Option<DateTime<Utc>>,
            /// The time of the last action associated with this batch, if any.
            pub end_time: Option<DateTime<Utc>>,
            /// Current state of this batch.
            pub state: BatchState,
            /// Shows a comment for the batch if it is available; else it contains an empty string.
            pub comment: String,
            /// Links to the specific task within Airflow that this batch is currently performing; else it contains an empty string.
            pub display_url: String,
            /// A list of selectors used to select which nodes to target for upgrade.
            /// This is None if the selectors are not yet known.
            pub selectors: Option<NodeSelectors>,
            /// An optional tolerance for nodes or percentage that can fail in this batch.
            /// This may not be specified at rollout time, in which case the rollout
            /// assumes zero tolerance for failures in the batch.
            pub tolerance: Option<NodeFailureTolerance>,
            /// A count of the nodes planned to be upgraded as part of this batch.
            pub planned_nodes: Vec<NodeInfo>,
            /// A count of the nodes that actually were or are upgraded as part of this batch.
            /// Usually updated after collect_nodes has executed and has obtained a list of nodes.
            /// If that phase of the batch has yet to take place, this is usually null.
            pub actual_nodes: Option<Vec<NodeInfo>>,
            /// A dictionary mapping node ID to node upgrade status.  This might be empty
            /// if the upgrade phase has not yet been reached, or for very old rollouts
            /// that did not supply this information.
            pub upgraded_nodes: Option<NodeUpgradeStatuses>,
            /// A dictionary mapping node ID to alert status.  This might be empty if the
            /// waiting for alerts to subside phase has not yet been reached, or for very
            /// old rollouts that do not have this information.
            pub alerting_nodes: Option<NodeAlertStatuses>,
            /// If true, the checks for upgraded nodes were manually bypassed by the administrator.  None indicates unknown.
            pub adoption_checks_bypassed: Option<bool>,
            /// If true, the checks for alerting nodes were manually bypassed by the administrator.  None indicates unknown.
            pub health_checks_bypassed: Option<bool>,
        }

        impl From<&BatchResponse> for axum::response::sse::Event {
            fn from(m: &BatchResponse) -> Self {
                axum::response::sse::Event::default()
                    .event("BatchResponse")
                    .json_data(m)
                    .unwrap()
            }
        }

        #[derive(Clone, Debug, PartialEq, Eq)]
        /// Error response for batch detail requests.
        pub enum BatchError {
            /// The dashboard has not yet completely synced data from Airflow.
            NotYetSynced,
            /// The rollout in question does not yet have a plan.  A plan may
            /// be computed in the future.
            NoPlanDataYet,
            /// The batch has not yet appeared in the rollout.  It may not appear
            /// ever.
            NoBatchDataYet,
            /// The supplied batch number is invalid.
            InvalidBatchNumber,
            /// The supplied stage name ([StageName]) is invalid.
            InvalidStageName,
            /// The supplied DAG ID ([super::RolloutKind]) is invalid.
            InvalidDagID,
            /// The supplied DAG ID is not a HostOS rollout.
            WrongDagRunKind,
            /// The supplied DAG run ID (rollout name, [super::DagID]) is invalid.
            InvalidDagRunID,
            /// No HostOS rollout with that name.
            NoSuchDagRun,
            /// The rollout never computed a plan.
            NoPlanData,
            /// The requested batch definitely does not exist.
            NoSuchBatch,
            /// The dashboard failed to synchronize data from Airflow.
            RolloutDataGatherError(String),
        }

        impl From<&BatchError> for String {
            fn from(h: &BatchError) -> String {
                type T = BatchError;
                match h {
                    T::NotYetSynced => "Backend has not yet fetched rollouts".to_string(),
                    T::NoPlanDataYet => "Rollout has not yet computed a plan".to_string(),
                    T::NoBatchDataYet => {
                        "Requested batch is not yet in rollout but may appear later".to_string()
                    }
                    T::InvalidDagID => "Invalid DAG ID".to_string(),
                    T::WrongDagRunKind => "DAG run refers to the wrong kind of rollout".to_string(),
                    T::InvalidDagRunID => "Invalid DAG run ID".to_string(),
                    T::InvalidStageName => "Invalid stage name".to_string(),
                    T::InvalidBatchNumber => "Invalid batch number".to_string(),
                    T::NoSuchDagRun => {
                        "The requested DAG run does not correspond to any rollout".to_string()
                    }
                    T::NoPlanData => "Rollout never computed a plan".to_string(),
                    T::NoSuchBatch => {
                        "The requested batch number does not correspond to any batch in the rollout"
                            .to_string()
                    }
                    T::RolloutDataGatherError(e) => e.to_string(),
                }
            }
        }

        impl From<&BatchError> for StatusCode {
            fn from(h: &BatchError) -> StatusCode {
                type T = BatchError;
                type C = StatusCode;
                match h {
                    T::NotYetSynced => C::NO_CONTENT,
                    T::NoPlanDataYet => reqwest::StatusCode::from_u16(209).unwrap(),
                    T::NoBatchDataYet => reqwest::StatusCode::from_u16(209).unwrap(),
                    T::InvalidDagID => C::BAD_REQUEST,
                    T::WrongDagRunKind => C::BAD_REQUEST,
                    T::InvalidDagRunID => C::BAD_REQUEST,
                    T::InvalidStageName => C::BAD_REQUEST,
                    T::InvalidBatchNumber => C::BAD_REQUEST,
                    T::NoSuchDagRun => C::NOT_FOUND,
                    T::NoPlanData => C::NOT_FOUND,
                    T::NoSuchBatch => C::NOT_FOUND,
                    T::RolloutDataGatherError(_) => C::INTERNAL_SERVER_ERROR,
                }
            }
        }

        impl BatchError {
            pub fn permanent(&self) -> bool {
                !(reqwest::StatusCode::from(self).as_u16() < 400
                    || matches!(self, BatchError::RolloutDataGatherError(_)))
            }
        }

        impl From<&BatchError> for (StatusCode, String) {
            fn from(h: &BatchError) -> Self {
                (h.into(), h.into())
            }
        }

        impl From<&BatchError> for super::Error {
            fn from(h: &BatchError) -> Self {
                (h.into(), h.permanent()).into()
            }
        }

        impl From<&BatchError> for axum::response::sse::Event {
            fn from(m: &BatchError) -> Self {
                axum::response::sse::Event::default()
                    .event("Error")
                    .json_data(super::Error::from(m))
                    .unwrap()
            }
        }

        #[cfg(test)]
        mod tests {
            use super::{NodeSelectors, NodeSpecifier};

            #[test]
            fn test_deserialization_of_simple_selectors() {
                let inp = r#"{"assignment": "unassigned",
                                             "nodes_per_group": 100,
                                             "status": "Healthy"}"#;
                let res: NodeSelectors = serde_json::from_str(inp).unwrap();
                let exp = NodeSelectors::NodeSpecifier(
                    NodeSpecifier::default().unassigned().number(100).healthy(),
                );
                assert_eq!(res, exp)
            }

            #[test]
            fn test_deserialization_of_list_of_selectors() {
                let inp = r#"[{"assignment": "unassigned",
                                              "nodes_per_group": 1,
                                              "owner": "DFINITY",
                                              "status": "Healthy"}]"#;
                let res: NodeSelectors = serde_json::from_str(inp).unwrap();
                let exp = NodeSelectors::NodeFilter {
                    intersect: vec![NodeSelectors::NodeSpecifier(
                        NodeSpecifier::default()
                            .unassigned()
                            .number(1)
                            .healthy()
                            .dfinity(),
                    )],
                };
                assert_eq!(res, exp)
            }

            #[test]
            fn test_serdeserialization_of_complement_selector() {
                let inp = r#"{"not": {"assignment": "unassigned",
                                              "nodes_per_group": 1,
                                              "datacenter": "hk4",
                                              "status": "Healthy"}}"#;
                let res: NodeSelectors = serde_json::from_str(inp).unwrap();
                let exp = NodeSelectors::NodeComplement {
                    not: Box::new(NodeSelectors::NodeSpecifier(
                        NodeSpecifier::default()
                            .unassigned()
                            .number(1)
                            .healthy()
                            .datacenter("hk4"),
                    )),
                };
                assert_eq!(res, exp);
                let resstr = serde_json::to_string(&exp).unwrap();
                let expstr = "{\"not\":{\"assignment\":\"unassigned\",\"owner\":null,\"nodes_per_group\":1,\"group_by\":null,\"status\":\"Healthy\",\"datacenter\":\"hk4\",\"subnet_healthy_threshold\":null}}";
                assert_eq!(expstr, &resstr)
            }

            #[test]
            fn test_serdeserialization_of_intersect_selector() {
                let inp = r#"{"intersect": [{"owner": "DFINITY"}]}"#;
                let res: NodeSelectors = serde_json::from_str(inp).unwrap();
                let exp = NodeSelectors::NodeFilter {
                    intersect: vec![NodeSelectors::NodeSpecifier(
                        NodeSpecifier::default().dfinity(),
                    )],
                };
                assert_eq!(res, exp);
            }

            #[test]
            fn test_serdeserialization_of_join_selector() {
                let inp = r#"{"join": [{"owner": "DFINITY"}]}"#;
                let res: NodeSelectors = serde_json::from_str(inp).unwrap();
                let exp = NodeSelectors::NodeAggregator {
                    join: vec![NodeSelectors::NodeSpecifier(
                        NodeSpecifier::default().dfinity(),
                    )],
                };
                assert_eq!(res, exp);
            }

            #[test]
            fn test_serdeserialization_of_empty_join_selector() {
                let inp = r#"{"join": []}"#;
                let res: NodeSelectors = serde_json::from_str(inp).unwrap();
                let exp = NodeSelectors::NodeAggregator { join: vec![] };
                assert_eq!(res, exp);
            }

            #[test]
            fn test_serdeserialization_of_empty_intersect_selector() {
                let inp = r#"{"intersect": []}"#;
                let res: NodeSelectors = serde_json::from_str(inp).unwrap();
                let exp = NodeSelectors::NodeFilter { intersect: vec![] };
                assert_eq!(res, exp);
            }

            #[test]
            fn test_serdeserialization_of_intersect_complement_selector() {
                let inp = r#"{"intersect": [{"not": {"assignment": "unassigned",
                                              "nodes_per_group": 1,
                                              "datacenter": "hk4",
                                              "status": "Healthy"}}, {"owner": "DFINITY"}]}"#;
                let res: NodeSelectors = serde_json::from_str(inp).unwrap();
                let exp = NodeSelectors::NodeFilter {
                    intersect: vec![
                        NodeSelectors::NodeComplement {
                            not: Box::new(NodeSelectors::NodeSpecifier(
                                NodeSpecifier::default()
                                    .unassigned()
                                    .number(1)
                                    .healthy()
                                    .datacenter("hk4"),
                            )),
                        },
                        NodeSelectors::NodeSpecifier(NodeSpecifier::default().dfinity()),
                    ],
                };
                assert_eq!(res, exp);
                //let resstr = serde_json::to_string(&exp).unwrap();
                //let expstr = "{\"not\":{\"assignment\":\"unassigned\",\"owner\":null,\"nodes_per_group\":1,\"group_by\":null,\"status\":\"Healthy\",\"datacenter\":\"hk4\"}}";
                //assert_eq!(expstr, &resstr)
            }

            // #[test]
            // #[should_panic]
            // fn test_deserialization_of_empty_specifier() {
            //     let inp = r#"{}"#;
            //     let _: NodeSelectors = serde_json::from_str(inp).unwrap();
            // }

            #[test]
            fn test_deserialization_of_empty_list_of_specifiers() {
                let inp = r#"[]"#;
                let res: NodeSelectors = serde_json::from_str(inp).unwrap();
                let exp = NodeSelectors::NodeFilter { intersect: vec![] };
                assert_eq!(res, exp)
            }
        }
    }

    /// A kind of rollout (e.g. rollout to API boundary nodes).
    ///
    /// Used in the [Rollout] type to add information specific
    /// to the kind of rollout it contains.  Adds a tag `kind`
    /// to the [Rollout] with this type's name serialized as
    /// indicated by the variant.
    #[derive(Debug, Serialize, Clone)]
    #[serde(tag = "kind")]
    #[serde(rename_all = "snake_case")]
    #[allow(clippy::large_enum_variant)]
    pub enum RolloutKind {
        /// GuestOS rollout. Serialized as `rollout_ic_os_to_mainnet_subnets`.
        RolloutIcOsToMainnetSubnets(guestos::Rollout),
        /// API boundary node rollout. Serialized as `rollout_ic_os_to_mainnet_api_boundary_nodes`.
        RolloutIcOsToMainnetApiBoundaryNodes(api_boundary_nodes::Rollout),
        /// HostOS rollout. Serialized as `rollout_ic_os_to_mainnet_nodes`.
        RolloutIcOsToMainnetNodes(hostos::Rollout),
    }

    /// Rollout kind, available as an identifier.
    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, EnumString)]
    #[strum(serialize_all = "snake_case")]
    pub enum DagID {
        /// GuestOS rollout. Serialized to `rollout_ic_os_to_mainnet_subnets`.
        RolloutIcOsToMainnetSubnets,
        /// API boundary node rollout. Serialized as `rollout_ic_os_to_mainnet_api_boundary_nodes`.
        RolloutIcOsToMainnetApiBoundaryNodes,
        /// HostOS rollout. Serialized as `rollout_ic_os_to_mainnet_nodes`.
        RolloutIcOsToMainnetNodes,
    }

    impl Display for DagID {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(
                f,
                "{}",
                match self {
                    Self::RolloutIcOsToMainnetSubnets => "rollout_ic_os_to_mainnet_subnets",
                    Self::RolloutIcOsToMainnetApiBoundaryNodes =>
                        "rollout_ic_os_to_mainnet_api_boundary_nodes",
                    Self::RolloutIcOsToMainnetNodes => "rollout_ic_os_to_mainnet_nodes",
                }
            )
        }
    }

    /// Name of a rollout.
    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
    pub struct DagRunID(String);

    impl Display for DagRunID {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl FromStr for DagRunID {
        type Err = Infallible;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            Ok(Self(s.to_string()))
        }
    }

    /// Represents a generic rollout of any kind.
    #[derive(Debug, Serialize, Clone)]
    pub struct Rollout {
        /// Unique, enforced by Airflow, corresponds to DAG run ID.
        pub name: DagRunID,
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
        /// Data about the specific type of rollout this is.  Used as `kind` tag when
        /// serializing the specific rollout in question.
        pub kind: RolloutKind,
    }

    impl Rollout {
        pub fn kind(&self) -> DagID {
            match self.kind {
                RolloutKind::RolloutIcOsToMainnetSubnets(_) => {
                    DagID::from_str("rollout_ic_os_to_mainnet_subnets").unwrap()
                }
                RolloutKind::RolloutIcOsToMainnetApiBoundaryNodes(_) => {
                    DagID::from_str("rollout_ic_os_to_mainnet_api_boundary_nodes").unwrap()
                }
                RolloutKind::RolloutIcOsToMainnetNodes(_) => {
                    DagID::from_str("rollout_ic_os_to_mainnet_nodes").unwrap()
                }
            }
        }
        pub fn key(&self) -> (DagID, DagRunID) {
            (self.kind(), self.name.clone())
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
                name: rollout.name.to_string(),
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
        /// The kind of rollout that was deleted.
        ///
        /// Internally this is a [DagID].
        pub kind: String,
        /// The name of the rollout that was deleted.
        ///
        /// Internally this is a [DagRunID].
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

    /// Represents one or more engine update states.
    ///
    /// When sent in a
    /// full status update, it contains an exhaustive map of rollout
    /// kind (see [RolloutKind]) and its corresponding [RolloutEngineState].
    ///
    /// In the case of incremental updates (usually sent by SSE endpoints)
    /// it contains only the rollout engine states that changed since the
    /// last incremental update.  The key of the dictionary is a [RolloutKind],
    /// and the value is its [RolloutEngineState].
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

    /// List of rollout information.
    pub type Rollouts = VecDeque<Rollout>;

    /// Represents the current state of the rollouts and the rollout engines.
    #[derive(Serialize, Clone, Default)]
    pub struct State {
        pub rollouts: Rollouts,
        pub rollout_engine_states: RolloutEngineStates,
    }

    #[doc(hidden)]
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
        // FIXME does not need to be struct, or code and message should be separate instead of a tuple.
        #[serde(serialize_with = "serialize_status_code")]
        pub code: StatusCode,
        pub message: String,
        // In SSE connections, if the client receives this message, and this field is present and true,
        // the server will disconnect the client after the message, and the client should not attempt
        // to reconnect or re-request the data, as the result will be the same.  If this field is
        // present but false, the server will likely send more updates over the SSE connection.
        pub permanent: bool,
    }

    impl From<((StatusCode, String), bool)> for Error {
        fn from(((code, message), permanent): ((StatusCode, String), bool)) -> Self {
            Self {
                code,
                message,
                permanent,
            }
        }
    }

    impl From<&Error> for (StatusCode, String) {
        fn from(e: &Error) -> (StatusCode, String) {
            (e.code, e.message.clone())
        }
    }

    /// Types used by server-side events endpoints.
    pub mod sse {
        use super::{DeletedRollout, Error, Rollout, RolloutEngineStates, State};
        use serde::Serialize;
        use std::collections::VecDeque;

        /// Represents changes from a prior rollouts state update.
        #[derive(Serialize)]
        pub struct RolloutsDelta {
            /// Updated rollouts.
            ///
            /// Client should use its existing knowledge and this data
            /// to produce an updated view of the rollouts.
            pub updated: VecDeque<Rollout>,
            /// Rollouts that were deleted from the state.
            ///
            /// Client should
            /// use this information to delete rollouts from its state.
            pub deleted: VecDeque<DeletedRollout>,
        }

        /// Streamed state update sent by the dashboard backend
        /// via its SSE update endpoint designated for rollouts.
        ///
        /// Messages are sent as SSE events, where the name of the event
        /// is the name of the struct embedded in each variant below,
        /// and the data field is a JSON serialization of the data.
        /// E.g. a [Message::CompleteState] variant would look like this
        /// on the wire:
        ///
        /// ```yaml
        /// event: State
        /// data: {"rollouts": ..., "rollout_engine_states": ...}
        /// ```
        #[derive(Serialize)]
        #[serde(tag = "untagged")]
        pub enum Message {
            /// Full state sent by the dashboard backend via its SSE update endpoint.
            ///
            /// In SSE use, only sent during initial update or after a state update error,
            /// in order to fully synchronize the client with the state of the world.
            ///
            /// Event name: `State`.
            CompleteState(State),
            /// Error state sent by the dashboard backend via its
            /// SSE update endpoint.
            ///
            /// Clients should invalidate their view of the world
            /// when an error is received.
            ///
            /// Event name: `Error`.
            Error(Error),
            /// Incremental rollout info update sent by the dashboard backend via its
            /// SSE update endpoint.  Updated rollouts must
            /// be replaced in clients' local state, while deleted rollouts
            /// must be deleted fromm clients' local state.
            /// Only sent after a full state update and if the caller requested incremental
            /// updates (false by default for API v1 and true by default for API v2).
            /// Clients are meant to use this
            /// to synchronize their internal state to the state of the world that the
            /// servers see.
            ///
            /// Event Name: `RolloutsDelta`.
            RolloutsDelta(RolloutsDelta),
            /// Engine info update sent by the dashboard backend via its SSE update endpoint.
            /// Upon receipt, clients must replace their own local
            /// copy of the states of each engine with the update for that engine sent
            /// here.
            ///
            /// Only sent after a full state update.  Clients are meant to use this
            /// to synchronize their internal state to the state of the world that the
            /// servers see.
            /// Event Name: `RolloutEngineStates`.
            RolloutEngineStatesUpdate(RolloutEngineStates),
        }

        impl From<&Message> for (bool, axum::response::sse::Event) {
            /// Transforms a Message into a pair (bool, SSE event).
            /// If the boolean is true, the caller running the SSE stream must interrupt
            /// the connection after sending the event.
            fn from(m: &Message) -> (bool, axum::response::sse::Event) {
                match m {
                    Message::CompleteState(sok) => (
                        false,
                        axum::response::sse::Event::default()
                            .event("State")
                            .json_data(sok)
                            .unwrap(),
                    ),
                    Message::Error(serr) => (
                        serr.permanent,
                        axum::response::sse::Event::default()
                            .event("Error")
                            .json_data(serr)
                            .unwrap(),
                    ),
                    Message::RolloutsDelta(sdelta) => (
                        false,
                        axum::response::sse::Event::default()
                            .event("RolloutsDelta")
                            .json_data(sdelta)
                            .unwrap(),
                    ),
                    Message::RolloutEngineStatesUpdate(sdelta) => (
                        false,
                        axum::response::sse::Event::default()
                            .event("RolloutEngineStates")
                            .json_data(sdelta)
                            .unwrap(),
                    ),
                }
            }
        }

        #[derive(PartialEq, Eq)]
        /// Streamed HostOS batch detail result.
        pub struct HostOsBatchResult(
            pub Result<super::hostos::BatchResponse, super::hostos::BatchError>,
        );

        #[allow(clippy::from_over_into)]
        impl Into<HostOsBatchResult> for Result<super::hostos::BatchResponse, super::hostos::BatchError> {
            fn into(self) -> HostOsBatchResult {
                HostOsBatchResult(self)
            }
        }

        impl From<&HostOsBatchResult> for (bool, axum::response::sse::Event) {
            /// Transforms a Message into a pair (bool, SSE event).
            /// If the boolean is true, the caller running the SSE stream must interrupt
            /// the connection after sending the event.
            fn from(m: &HostOsBatchResult) -> (bool, axum::response::sse::Event) {
                match &m.0 {
                    Ok(sok) => (false, sok.into()),
                    Err(e) => (e.permanent(), e.into()),
                }
            }
        }
    }
}

/// Types used by API calls not yet stabilized.
///
/// This is not exposed in production and should not be relied upon.
pub mod unstable {
    pub use super::super::airflow_client::DagRunsResponseItem;
    pub use super::super::airflow_client::TaskInstancesResponseItem;
    pub use super::super::airflow_client::XComEntryResponse;
    use chrono::{DateTime, Utc};
    use serde::Serialize;

    #[derive(Serialize)]
    pub struct FlowCacheResponse {
        pub dag_id: super::v2::DagID,
        pub rollout_id: super::v2::DagRunID,
        pub dispatch_time: DateTime<Utc>,
        pub last_update_time: Option<DateTime<Utc>>,
        pub update_count: usize,
        pub linearized_task_instances: Vec<TaskInstancesResponseItem>,
    }
}
