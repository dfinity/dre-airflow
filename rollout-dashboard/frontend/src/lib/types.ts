// Generic rollout variant and helpers / support structures.
export type DAGInfo = {
    name: String;
    display_url: string;
    note?: String;
    dispatch_time: Date;
    last_scheduling_decision?: Date;
};

// Start GuestOS rollout to mainnet subnet types.

// types::v2::guestos::SubnetState
const GuestOsSubnetState = {
    pending: { icon: "🕐", name: "pending" },
    waiting: { icon: "⌛", name: "waiting" },
    proposing: { icon: "📝", name: "proposing update to new revision" },
    waiting_for_election: {
        icon: "🗳️",
        name: "waiting for revision election",
    },
    waiting_for_adoption: {
        icon: "⚡",
        name: "waiting for revision adoption",
    },
    waiting_for_alerts_gone: {
        icon: "📢",
        name: "waiting until no more alerts",
    },
    complete: { icon: "✅", name: "complete" },
    skipped: { icon: "⏩", name: "skipped" },
    error: { icon: "❌", name: "error" },
    predecessor_failed: { icon: "❌", name: "predecessor failed" },
    unknown: { icon: "❓", name: "does not appear in Airflow" },
};
export function subnetStateIcon(subnet: GuestOsSubnet): String {
    return GuestOsSubnetState[subnet.state].icon;
};
export function subnetStateComment(subnet: GuestOsSubnet): string {
    let s = GuestOsSubnetState[subnet.state].name;
    if (subnet.comment) {
        s = s + " • " + subnet.comment
    }
    return s
};

// types::v2::guestos::Subnet
export type GuestOsSubnet = {
    subnet_id: string;
    git_revision: string;
    state: keyof typeof GuestOsSubnetState;
    comment: String;
    display_url: string;
};

// types::v2::guestos::Batch
export type GuestOsBatch = {
    planned_start_time: Date;
    actual_start_time: Date | null;
    end_time: Date | null;
    subnets: GuestOsSubnet[];
};

// types::v2::guestos::State
const GuestOsState = {
    complete: { icon: "🏁", name: "complete" },
    failed: { icon: "❌", name: "failed" },
    preparing: { icon: "🔁", name: "preparing" },
    upgrading_subnets: { icon: "▶️", name: "upgrading subnets" },
    upgrading_unassigned_nodes: {
        icon: "⏩",
        name: "upgrading unassigned nodes",
    },
    waiting: { icon: "⌛", name: "waiting" },
    problem: { icon: "⚠️", name: "problem" },
};
export function guestOsStateIcon(rollout: GuestOsRollout): String {
    return GuestOsState[rollout.state].icon;
};
export function GuestOsStateName(rollout: GuestOsRollout): string {
    return GuestOsState[rollout.state].name;
};

export type GuestOsRolloutConfiguration = {
    simulate: boolean;
};

export type GuestOSBatches = {
    String: GuestOsBatch
};

// types::v2::guestos::Rollout
export type GuestOsRollout = {
    kind: "rollout_ic_os_to_mainnet_subnets";
    conf: GuestOsRolloutConfiguration;
    state: keyof typeof GuestOsState;
    batches: GuestOSBatches;
} & DAGInfo;

// End GuestOS rollout to mainnet subnet types.

// Start GuestOS rollout to API boundary nodes types.

// types::v2::api_boundary_nodes::State
const ApiBoundaryNodesState = {
    complete: { icon: "🏁", name: "complete" },
    failed: { icon: "❌", name: "failed" },
    preparing: { icon: "🔁", name: "preparing" },
    upgrading_api_boundary_nodes: { icon: "▶️", name: "upgrading API boundary nodes" },
    waiting: { icon: "⌛", name: "waiting" },
    problem: { icon: "⚠️", name: "problem" },
};
export function apiBoundaryNodesStateIcon(rollout: ApiBoundaryNodesRollout): String {
    return ApiBoundaryNodesState[rollout.state].icon;
};
export function apiBoundaryNodesStateName(rollout: ApiBoundaryNodesRollout): string {
    return ApiBoundaryNodesState[rollout.state].name;
};

// types::v2::api_boundary_nodes::BatchState
const ApiBoundaryNodesBatchState = {
    pending: { icon: "🕐", name: "pending" },
    waiting: { icon: "⌛", name: "waiting" },
    proposing: { icon: "📝", name: "proposing update to new revision" },
    waiting_for_election: {
        icon: "🗳️",
        name: "waiting for revision election",
    },
    waiting_for_adoption: {
        icon: "⚡",
        name: "waiting for revision adoption",
    },
    waiting_until_nodes_healthy: {
        icon: "📢",
        name: "waiting until all upgraded API boundary nodes are healthy",
    },
    complete: { icon: "✅", name: "complete" },
    skipped: { icon: "⏩", name: "skipped" },
    error: { icon: "❌", name: "error" },
    predecessor_failed: { icon: "❌", name: "predecessor failed" },
    unknown: { icon: "❓", name: "does not appear in Airflow" },
};
export function apiBoundaryNodesBatchStateIcon(batch: ApiBoundaryNodesBatch): String {
    return ApiBoundaryNodesBatchState[batch.state].icon;
}
export function apiBoundaryNodesBatchStateComment(subnet: ApiBoundaryNodesBatch): string {
    let s = ApiBoundaryNodesBatchState[subnet.state].name;
    if (subnet.comment) {
        s = s + " • " + subnet.comment
    }
    return s
}

// types::v2::api_boundary_nodes::Node
export type ApiBoundaryNodesNode = {
    node_id: string;
};

// types::v2::api_boundary_nodes::Batch
export type ApiBoundaryNodesBatch = {
    planned_start_time: Date;
    actual_start_time: Date | null;
    end_time: Date | null;
    state: keyof typeof ApiBoundaryNodesBatchState;
    comment: String;
    display_url: string;
    api_boundary_nodes: ApiBoundaryNodesNode[];
};


export type ApiBoundaryNodesRolloutConfiguration = {
    simulate: boolean;
    git_revision: String;
};

export type ApiBoundaryNodesBatches = {
    String: ApiBoundaryNodesBatch
};

// types::v2::api_boundary_nodes::Rollout
export type ApiBoundaryNodesRollout = {
    kind: "rollout_ic_os_to_mainnet_api_boundary_nodes";
    conf: ApiBoundaryNodesRolloutConfiguration;
    state: keyof typeof ApiBoundaryNodesState;
    batches: ApiBoundaryNodesBatches;
} & DAGInfo;

// End GuestOS rollout to API boundary nodes types.

// Start HostOS rollout types.

// types::v2::hostos::State
const HostOsState = {
    failed: { icon: "❌", name: "failed" },
    problem: { icon: "⚠️", name: "problem" },
    preparing: { icon: "🔁", name: "preparing" },
    waiting: { icon: "⌛", name: "waiting" },
    canary: { icon: "🐤", name: "in canary phase" },
    main: { icon: "🚀", name: "in main phase" },
    unassigned: { icon: "🪺", name: "tending to unassigned nodes" },
    stragglers: { icon: "🐌", name: "tending to stragglers" },
    complete: { icon: "🏁", name: "complete" },
};
export function hostOsStateIcon(rollout: HostOsRollout): String {
    return HostOsState[rollout.state].icon;
};
export function hostOsStateName(rollout: HostOsRollout): string {
    return HostOsState[rollout.state].name;
};

// types::v2::hostos::BatchState
const HostOsBatchState = {
    error: { icon: "❌", name: "error" },
    predecessor_failed: { icon: "❌", name: "predecessor failed" },
    pending: { icon: "🕐", name: "pending" },
    waiting: { icon: "⌛", name: "waiting" },
    determining_targets: { icon: "🔎", name: "determining targets" },
    proposing: { icon: "📝", name: "proposing update to nodes" },
    waiting_for_election: {
        icon: "🗳️",
        name: "waiting for proposal to be approved",
    },
    waiting_for_adoption: {
        icon: "⚡",
        name: "waiting for nodes to upgrade",
    },
    waiting_until_nodes_healthy: {
        icon: "📢",
        name: "waiting until all nodes are healthy",
    },
    complete: { icon: "✅", name: "complete" },
    skipped: { icon: "⏩", name: "skipped" },
    unknown: { icon: "❓", name: "does not appear in Airflow" },
};
export function hostOsBatchStateName(batch: HostOsBatch): String {
    return HostOsBatchState[batch.state].name;
}
export function hostOsBatchStateIcon(batch: HostOsBatch): String {
    return HostOsBatchState[batch.state].icon;
}
export function hostOsBatchStateComment(subnet: HostOsBatch): string {
    let s = HostOsBatchState[subnet.state].name;
    if (subnet.comment) {
        s = s + " • " + subnet.comment
    }
    return s
}

// types::v2::hostos::Node
export type HostOsNode = {
    node_id: string;
};

// types::v2::hostos::NodeSelector
export type HostOsNodeSpecifier = {
    assignment: "assigned" | "unassigned" | "API boundary" | null
    owner: "DFINITY" | "others" | null
    nodes_per_group: number | string
    group_by: "datacenter" | "subnet" | null
    status: "Healthy" | "Degraded" | "Dead" | null
}

export type HostOsNodeAggregator = {
    join: HostOsNodeSelectors[]
}

export type HostOsNodeFilter = {
    intersect: HostOsNodeSelectors[]
}

export type HostOsNodeSelectors = HostOsNodeAggregator | HostOsNodeFilter | HostOsNodeSpecifier;

function formatSelector(selector: HostOsNodeSpecifier): string {
    let health =
        selector.status === null
            ? " "
            : ` ${selector.status.toLowerCase()} `;

    let s = "";
    if (typeof selector.nodes_per_group === "string") {
        s = `${selector.nodes_per_group} of${health}nodes`;
    } else if (selector.nodes_per_group == 1) {
        s = `${selector.nodes_per_group}${health}node`;
    } else {
        s = `${selector.nodes_per_group}${health}nodes`;
    }

    if (selector.group_by !== null) {
        if (
            !(
                selector.assignment === "unassigned" &&
                selector.group_by === "subnet"
            )
        ) {
            s = `${s} per ${selector.group_by}`;
        }
    }

    if (selector.owner === "DFINITY") {
        s = `${s} owned by ${selector.owner}`;
    } else if (selector.owner === "others") {
        s = `${s} not owned by DFINITY`;
    }

    if (
        selector.assignment === "assigned" &&
        selector.group_by !== "subnet"
    ) {
        s = `${s} assigned to a subnet`;
    } else if (selector.assignment === "unassigned") {
        s = `${s} not assigned to any subnet`;
    } else if (selector.assignment === "API boundary") {
        s = `${s} assigned to API boundary duty`;
    }

    return s;
}

export function formatSelectors(selectors: HostOsNodeSelectors | null): string {
    if (selectors === null) {
        return "Selectors not known";
    }
    if ((selectors as HostOsNodeFilter).intersect !== undefined) {
        if ((selectors as HostOsNodeFilter).intersect.length === 0) {
            return "All remaining nodes";
        } else if ((selectors as HostOsNodeFilter).intersect.length === 1) {
            return formatSelectors((selectors as HostOsNodeFilter).intersect[0])
        } else {
            return "( " + (selectors as HostOsNodeFilter).intersect.map((s) => formatSelectors(s)).join(" ∩ ") + " )"
        }
    } else if ((selectors as HostOsNodeAggregator).join !== undefined) {
        if ((selectors as HostOsNodeAggregator).join.length === 0) {
            return "No nodes";
        } else if ((selectors as HostOsNodeAggregator).join.length === 1) {
            return formatSelectors((selectors as HostOsNodeAggregator).join[0])
        } else {
            return "( " + (selectors as HostOsNodeAggregator).join.map((s) => formatSelectors(s)).join(" ∪ ") + " )"
        }
    }
    return formatSelector((selectors as HostOsNodeSpecifier));
}


// types::v2::hostos::Batch
export type HostOsBatch = {
    planned_start_time: Date;
    actual_start_time: Date | null;
    end_time: Date | null;
    state: keyof typeof HostOsBatchState;
    comment: String;
    display_url: string;
    planned_nodes: HostOsNode[];
    actual_nodes: HostOsNode[] | null;
    selectors: HostOsNodeSelectors | null;
};

export type HostOsRolloutConfiguration = {
    simulate: boolean;
    git_revision: String;
};

export type HostOsStages = {
    canary: { [key: string]: HostOsBatch }
    main: { [key: string]: HostOsBatch }
    unassigned: { [key: string]: HostOsBatch }
    stragglers: { [key: string]: HostOsBatch }
};

// types::v2::hostos::Rollout
export type HostOsRollout = {
    kind: "rollout_ic_os_to_mainnet_nodes";
    conf: HostOsRolloutConfiguration;
    state: keyof typeof HostOsState;
    stages: HostOsStages | null;
} & DAGInfo;

// types:v2::hostos::NodeInfo
export type NodeInfo = {
    node_id: string
    node_provider_id: string
    assignment: string | null // Either a subnet ID or "API boundary"
    dc_id: string
    status: string
}

export type UpgradeStatus = "pending" | "upgraded" | "AWOL"

export type AlertStatus = "OK" | "alerting" | "unknown"

// types::v2::hostos::BatchResponse
export type HostOsBatchResponse = {
    stage: keyof HostOsStages
    batch_number: number
    planned_start_time: Date;
    actual_start_time: Date | null;
    end_time: Date | null;
    state: keyof typeof HostOsBatchState;
    comment: String;
    display_url: string;
    planned_nodes: NodeInfo[];
    actual_nodes: NodeInfo[] | null;
    selectors: HostOsNodeSelectors | null;
    upgraded_nodes: { [key: string]: UpgradeStatus } | null
    alerting_nodes: { [key: string]: AlertStatus } | null
}

// End HostOS rollout types.

// Combination structures.
export type RolloutKind = "rollout_ic_os_to_mainnet_subnets" | "rollout_ic_os_to_mainnet_api_boundary_nodes" | "rollout_ic_os_to_mainnet_nodes";
const RolloutKindName = {
    rollout_ic_os_to_mainnet_subnets: "GuestOS rollout to subnets",
    rollout_ic_os_to_mainnet_api_boundary_nodes: "GuestOS rollout to API boundary nodes",
    rollout_ic_os_to_mainnet_nodes: "HostOS rollout to nodes",
};
export function rolloutKindName(rollout: Rollout | keyof typeof RolloutKindName): String {
    var state: keyof typeof RolloutKindName;
    if (typeof rollout === "object") {
        state = (rollout as Rollout).kind;
    } else {
        state = rollout as keyof typeof RolloutKindName;
    }
    return RolloutKindName[state];
}
export type Rollout = GuestOsRollout | ApiBoundaryNodesRollout | HostOsRollout;

// Type for deleted rollout.
export type DeletedRollout = {
    kind: RolloutKind;
    name: String;
};

// Rollout engine states.
export type RolloutEngineStates = {
    [Property in RolloutKind]: string;
};
type ValueOf<T> = T[keyof T]
type Entries<T> = [keyof T, ValueOf<T>][]
export const getRolloutEngineStates = <T extends object>(obj: RolloutEngineStates) => Object.entries(obj) as Entries<RolloutEngineStates>;

// State update types.
// Full state update.
export type State = {
    rollouts: Rollout[];
    rollout_engine_states: RolloutEngineStates;
};
// Error.  An HTTP code and a message in a dict.  Used by SSE.
// A flag permanent indicates if the error is expected to never resolve
// itself, and therefore the client should close the connection.  The
// server will usually close the connection when the message it sent
// has this flag and the flag is true.
export type Error = {
    code: number;
    message: string;
    permanent: boolean;
};

// Delta state update.
export type RolloutsDelta = {
    updated: Rollout[];
    deleted: DeletedRollout[];
};

/* Unstable types. */
