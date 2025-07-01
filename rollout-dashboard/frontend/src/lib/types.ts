// Generic rollout variant and helpers / support structures.
export type DAGInfo = {
    name: String;
    display_url: string;
    note?: String;
    dispatch_time: Date;
    last_scheduling_decision?: Date;
}

// GuestOS rollout to mainnet subnet types.
const SubnetRolloutState = {
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
export function subnetStateIcon(subnet: Subnet): String {
    return SubnetRolloutState[subnet.state].icon;
}
export function subnetStateComment(subnet: Subnet): string {
    let s = SubnetRolloutState[subnet.state].name;
    if (subnet.comment) {
        s = s + ": " + subnet.comment
    }
    return s
}
export type Subnet = {
    subnet_id: string;
    git_revision: string;
    state: keyof typeof SubnetRolloutState;
    comment: String;
    display_url: string;
};
export type SubnetBatch = {
    planned_start_time: Date;
    actual_start_time?: Date;
    end_time?: Date;
    subnets: Subnet[];
};
const RolloutIcOsToMainnetSubnetsState = {
    complete: { icon: "✅", name: "complete" },
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
export function rolloutIcOsToMainnetSubnetsStateIcon(rollout: RolloutIcOsToMainnetSubnets): String {
    return RolloutIcOsToMainnetSubnetsState[rollout.state].icon;
}
export function rolloutIcOsToMainnetSubnetsStateName(rollout: RolloutIcOsToMainnetSubnets): string {
    return RolloutIcOsToMainnetSubnetsState[rollout.state].name;
}
export type RolloutIcOsToMainnetSubnetsConfiguration = {
    simulate: boolean;
};
export type SubnetBatches = {
    String: SubnetBatch
}
export type RolloutIcOsToMainnetSubnets = {
    kind: "rollout_ic_os_to_mainnet_subnets";
    conf: RolloutIcOsToMainnetSubnetsConfiguration;
    state: keyof typeof RolloutIcOsToMainnetSubnetsState;
    batches: SubnetBatches;
} & DAGInfo;

// GuestOS rollout to API boundary nodes types.
const RolloutIcOsToMainnetApiBoundaryNodesState = {
    complete: { icon: "✅", name: "complete" },
    failed: { icon: "❌", name: "failed" },
    preparing: { icon: "🔁", name: "preparing" },
    upgrading_api_boundary_nodes: { icon: "▶️", name: "upgrading API boundary nodes" },
    waiting: { icon: "⌛", name: "waiting" },
    problem: { icon: "⚠️", name: "problem" },
};
export function rolloutIcOsToMainnetApiBoundaryNodesStateIcon(rollout: RolloutIcOsToMainnetApiBoundaryNodes): String {
    return RolloutIcOsToMainnetApiBoundaryNodesState[rollout.state].icon;
}
export function rolloutIcOsToMainnetApiBoundaryNodesStateName(rollout: RolloutIcOsToMainnetApiBoundaryNodes): string {
    return RolloutIcOsToMainnetApiBoundaryNodesState[rollout.state].name;
}

export type RolloutIcOsToMainnetApiBoundaryNodesConfiguration = {
    simulate: boolean;
};
export type ApiBoundaryNode = {
    node_id: string;
    git_revision: string;
};
const ApiBoundaryNodesBatchRolloutState = {
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
export function apiBoundaryNodesBatchRolloutStateIcon(batch: ApiBoundaryNodesBatch): String {
    return ApiBoundaryNodesBatchRolloutState[batch.state].icon;
}
export function apiBoundaryNodesBatchRolloutStateComment(subnet: ApiBoundaryNodesBatch): string {
    let s = ApiBoundaryNodesBatchRolloutState[subnet.state].name;
    if (subnet.comment) {
        s = s + ": " + subnet.comment
    }
    return s
}
export type ApiBoundaryNodesBatch = {
    planned_start_time: Date;
    actual_start_time?: Date;
    end_time?: Date;
    state: keyof typeof ApiBoundaryNodesBatchRolloutState;
    comment: String;
    display_url: string;
    api_boundary_nodes: ApiBoundaryNode[];
};
export type ApiBoundaryNodesBatches = {
    String: ApiBoundaryNodesBatch
}
export type RolloutIcOsToMainnetApiBoundaryNodes = {
    kind: "rollout_ic_os_to_mainnet_api_boundary_nodes";
    conf: RolloutIcOsToMainnetApiBoundaryNodesConfiguration;
    state: keyof typeof RolloutIcOsToMainnetApiBoundaryNodesState;
    batches: ApiBoundaryNodesBatches;
} & DAGInfo;

// Combination structures.
export type RolloutKind = "rollout_ic_os_to_mainnet_subnets" | "rollout_ic_os_to_mainnet_api_boundary_nodes";
const RolloutKindName = {
    rollout_ic_os_to_mainnet_subnets: "GuestOS rollout to subnets",
    rollout_ic_os_to_mainnet_api_boundary_nodes: "GuestOS rollout to API boundary nodes",
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
export type Rollout = RolloutIcOsToMainnetSubnets | RolloutIcOsToMainnetApiBoundaryNodes;

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
// Error.  An HTTP code and a message in a dict.
export type Error = {
    code: Number;
    message: String;
};

// Delta state update.
export type RolloutsDelta = {
    updated: Rollout[];
    deleted: DeletedRollout[];
};
