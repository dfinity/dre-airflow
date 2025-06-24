// GuestOS rollout to mainnet types.
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
export type RolloutIcOsToMainnetConfiguration = {
    simulate: boolean;
};
export type RolloutIcOsToMainnetSubnets = {
    kind: RolloutKind;
    conf: RolloutIcOsToMainnetConfiguration;
    state: keyof typeof RolloutIcOsToMainnetSubnetsState;
    batches: SubnetBatch[];
} & DAGInfo;

// Generic rollout variant and helpers / support structures.
export type DAGInfo = {
    name: String;
    display_url: string;
    note?: String;
    dispatch_time: Date;
    last_scheduling_decision?: Date;
}
export type RolloutKind = "rollout_ic_os_to_mainnet_subnets" | "rollout_ic_os_to_mainnet_api_boundary_nodes";
const RolloutKindName = {
    rollout_ic_os_to_mainnet_subnets: "GuestOS rollout",
    rollout_ic_os_to_mainnet_api_boundary_nodes: "APIBoundaryNodeRollout",
};
export function rolloutKindName(rollout: Rollout | keyof typeof RolloutKindName): String {
    var state: keyof typeof RolloutKindName;
    if (rollout instanceof String) {
        state = rollout as keyof typeof RolloutKindName;
    } else {
        state = (rollout as Rollout).kind;
    }
    return RolloutKindName[state];
}
export type Rollout = RolloutIcOsToMainnetSubnets;

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
