const subnet_rollout_states = {
    pending: { icon: "🕐", name: "Pending" },
    waiting: { icon: "⌛", name: "Waiting" },
    proposing: { icon: "📝", name: "Proposing update to new revision" },
    waiting_for_election: {
        icon: "🗳️",
        name: "Waiting for revision election",
    },
    waiting_for_adoption: {
        icon: "⚡",
        name: "Waiting for revision adoption",
    },
    waiting_for_alerts_gone: {
        icon: "📢",
        name: "Waiting until no more alerts",
    },
    complete: { icon: "✅", name: "Complete" },
    skipped: { icon: "⏩", name: "Skipped" },
    error: { icon: "❌", name: "Error" },
    predecessor_failed: { icon: "❌", name: "Predecessor failed" },
    unknown: { icon: "❓", name: "Does not appear in Airflow" },
};
export function batchStateIcon(state: keyof typeof subnet_rollout_states): String {
    return subnet_rollout_states[state].icon;
}
export function batchStateName(state: keyof typeof subnet_rollout_states): String {
    return subnet_rollout_states[state].name;
}
export type Subnet = {
    subnet_id: String;
    git_revision: String;
    state: keyof typeof subnet_rollout_states;
    comment: String;
};
export type Batch = {
    subnets: Subnet[];
    planned_start_time: Date;
    actual_start_time?: Date;
    end_time?: Date;
};
const rollout_states = {
    complete: { icon: "✅", name: "Complete" },
    failed: { icon: "❌", name: "Failed" },
    preparing: { icon: "🔁", name: "Preparing" },
    upgrading_subnets: { icon: "▶️", name: "Upgrading subnets" },
    upgrading_unassigned_nodes: {
        icon: "⏩",
        name: "Upgrading unassigned nodes",
    },
    waiting: { icon: "⌛", name: "Waiting" },
    problem: { icon: "⚠️", name: "Problem" },
};
export function rolloutStateIcon(state: keyof typeof rollout_states): String {
    return rollout_states[state].icon;
}
export function rolloutStateName(state: keyof typeof rollout_states): String {
    return rollout_states[state].name;
}
export type RolloutConfiguration = {
    simulate: boolean;
};
export type Rollout = {
    name: String;
    note?: String;
    conf: RolloutConfiguration;
    state: keyof typeof rollout_states;
    dispatch_time: Date;
    last_scheduling_decision?: Date;
    batches: Batch[];
};
