<script>
    import Time from "svelte-time";
	export let batch;
    export let batch_num;
    var subnet_rollout_states = {
        pending: {icon: "🕐", name: "Pending"},
        waiting: {icon: "⌛", name: "Waiting"},
        proposing: {icon: "📝", name: "Proposing update to new revision"},
        waiting_for_election: {icon: "🗳️", name: "Waiting for revision election"},
        waiting_for_adoption: {icon: "⚡", name: "Waiting for revision adoption"},
        waiting_for_alerts_gone: {icon: "📢", name: "Waiting until no more alerts"},
        complete: {icon: "✅", name: "Complete"},
        skipped: {icon: "⏩", name: "Skipped"},
        error: {icon: "❌", name: "Error"},
    };
</script>

<style>
	ul {
		display: flex;
        flex-direction: row;
        flex-wrap: wrap;
        margin: 0;
        padding: 0;
        list-style-type: none;
        dborder: 1px solid green;
	}
    li {
        padding-left: 0;
        margin-left: 0;
        dborder: 1px solid orange;
    }
    .subnet_id {
        width: 4rem;
        display: inline-block;
        font-family: monospace;
        font-size: 120%;
    }
    .git_revision {
        color: #999;
    }
    .start_time {
        text-align: center;
    }
</style>

{#each batch.subnets as subnet}
<ul>
    <li>
        <div class="subnet_state_icon tooltip">{subnet_rollout_states[subnet.state].icon}<span class="subnet_state tooltiptext">{subnet_rollout_states[subnet.state].name}</span></div>
        <span class="subnet_id">{subnet.subnet_id.substring(0, 5)}</span>
        <span class="git_revision">{subnet.git_revision.substring(0, 8)}</span>
    </li>
</ul>
{/each}
<div class="start_time text-gray-500"><Time live relative timestamp="{batch.start_time}"  format="dddd @ h:mm A · MMMM D, YYYY" /></div>
