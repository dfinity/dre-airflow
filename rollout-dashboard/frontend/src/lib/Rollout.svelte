<script>
    import Time from "svelte-time";
	import Batch from './Batch.svelte';
	export let rollout;
    var rollout_states = {
        complete: {icon: "✅", name: "Complete"},
        failed: {icon: "❌", name: "Failed"},
        preparing: {icon: "🔁", name: "Preparing"},
        upgrading_subnets: {icon: "▶️", name: "Upgrading subnets"},
        upgrading_unassigned_nodes: {icon: "⏩", name: "Upgrading unassigned nodes"},
        waiting: {icon: "⌛", name: "Waiting"},
        problem: {icon: "⚠️", name: "Problem"}
    };
</script>

<style>
	ul {
		display: flex;
        gap: 0.6em;
        flex-direction: row;
        flex-wrap: wrap;
        margin: 0;
        padding: 0;
        list-style-type: none;
        dborder: 1px solid red;
        margin-top: 0.6em;
	}
    li {
        dborder: 1px solid blue;
        background-color: #e4e4e4;
        padding: 0.6em;
    }
    .rollout {
        border-left: 10px solid #999;
        padding-left: 1em;
        margin-top: 2em;
        margin-bottom: 2em;
    }
    .rollout .general_info {
		display: flex;
        flex-direction: row;
    }
    .rollout .state_icon {
        order: 0;
        flex-grow: 0;
    }
    .rollout .name {
        order: 1;
        flex-grow: 1;
        margin-left: 2em;
        margin-right: 2em;
        font-weight: bold;
        text-align: center;
    }
    .rollout .start_time {
        order: 2;
    }
    .rollout .note {
        color: #333;
        font-style: italic;
    }
</style>

<section class="rollout">
    <div class="general_info">
        <div class="name">{rollout.name}</div>
        <div class="state_icon tooltip">{rollout_states[rollout.state].icon}<span class="state tooltiptext">{rollout_states[rollout.state].name}</span></div>
        <div class="start_time"><Time live relative timestamp="{rollout.dispatch_time}" /></div>
    </div>
    {#if rollout.note}
    <p class="note">{rollout.note}</p>
    {/if}
    <ul>
        {#each Object.entries(rollout.batches) as [batch_num, batch]}
        <li>
        <Batch batch_num={batch_num} batch={batch}/>
        </li>
        {/each}
    </ul>
</section>
