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
	}
    li {
        background-color: #e4e4e4;
        padding: 0.6em;
    }
    .rollout {
        border-left: 10px solid #999;
        padding-left: 0.6em;
        display: flex;
        flex-direction: column;
        row-gap: 0.6em;
    }
    .rollout .general_info {
		display: flex;
        gap: 0.3em;
        flex-direction: row;
    }
    .rollout .state_icon {
        order: 0;
        flex-grow: 0;
    }
    .rollout .name {
        order: 1;
        flex-grow: 1;
        font-weight: bold;
        text-align: left;
    }
    .rollout .name .simulated {
        color: #9f8317;
        font-style: italic;
    }
    .rollout .times {
        order: 2;
    }
    p.note {
        color: #444;
        font-style: italic;
        white-space: pre-wrap;
    }
</style>

<section class="rollout">
    <div class="general_info">
        <div class="name">{rollout.name} {#if rollout.conf.simulate}<i class="simulated">(simulated)</i>{/if}</div>
        <div class="state_icon tooltip">{rollout_states[rollout.state].icon}<span class="state tooltiptext">{rollout_states[rollout.state].name}</span></div>
        <div class="times text-gray-500">Started <Time live relative timestamp="{rollout.dispatch_time}"  format="dddd @ h:mm A · MMMM D, YYYY" />{#if rollout.last_scheduling_decision}, updated <Time live relative timestamp="{rollout.last_scheduling_decision}"  format="dddd @ h:mm A · MMMM D, YYYY" />{/if}</div>
    </div>
    {#if rollout.note}
    <p class="note">{rollout.note}</p>
    {/if}
    {#if rollout.batches && Object.keys(rollout.batches).length  > 0}
    <ul>
        {#each Object.entries(rollout.batches) as [batch_num, batch]}
        <li class="rounded-lg border">
        <Batch batch_num={batch_num} batch={batch}/>
        </li>
        {/each}
    </ul>
    {:else}
    <div class="flex items-center p-4 text-sm text-gray-800 border border-gray-300 rounded-lg bg-gray-50 dark:bg-gray-800 dark:text-gray-300 dark:border-gray-600" role="alert">
        <svg class="flex-shrink-0 inline w-4 h-4 me-3" aria-hidden="true" xmlns="http://www.w3.org/2000/svg" fill="currentColor" viewBox="0 0 20 20">
            <path d="M10 .5a9.5 9.5 0 1 0 9.5 9.5A9.51 9.51 0 0 0 10 .5ZM9.5 4a1.5 1.5 0 1 1 0 3 1.5 1.5 0 0 1 0-3ZM12 15H8a1 1 0 0 1 0-2h1v-3H8a1 1 0 0 1 0-2h2a1 1 0 0 1 1 1v4h1a1 1 0 0 1 0 2Z"/>
        </svg>
        <span class="sr-only">Info</span>
        <div>
            This rollout {#if rollout.state == "complete" || rollout.state == "failed"}never{:else}has not yet{/if} computed a rollout plan.
        </div>
    </div>
    {/if}
</section>
