<script lang="ts">
    import Time from "svelte-time";
    import SvelteMarkdown from "svelte-markdown";
    import { type Rollout, rolloutStateIcon, rolloutStateName } from "./types";
    import Batch from "./Batch.svelte";
    export let rollout: Rollout;

    let rolloutClass: string = rollout.state;
    if (rolloutClass !== "complete" && rolloutClass !== "failed") {
        rolloutClass = "active";
    }
</script>

<section class="rollout {rolloutClass}">
    <div class="general_info">
        <a
            rel="external"
            href={rollout.display_url}
            target="_blank"
            title={rolloutStateName(rollout.state)}
            data-sveltekit-preload-data="off"
        >
            <span class="state_icon">
                {rolloutStateIcon(rollout.state)}
            </span>
            <span class="name">
                {rollout.name}
                <svg
                    style="display: inline-block"
                    xmlns="http://www.w3.org/2000/svg"
                    width="1em"
                    height="1em"
                    viewBox="0 0 15 15"
                    ><path
                        fill="currentColor"
                        fill-rule="evenodd"
                        d="M12 13a1 1 0 0 0 1-1V3a1 1 0 0 0-1-1H3a1 1 0 0 0-1 1v3.5a.5.5 0 0 0 1 0V3h9v9H8.5a.5.5 0 0 0 0 1zM9 6.5v3a.5.5 0 0 1-1 0V7.707l-5.146 5.147a.5.5 0 0 1-.708-.708L7.293 7H5.5a.5.5 0 0 1 0-1h3a.498.498 0 0 1 .5.497"
                        clip-rule="evenodd"
                    /></svg
                >
                {#if rollout.conf.simulate}<i class="simulated">(simulated)</i
                    >{/if}
            </span>
        </a>
        <div class="times text-gray-500">
            Started <Time
                live
                relative
                timestamp={rollout.dispatch_time}
                format="dddd @ h:mm A · MMMM D, YYYY"
            />{#if rollout.last_scheduling_decision}, updated <Time
                    live
                    relative
                    timestamp={rollout.last_scheduling_decision}
                    format="dddd @ h:mm A · MMMM D, YYYY"
                />{/if}
        </div>
    </div>
    {#if rollout.note}
        <div class="note space-y-4">
            <SvelteMarkdown source={rollout.note} />
        </div>
    {/if}
    {#if rollout.batches && Object.keys(rollout.batches).length > 0}
        <ul>
            {#each Object.entries(rollout.batches) as [batch_num, batch]}
                <Batch {batch_num} {batch} />
            {/each}
        </ul>
    {:else}
        <div
            class="flex items-center p-4 text-sm text-gray-800 border border-gray-300 rounded-lg bg-gray-50 dark:bg-gray-800 dark:text-gray-300 dark:border-gray-600"
            role="alert"
        >
            <svg
                class="flex-shrink-0 inline w-4 h-4 me-3"
                aria-hidden="true"
                xmlns="http://www.w3.org/2000/svg"
                fill="currentColor"
                viewBox="0 0 20 20"
            >
                <path
                    d="M10 .5a9.5 9.5 0 1 0 9.5 9.5A9.51 9.51 0 0 0 10 .5ZM9.5 4a1.5 1.5 0 1 1 0 3 1.5 1.5 0 0 1 0-3ZM12 15H8a1 1 0 0 1 0-2h1v-3H8a1 1 0 0 1 0-2h2a1 1 0 0 1 1 1v4h1a1 1 0 0 1 0 2Z"
                />
            </svg>
            <span class="sr-only">Info</span>
            <div>
                This rollout {#if rollout.state == "complete" || rollout.state == "failed"}never{:else}has
                    not yet{/if} computed a rollout plan.
            </div>
        </div>
    {/if}
</section>

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
    .rollout {
        border-left: 10px solid #999;
        padding-left: 0.6em;
        display: flex;
        flex-direction: column;
        row-gap: 0.6em;
    }
    .rollout.active {
        border-left: 10px solid #4280b3;
    }
    .rollout.complete {
        border-left: 10px solid #7cb342;
    }
    .rollout.failed {
        border-left: 10px solid #b34242;
    }
    .rollout .general_info {
        display: flex;
        gap: 0.3em;
        flex-direction: row;
    }
    .rollout a {
        order: 0;
        flex-grow: 1;
    }
    .rollout .name {
        font-weight: bold;
        text-align: left;
    }
    .rollout .name .simulated {
        color: #9f8317;
        font-style: italic;
    }
    .rollout .times {
        order: 1;
    }
</style>
