<script lang="ts">
    import Time from "svelte-time";
    import { copy } from "svelte-copy";
    import { toast } from "@zerodevx/svelte-toast";
    import SvelteMarkdown from "svelte-markdown";
    import {
        type HostOsRollout,
        hostOsStateName,
        hostOsStateIcon,
        rolloutKindName,
    } from "./types";
    import { cap, activeClass, selectTextOnFocus } from "./lib";
    import HostOSBatch from "./HostOSBatch.svelte";
    export let rollout: HostOsRollout;

    let rolloutClass: String = activeClass(rollout.state);
    let git_revision: string = rollout.conf.git_revision.toString();
</script>

<section class="rollout {rolloutClass} {rollout.kind}">
    <div class="general_info">
        <a
            rel="external"
            href={rollout.display_url}
            target="_blank"
            title={cap(hostOsStateName(rollout))}
            data-sveltekit-preload-data="off"
        >
            <span class="state_icon">
                {hostOsStateIcon(rollout)}
            </span>
            <span class="kind">
                {rolloutKindName(rollout)}
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
            />{#if rollout.last_scheduling_decision}, {hostOsStateName(rollout)}
                <Time
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
    <div class="rollout_info space-y-4 text-gray-500">
        Git revision:
        <div
            class="git_revision"
            role="link"
            tabindex="0"
            use:copy={git_revision}
            use:selectTextOnFocus
            on:svelte-copy={(event) =>
                toast.push("Copied git revision to clipboard")}
        >
            <svg
                class="w-3 h-3 me-1.5"
                aria-hidden="true"
                xmlns="http://www.w3.org/2000/svg"
                fill="currentColor"
                viewBox="0 0 18 20"
            >
                <path
                    d="M16 1h-3.278A1.992 1.992 0 0 0 11 0H7a1.993 1.993 0 0 0-1.722 1H2a2 2 0 0 0-2 2v15a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2V3a2 2 0 0 0-2-2Zm-3 14H5a1 1 0 0 1 0-2h8a1 1 0 0 1 0 2Zm0-4H5a1 1 0 0 1 0-2h8a1 1 0 1 1 0 2Zm0-5H5a1 1 0 0 1 0-2h2V2h4v2h2a1 1 0 1 1 0 2Z"
                />
            </svg>{git_revision}
        </div>
    </div>
    {#if rollout.stages}
        <div class="stages">
            {#if rollout.stages.canary["1"] !== undefined}
                <ul class="stage-canary">
                    {#each Object.entries(rollout.stages.canary) as [batch_num, batch]}
                        <HostOSBatch {batch_num} {batch} />
                    {/each}
                </ul>
                <div class="stage-canary stage-name text-gray-500 rounded-lg">
                    Canary
                </div>
            {/if}
            {#if rollout.stages.main["1"] !== undefined}
                <ul class="stage-main">
                    {#each Object.entries(rollout.stages.main) as [batch_num, batch]}
                        <HostOSBatch {batch_num} {batch} />
                    {/each}
                </ul>
                <div class="stage-main stage-name text-gray-500 rounded-lg">
                    Main
                </div>
            {/if}
            {#if rollout.stages.unassigned["1"] !== undefined}
                <ul class="stage-unassigned">
                    {#each Object.entries(rollout.stages.unassigned) as [batch_num, batch]}
                        <HostOSBatch {batch_num} {batch} />
                    {/each}
                </ul>
                <div
                    class="stage-unassigned stage-name text-gray-500 rounded-lg"
                >
                    Unassigned
                </div>
            {/if}
            {#if rollout.stages.stragglers["1"] !== undefined}
                <ul class="stage-stragglers">
                    {#each Object.entries(rollout.stages.stragglers) as [batch_num, batch]}
                        <HostOSBatch {batch_num} {batch} />
                    {/each}
                </ul>
                <div
                    class="stage-stragglers stage-name text-gray-500 rounded-lg"
                >
                    Stragglers
                </div>
            {/if}
        </div>
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
    <!-- 
    {#if rollout.batches && Object.keys(rollout.batches).length > 0}
    {:else}
    {/if}-->
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
    .rollout .general_info,
    .rollout .rollout_info {
        display: flex;
        gap: 0.3em;
        flex-direction: row;
    }
    .rollout .rollout_info {
        justify-content: flex-end;
    }
    .git_revision {
        display: inline;
        color: #999;
        text-overflow: ellipsis;
        overflow-x: hidden;
        font-family: monospace;
        font-size: 120%;
        text-align: right;
    }
    .git_revision {
        cursor: copy;
    }
    .git_revision svg {
        display: none;
    }
    .git_revision:hover svg {
        display: block;
        position: absolute;
        margin-left: -1em;
        margin-top: 0.35em;
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
    div.stages {
        display: grid;
        grid-template-columns: auto min-content;
        gap: 0.6em;
    }
    div.stage-name {
        text-align: center;
        writing-mode: vertical-lr;
        text-orientation: mixed;
        padding-top: 1em; /* add padding to the pills on the right side of each stage */
        padding-bottom: 1em;
        padding-left: 0.3em;
        padding-right: 0.3em;
    }
    div.stage-name.stage-canary {
        background-color: #e1e0d2;
    }
    div.stage-name.stage-main {
        background-color: #d2e1d4;
    }
    div.stage-name.stage-unassigned {
        background-color: #d2dce1;
    }
    div.stage-name.stage-stragglers {
        background-color: #dcd2e1;
    }
</style>
