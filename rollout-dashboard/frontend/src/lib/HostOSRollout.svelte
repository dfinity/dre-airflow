<script lang="ts">
    import Time from "svelte-time";
    import { copy } from "svelte-copy";
    import { toast } from "@zerodevx/svelte-toast";
    import SvelteMarkdown from "@humanspeak/svelte-markdown";
    import {
        type HostOsRollout,
        type HostOsStages,
        hostOsStateName,
        hostOsStateIcon,
        rolloutKindName,
    } from "./types";
    import { cap, activeClass, selectTextOnFocus } from "./lib";
    import HostOSBatch from "./HostOSBatch.svelte";
    import BlackInfoBlock from "./BlackInfoBlock.svelte";
    import ExternalLinkIcon from "./ExternalLinkIcon.svelte";
    import ClipboardIcon from "./ClipboardIcon.svelte";
    interface Props {
        rollout: HostOsRollout;
    }

    let { rollout }: Props = $props();

    let rolloutClass: String = activeClass(rollout.state);
    let git_revision: string = rollout.conf.git_revision.toString();
    let stage_names: (keyof HostOsStages)[] = [
        "canary",
        "main",
        "unassigned",
        "stragglers",
    ];
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
                <ExternalLinkIcon />
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
            <SvelteMarkdown source={rollout.note.toString()} />
        </div>
    {/if}
    <div class="rollout_info space-y-4 text-gray-500">
        Git revision:
        <div
            class="git_revision"
            role="link"
            tabindex="0"
            use:copy={{
                text: git_revision,
                onCopy({ text, event }) {
                    toast.push("Copied git revision to clipboard");
                },
            }}
            use:selectTextOnFocus
        >
            {git_revision}<ClipboardIcon />
        </div>
    </div>
    {#if rollout.stages}
        <div class="stages">
            {#each stage_names as stage_name}
                {#if rollout.stages[stage_name]["1"] !== undefined}
                    <ul class="stage-{stage_name}">
                        {#each Object.entries(rollout.stages[stage_name]) as [batch_num, batch]}
                            <HostOSBatch
                                dag_run_id={rollout.name.toString()}
                                {stage_name}
                                batch_number={batch_num}
                                {batch}
                            />
                        {/each}
                    </ul>
                    <div
                        class="stage-{stage_name} stage-name text-gray-500 rounded-lg"
                    >
                        {cap(stage_name)}
                    </div>
                {/if}
            {/each}
        </div>
    {:else}
        <BlackInfoBlock>
            This rollout {#if rollout.state == "complete" || rollout.state == "failed"}never{:else}has
                not yet{/if} computed a rollout plan.
        </BlackInfoBlock>
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
