<script lang="ts">
    import Time from "svelte-time";
    import { copy } from "svelte-copy";
    import { toast } from "@zerodevx/svelte-toast";
    import SvelteMarkdown from "@humanspeak/svelte-markdown";
    import {
        type ApiBoundaryNodesRollout,
        apiBoundaryNodesStateName,
        apiBoundaryNodesStateIcon,
        rolloutKindName,
    } from "./types";
    import { cap, activeClass, selectTextOnFocus } from "./lib";
    import ApiBoundaryNodesBatch from "./ApiBoundaryNodesBatch.svelte";
    import BlackInfoBlock from "./BlackInfoBlock.svelte";
    import ExternalLinkIcon from "./ExternalLinkIcon.svelte";
    import ClipboardIcon from "./ClipboardIcon.svelte";
    import InfoBlock from "./InfoBlock.svelte";
    interface Props {
        rollout: ApiBoundaryNodesRollout;
        paused: boolean;
    }

    let { rollout, paused }: Props = $props();

    let rolloutClass: String = activeClass(rollout.state);
    let git_revision: string = rollout.conf.git_revision.toString();
</script>

<section class="rollout {rolloutClass} {rollout.kind}">
    <div class="general_info">
        <a
            rel="external"
            href={rollout.display_url}
            target="_blank"
            title={cap(apiBoundaryNodesStateName(rollout))}
            data-sveltekit-preload-data="off"
        >
            <span class="state_icon">
                {apiBoundaryNodesStateIcon(rollout)}
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
            />{#if rollout.last_scheduling_decision}, {apiBoundaryNodesStateName(
                    rollout,
                )}
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
    {#if paused}
        <InfoBlock>
            <span class="font-medium">This rollout has its engine paused.</span>
            Rollouts of this type have been paused by DRE. Use the
            <i>Help</i> link below if you want to inquire why.
        </InfoBlock>
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
    {#if rollout.batches && Object.keys(rollout.batches).length > 0}
        <ul>
            {#each Object.entries(rollout.batches) as [batch_num, batch]}
                <ApiBoundaryNodesBatch {batch_num} {batch} />
            {/each}
        </ul>
    {:else}
        <BlackInfoBlock>
            This rollout {#if rollout.state == "complete" || rollout.state == "failed"}never{:else}has
                not yet{/if} computed a rollout plan.
        </BlackInfoBlock>
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
</style>
