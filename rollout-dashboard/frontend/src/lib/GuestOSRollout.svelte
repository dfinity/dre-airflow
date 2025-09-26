<script lang="ts">
    import Time from "svelte-time";
    import SvelteMarkdown from "@humanspeak/svelte-markdown";
    import {
        type GuestOsRollout,
        guestOsStateIcon,
        GuestOsStateName,
        rolloutKindName,
    } from "./types";
    import { cap, activeClass } from "./lib";
    import SubnetBatch from "./SubnetBatch.svelte";
    import BlackInfoBlock from "./BlackInfoBlock.svelte";
    import ExternalLinkIcon from "./ExternalLinkIcon.svelte";
    import InfoBlock from "./InfoBlock.svelte";
    interface Props {
        rollout: GuestOsRollout;
        paused: boolean;
    }

    let { rollout, paused }: Props = $props();

    let rolloutClass: String = activeClass(rollout.state);
</script>

<section class="rollout {rolloutClass} {rollout.kind}">
    <div class="general_info">
        <a
            rel="external"
            href={rollout.display_url}
            target="_blank"
            title={cap(GuestOsStateName(rollout))}
            data-sveltekit-preload-data="off"
        >
            <span class="state_icon">
                {guestOsStateIcon(rollout)}
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
            />{#if rollout.last_scheduling_decision}, {GuestOsStateName(
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
    {#if rollout.batches && Object.keys(rollout.batches).length > 0}
        <ul class="batch_list">
            {#each Object.entries(rollout.batches) as [batch_num, batch]}
                <SubnetBatch {batch_num} {batch} />
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
    ul.batch_list {
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
