<script lang="ts">
    import Time from "svelte-time";
    import { copy } from "svelte-copy";
    import { toast } from "@zerodevx/svelte-toast";
    import {
        type HostOsBatch,
        hostOsBatchStateComment,
        hostOsBatchStateIcon,
    } from "./types";
    import { cap, selectTextOnFocus } from "./lib";
    export let batch_num: String;
    export let batch: HostOsBatch;
</script>

<li class="rounded-lg border batch batch-{batch_num}">
    <a
        rel="external"
        href={batch.display_url || ""}
        target="_blank"
        class="hostos_node_batch_state"
        data-sveltekit-preload-data="off"
        title={cap(hostOsBatchStateComment(batch))}
    >
        <div class="hostos_node_batch_state_icon">
            {hostOsBatchStateIcon(batch)}
        </div></a
    >
    {#if batch.actual_nodes !== null && batch.actual_nodes.length != batch.planned_nodes.length}
        <div class="node_count">
            {batch.actual_nodes.length} nodes targeted
        </div>
        <div class="node_count">
            {batch.planned_nodes.length} nodes planned
        </div>
    {:else if batch.actual_nodes !== null && batch.actual_nodes.length > 0}
        <div class="node_count">
            {batch.actual_nodes.length} nodes targeted
        </div>
    {:else}
        <div class="node_count">
            {batch.planned_nodes.length} nodes planned
        </div>
    {/if}
    <div class="start_time text-gray-500">
        Planned <Time
            live
            relative
            timestamp={batch.planned_start_time}
            format="dddd @ h:mm A · MMMM D, YYYY"
        />
    </div>
    {#if batch.actual_start_time}
        <div class="start_time text-gray-500">
            Started <Time
                live
                relative
                timestamp={batch.actual_start_time}
                format="dddd @ h:mm A · MMMM D, YYYY"
            />
        </div>
    {/if}
    {#if batch.end_time}
        <div class="start_time text-gray-500">
            Finished <Time
                live
                relative
                timestamp={batch.end_time}
                format="dddd @ h:mm A · MMMM D, YYYY"
            />
        </div>
    {/if}
</li>

<style>
    li.batch {
        background-color: #e4e4e4;
        padding: 0.6em;
        flex-grow: 1;
    }
    li.batch .hostos_node_batch_state div {
        min-width: 1.8em;
        float: left;
        margin-top: -2px;
        text-align: left;
    }
    .start_time,
    .node_count {
        text-align: right;
    }
</style>
