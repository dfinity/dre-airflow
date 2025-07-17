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
    {#if batch.actual_nodes !== null && JSON.stringify(batch.actual_nodes) != JSON.stringify(batch.planned_nodes)}<!-- planned and actual are different -->
        <div class="nodes">
            {batch.planned_nodes.length} nodes planned
        </div>
        <div class="nodes">
            {batch.actual_nodes.length} nodes targeted
        </div>
    {:else if batch.actual_nodes !== null && batch.actual_nodes.length > 0}<!-- planned and actual are same nodes -->
        <div class="nodes">
            {batch.actual_nodes.length} nodes targeted
        </div>
    {:else}<!-- no actual nodes yet -->
        <div class="nodes">
            {batch.planned_nodes.length} nodes planned
        </div>
    {/if}
    <div class="time text-gray-500">
        Planned <Time
            live
            relative
            timestamp={batch.planned_start_time}
            format="dddd @ h:mm A · MMMM D, YYYY"
        />
    </div>
    {#if batch.actual_start_time}
        <div class="time text-gray-500">
            Started <Time
                live
                relative
                timestamp={batch.actual_start_time}
                format="dddd @ h:mm A · MMMM D, YYYY"
            />
        </div>
    {/if}
    {#if batch.end_time}
        <div class="time text-gray-500">
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
    li.batch a.hostos_node_batch_state div {
        min-width: 1.8em;
        float: left;
        margin-top: -2px;
        text-align: left;
    }
    .time,
    .nodes {
        text-align: right;
        white-space: nowrap; /* prevent breaking spaces */
    }
</style>
