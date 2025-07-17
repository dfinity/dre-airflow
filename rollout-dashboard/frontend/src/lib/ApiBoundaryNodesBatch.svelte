<script lang="ts">
    import Time from "svelte-time";
    import { copy } from "svelte-copy";
    import { toast } from "@zerodevx/svelte-toast";
    import {
        type ApiBoundaryNodesBatch,
        apiBoundaryNodesBatchStateComment,
        apiBoundaryNodesBatchStateIcon,
    } from "./types";
    import { cap, selectTextOnFocus } from "./lib";
    export let batch_num: String;
    export let batch: ApiBoundaryNodesBatch;
</script>

<li class="rounded-lg border batch batch-{batch_num}">
    {#each batch.api_boundary_nodes as node}
        <ul>
            <li class="node">
                <a
                    rel="external"
                    href={batch.display_url || ""}
                    target="_blank"
                    class="api_boundary_node_batch_state"
                    data-sveltekit-preload-data="off"
                    title={cap(apiBoundaryNodesBatchStateComment(batch))}
                >
                    <div class="api_boundary_node_batch_state_icon">
                        {apiBoundaryNodesBatchStateIcon(batch)}
                    </div></a
                >
                <a
                    class="node_id"
                    use:selectTextOnFocus
                    href="https://dashboard.internetcomputer.org/network/nodes/{node.node_id}"
                    target="_blank">{node.node_id}</a
                >
            </li>
        </ul>
    {/each}
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
    ul {
        display: flex;
        flex-direction: row;
        flex-wrap: wrap;
        margin: 0;
        padding: 0;
        list-style-type: none;
    }
    li.node {
        display: grid;
        width: 100%;
        justify-items: stretch;
        padding-left: 0;
        margin-left: 0;
        grid-template-columns: min-content 1fr min-content;
        column-gap: 0.6em;
    }
    .api_boundary_node_batch_state {
        margin-top: -3px;
    }
    .node_id {
        align-self: center;
        max-width: 3em;
        overflow-x: hidden;
        text-wrap: nowrap;
        text-overflow: hidden;
        font-family: monospace;
        font-size: 120%;
    }
    .node_id a {
        text-decoration: underline;
        text-decoration-style: dotted;
        text-decoration-color: #e4e4e4;
    }
    .time {
        text-align: right;
        white-space: nowrap; /* prevent breaking spaces */
    }
</style>
