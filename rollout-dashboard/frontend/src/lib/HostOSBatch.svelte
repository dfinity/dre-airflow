<script lang="ts">
    import { A } from "flowbite-svelte";
    import Time from "svelte-time";
    import {
        type HostOsBatch,
        hostOsBatchStateComment,
        hostOsBatchStateIcon,
    } from "./types";
    import { cap } from "./lib";
    import { url } from "@roxi/routify";
    interface Props {
        dag_run_id: string;
        stage_name: string;
        batch_number: string;
        batch: HostOsBatch;
    }

    let { dag_run_id, stage_name, batch_number, batch }: Props = $props();

    const detailsUrl = $url(
        "../rollouts/rollout_ic_os_to_mainnet_nodes/[dag_run_id]/stages/[stage_name]/batches/[batch_number]",
        {
            dag_run_id: dag_run_id,
            stage_name: stage_name,
            batch_number: batch_number,
        },
    );
</script>

<li class="rounded-lg batch batch-{batch_number}">
    <div class="hostos_node_batch_state">
        <a
            rel="external"
            href={batch.display_url || ""}
            target="_blank"
            data-sveltekit-preload-data="off"
            title={cap(hostOsBatchStateComment(batch))}
        >
            <div class="hostos_node_batch_state_icon">
                {hostOsBatchStateIcon(batch)}
            </div></a
        >
        <div>
            {#if batch.actual_nodes !== null && JSON.stringify(batch.actual_nodes) != JSON.stringify(batch.planned_nodes)}<!-- planned and actual are different -->
                <div class="nodes">
                    <!-- planned and actual differ -->
                    {batch.planned_nodes.length} nodes planned
                </div>
                <div class="nodes">
                    <A class="text-secondary-600" href={detailsUrl}
                        >{batch.actual_nodes.length} nodes</A
                    > targeted
                </div>
            {:else if batch.actual_nodes !== null && batch.actual_nodes.length > 0}
                <div class="nodes">
                    <!-- planned and actual are same nodes -->
                    <A class="text-secondary-600" href={detailsUrl}
                        >{batch.actual_nodes.length} nodes</A
                    > targeted
                </div>
            {:else}
                <div class="nodes">
                    <!-- no actual nodes yet -->
                    <A class="text-secondary-600" href={detailsUrl}
                        >{batch.planned_nodes.length} nodes</A
                    > planned
                </div>
            {/if}
        </div>
    </div>
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
    .hostos_node_batch_state {
        display: grid;
        grid-template-columns: min-content auto;
    }
    li.batch .hostos_node_batch_state a {
        min-width: 1.8em;
    }
    .time,
    .nodes {
        text-align: right;
        white-space: nowrap; /* prevent breaking spaces */
    }
</style>
