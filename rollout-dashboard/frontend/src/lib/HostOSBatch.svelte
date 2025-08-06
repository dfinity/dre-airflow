<script lang="ts">
    import Time from "svelte-time";
    import {
        type HostOsBatch,
        hostOsBatchStateComment,
        hostOsBatchStateIcon,
        formatSelectors,
    } from "./types";
    import { cap } from "./lib";
    interface Props {
        dag_run_id: string;
        stage_name: string;
        batch_number: string;
        batch: HostOsBatch;
    }

    let { dag_run_id, stage_name, batch_number, batch }: Props = $props();

    const detailsUrl = `#/rollouts/rollout_ic_os_to_mainnet_nodes/${encodeURIComponent(dag_run_id)}/stages/${encodeURIComponent(stage_name)}/batches/${encodeURIComponent(batch_number)}`;
</script>

<li class="rounded-lg batch batch-{batch_number}">
    <a href={detailsUrl}>
        <div class="hostos_node_batch_state">
            <div
                title={cap(hostOsBatchStateComment(batch))}
                class="hostos_node_batch_state_icon"
            >
                {hostOsBatchStateIcon(batch)}
            </div>
            <div>
                {#if batch.actual_nodes !== null && JSON.stringify(batch.actual_nodes) != JSON.stringify(batch.planned_nodes)}<!-- planned and actual are different -->
                    <div class="nodes">
                        <!-- planned and actual differ -->
                        <span
                            class="tooltip"
                            title={formatSelectors(batch.selectors)}
                            >{batch.planned_nodes} nodes planned</span
                        >
                    </div>
                    <div class="nodes">
                        {batch.actual_nodes} nodes targeted
                    </div>
                {:else if batch.actual_nodes !== null && batch.actual_nodes > 0}
                    <div class="nodes">
                        <!-- planned and actual are same nodes -->
                        <span
                            class="tooltip"
                            title={formatSelectors(batch.selectors)}
                            >{batch.actual_nodes} nodes targeted</span
                        >
                    </div>
                {:else}
                    <div class="nodes">
                        <!-- no actual nodes yet -->
                        <span
                            class="tooltip"
                            title={formatSelectors(batch.selectors)}
                            >{batch.planned_nodes} nodes planned</span
                        >
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
    </a>
</li>

<style>
    li.batch {
        background-color: #e4e4e4;
        flex-grow: 1;
        display: flex;
    }
    li.batch > a {
        padding: 0.6em;
        flex-grow: 1;
    }
    li.batch > a:hover,
    li.batch > a:focus {
        background-color: var(--color-secondary-200);
        border-radius: var(--radius-lg);
    }
    .hostos_node_batch_state {
        display: grid;
        grid-template-columns: min-content auto;
    }
    li.batch .hostos_node_batch_state .hostos_node_batch_state_icon {
        min-width: 1.8em;
    }
    .time,
    .nodes {
        text-align: right;
        white-space: nowrap; /* prevent breaking spaces */
    }
</style>
