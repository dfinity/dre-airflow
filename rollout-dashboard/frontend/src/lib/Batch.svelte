<script lang="ts">
    import Time from "svelte-time";
    import { type Batch, batchStateName, batchStateIcon } from "./types";
    export let batch_num: String;
    export let batch: Batch;
</script>

<li class="rounded-lg border batch batch-{batch_num}">
    {#each batch.subnets as subnet}
        <ul>
            <li class="subnet">
                <div class="subnet_state_icon tooltip">
                    {batchStateIcon(subnet.state)}<span
                        class="subnet_state tooltiptext"
                        >{batchStateName(subnet.state)}{#if subnet.comment}<br
                            />{subnet.comment}{/if}</span
                    >
                </div>
                <span class="subnet_id">{subnet.subnet_id.substring(0, 5)}</span
                >
                <span class="git_revision"
                    >{subnet.git_revision.substring(0, 8)}</span
                >
            </li>
        </ul>
    {/each}
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
    ul {
        display: flex;
        flex-direction: row;
        flex-wrap: wrap;
        margin: 0;
        padding: 0;
        list-style-type: none;
    }
    li.subnet {
        padding-left: 0;
        margin-left: 0;
    }
    .subnet_id {
        width: 4rem;
        display: inline-block;
        font-family: monospace;
        font-size: 120%;
    }
    .git_revision {
        color: #999;
    }
    .start_time {
        text-align: right;
    }
</style>
