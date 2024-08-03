<script lang="ts">
    import Time from "svelte-time";
    import { copy } from "svelte-copy";
    import { toast } from "@zerodevx/svelte-toast";
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
                <span
                    class="subnet_id"
                    use:copy={subnet.subnet_id}
                    on:svelte-copy={(event) =>
                        toast.push("Copied subnet ID to clipboard")}
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
                    </svg>{subnet.subnet_id.substring(0, 5)}
                </span>
                <span
                    class="git_revision"
                    use:copy={subnet.git_revision}
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
                    </svg>{subnet.git_revision}</span
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
        display: grid;
        width: 100%;
        justify-items: stretch;
        padding-left: 0;
        margin-left: 0;
        grid-template-columns: min-content 1fr min-content;
        column-gap: 0.6em;
    }
    .subnet_id {
        display: block;
        font-family: monospace;
        font-size: 120%;
    }
    .git_revision {
        display: block;
        color: #999;
        max-width: 5em;
        text-overflow: ellipsis;
        overflow-x: hidden;
        font-family: monospace;
        font-size: 120%;
        text-align: right;
    }
    .subnet_id,
    .git_revision {
        cursor: copy;
    }
    .subnet_id svg,
    .git_revision svg {
        display: none;
    }
    .subnet_id:hover svg {
        display: block;
        position: absolute;
        margin-left: 3.5em;
        margin-top: 0.35em;
    }
    .git_revision:hover svg {
        display: block;
        position: absolute;
        margin-left: -1em;
        margin-top: 0.35em;
    }
    .start_time {
        text-align: right;
    }
</style>
