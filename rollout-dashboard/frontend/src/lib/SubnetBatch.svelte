<script lang="ts">
    import { A } from "flowbite-svelte";
    import Time from "svelte-time";
    import { copy } from "svelte-copy";
    import { toast } from "@zerodevx/svelte-toast";
    import { selectTextOnFocus } from "./lib";
    import {
        type GuestOsBatch,
        subnetStateComment,
        subnetStateIcon,
    } from "./types";
    import { cap } from "./lib";
    import ClipboardIcon from "./ClipboardIcon.svelte";
    interface Props {
        batch_num: String;
        batch: GuestOsBatch;
    }

    let { batch_num, batch }: Props = $props();
</script>

<li class="rounded-lg batch batch-{batch_num}">
    {#each batch.subnets as subnet}
        <ul>
            <li class="subnet">
                <a
                    rel="external"
                    href={subnet.display_url || ""}
                    target="_blank"
                    class="subnet_state"
                    data-sveltekit-preload-data="off"
                    title={cap(subnetStateComment(subnet))}
                >
                    <div class="subnet_state_icon">
                        {subnetStateIcon(subnet)}
                    </div></a
                >
                <a
                    class="subnet_id text-secondary-600 hover:underline"
                    use:selectTextOnFocus
                    href="https://dashboard.internetcomputer.org/subnet/{subnet.subnet_id}"
                    target="_blank">{subnet.subnet_id}</a
                >
                <div
                    class="git_revision"
                    role="link"
                    tabindex="0"
                    use:copy={{
                        text: subnet.git_revision,
                        onCopy({ text, event }) {
                            toast.push("Copied git revision to clipboard");
                        },
                    }}
                    use:selectTextOnFocus
                >
                    <div class="icon"><ClipboardIcon /></div>
                    {subnet.git_revision}
                </div>
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
    li.subnet {
        display: grid;
        width: 100%;
        justify-items: stretch;
        padding-left: 0;
        margin-left: 0;
        grid-template-columns: min-content 1fr min-content;
        column-gap: 0.6em;
    }
    .subnet_state {
        margin-top: -3px;
    }
    .subnet_id {
        align-self: center;
        max-width: 3em;
        overflow-x: hidden;
        text-wrap: nowrap;
        text-overflow: hidden;
        font-family: monospace;
        font-size: 120%;
    }
    .subnet_id a {
        text-decoration: underline;
        text-decoration-style: dotted;
        text-decoration-color: #e4e4e4;
    }
    .git_revision {
        align-self: center;
        color: #999;
        max-width: 5em;
        text-overflow: ellipsis;
        overflow-x: hidden;
        font-family: monospace;
        font-size: 120%;
        text-align: right;
    }
    .git_revision {
        cursor: copy;
    }
    .git_revision .icon {
        display: none;
    }
    .git_revision:hover .icon {
        display: block;
        position: absolute;
        margin-left: -1.1em;
    }
    .time {
        text-align: right;
        text-align: nowrap; /* prevent breaking spaces */
    }
</style>
