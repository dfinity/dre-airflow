<script lang="ts">
    import {
        hostOsBatchStateIcon,
        type HostOsBatchResponse,
        formatSelectors,
    } from "./types";
    import {
        Table as RegularTable,
        TableBody,
        TableBodyCell,
        TableBodyRow,
        TableHead,
        TableHeadCell,
    } from "flowbite-svelte";
    import { Tabs, TabItem } from "flowbite-svelte";
    import { hostOsBatchStateName } from "./types";
    import { cap } from "./lib";
    import InfoBlock from "./InfoBlock.svelte";
    import ExternalLinkIcon from "./ExternalLinkIcon.svelte";

    interface Props {
        dag_run_id: string;
        batch: HostOsBatchResponse;
    }

    function getUpgradeStatus(node_id: string): string {
        if (batch.upgraded_nodes !== null) {
            if (batch.upgraded_nodes[node_id]) {
                return batch.upgraded_nodes[node_id];
            } else {
                return "—";
            }
        } else {
            return "—";
        }
    }

    function getAlertStatus(node_id: string): string {
        if (batch.alerting_nodes !== null) {
            if (batch.alerting_nodes[node_id]) {
                return batch.alerting_nodes[node_id];
            } else {
                return "—";
            }
        } else {
            return "—";
        }
    }

    function reducer(akku: Record<string, number>, val: string) {
        let old_count = akku[val];
        if (old_count === undefined) {
            akku[val] = 1;
        } else {
            akku[val] = old_count + 1;
        }
        return akku;
    }

    let { dag_run_id, batch }: Props = $props();

    let planned_items = $derived(
        batch.planned_nodes.map((val) => ({
            Node: val.node_id,
            Provider: val.node_provider_id,
            DC: val.dc_id,
            Assignment:
                val.assignment === null
                    ? "—"
                    : val.assignment === "API boundary"
                      ? "API boundary"
                      : val.assignment,
            Status: val.status,
        })),
    );
    let actual_items = $derived(
        batch.actual_nodes !== null
            ? batch.actual_nodes.map((val) => ({
                  Node: val.node_id,
                  Provider: val.node_provider_id,
                  DC: val.dc_id,
                  Assignment:
                      val.assignment === null
                          ? "—"
                          : val.assignment === "API boundary"
                            ? "API boundary"
                            : val.assignment,
                  "Upgraded?": getUpgradeStatus(val.node_id),
                  "Alerting?": getAlertStatus(val.node_id),
              }))
            : null,
    );

    let upgraded_nodes_summary = $derived(
        batch.upgraded_nodes === null
            ? null
            : Object.entries(batch.upgraded_nodes)
                  .map(([k, v]) => v)
                  .reduce(reducer, {}),
    );

    let alerting_nodes_summary = $derived(
        batch.alerting_nodes === null
            ? null
            : Object.entries(batch.alerting_nodes)
                  .map(([k, v]) => v)
                  .reduce(reducer, {}),
    );
</script>

<div>
    <Tabs tabStyle="underline">
        <TabItem open title="Batch information">
            <RegularTable striped={true}>
                <TableBodyRow
                    ><TableHeadCell>Part of rollout</TableHeadCell
                    ><TableBodyCell>{dag_run_id}</TableBodyCell></TableBodyRow
                >
                <TableBodyRow
                    ><TableHeadCell>Stage</TableHeadCell><TableBodyCell
                        >{cap(batch.stage)}</TableBodyCell
                    ></TableBodyRow
                >
                <TableBodyRow
                    ><TableHeadCell>Planned at</TableHeadCell><TableBodyCell
                        >{batch.planned_start_time.toString()}</TableBodyCell
                    ></TableBodyRow
                >
                {#if batch.actual_start_time !== null}
                    <TableBodyRow
                        ><TableHeadCell>Started at</TableHeadCell><TableBodyCell
                            >{batch.actual_start_time.toString()}</TableBodyCell
                        ></TableBodyRow
                    >
                {/if}
                {#if batch.end_time !== null}
                    <TableBodyRow
                        ><TableHeadCell>Started at</TableHeadCell><TableBodyCell
                            >{batch.end_time.toString()}</TableBodyCell
                        ></TableBodyRow
                    >
                {/if}
                <TableBodyRow
                    ><TableHeadCell>State</TableHeadCell><TableBodyCell
                        >{#if batch.display_url}<a
                                rel="external"
                                title="Open Airflow logs for the most recently updated task"
                                href={batch.display_url || ""}
                                target="_blank"
                                class="text-secondary-600"
                                data-sveltekit-preload-data="off"
                                >{hostOsBatchStateIcon(batch)}
                                {cap(
                                    hostOsBatchStateName(batch),
                                )}<ExternalLinkIcon /></a
                            >{:else}{hostOsBatchStateIcon(batch)}
                            {cap(
                                hostOsBatchStateName(batch),
                            )}{/if}</TableBodyCell
                    ></TableBodyRow
                >
                {#if batch.comment !== null && batch.comment !== ""}
                    <TableBodyRow
                        ><TableHeadCell>Comment</TableHeadCell><TableBodyCell
                            >{batch.comment}</TableBodyCell
                        ></TableBodyRow
                    >
                {/if}
                <TableBodyRow
                    ><TableHeadCell>Selectors</TableHeadCell>
                    <TableBodyCell
                        >{formatSelectors(batch.selectors)}
                    </TableBodyCell>
                </TableBodyRow>
                <TableBodyRow
                    ><TableHeadCell>Targets</TableHeadCell>
                    {#if actual_items !== null}
                        <TableBodyCell
                            >{planned_items.length} nodes planned, {actual_items.length}
                            actually targeted</TableBodyCell
                        >
                    {:else}
                        <TableBodyCell
                            >{planned_items.length} nodes planned</TableBodyCell
                        >
                    {/if}</TableBodyRow
                >
                {#if upgraded_nodes_summary}
                    <TableBodyRow
                        ><TableHeadCell>Upgrade status</TableHeadCell
                        ><TableBodyCell
                            >{Object.entries(upgraded_nodes_summary)
                                .map(([k, v]) => `${v} nodes ${k}`)
                                .join(", ")}</TableBodyCell
                        ></TableBodyRow
                    >
                {/if}
                {#if alerting_nodes_summary}
                    <TableBodyRow
                        ><TableHeadCell>Health status</TableHeadCell
                        ><TableBodyCell
                            >{Object.entries(alerting_nodes_summary)
                                .map(([k, v]) => `${v} nodes ${k}`)
                                .join(", ")}</TableBodyCell
                        ></TableBodyRow
                    >
                {/if}
            </RegularTable>
        </TabItem>
        {#if actual_items !== null}
            <TabItem title="Actual nodes">
                <InfoBlock>
                    These are the nodes selected at the start of the batch just
                    prior to submitting the upgrade proposal. They may differ
                    from the planned nodes, originally selected at the beginning
                    of the rollout.
                </InfoBlock>
                <RegularTable striped={true}>
                    <TableHead>
                        <TableHeadCell>Node</TableHeadCell>
                        <TableHeadCell>Provider</TableHeadCell>
                        <TableHeadCell>DC</TableHeadCell>
                        <TableHeadCell>Upgraded?</TableHeadCell>
                        <TableHeadCell>Alerting?</TableHeadCell>
                    </TableHead>
                    <TableBody>
                        {#each actual_items as node}
                            <TableBodyRow>
                                <TableBodyCell class="oneliner"
                                    ><a
                                        class="text-secondary-600"
                                        target="_blank"
                                        href="https://dashboard.internetcomputer.org/network/nodes/{node[
                                            'Node'
                                        ]}">{node["Node"]}</a
                                    ></TableBodyCell
                                >
                                <TableBodyCell class="oneliner"
                                    ><a
                                        class="text-secondary-600"
                                        target="_blank"
                                        href="https://dashboard.internetcomputer.org/network/providers/{node[
                                            'Provider'
                                        ]}">{node["Provider"]}</a
                                    ></TableBodyCell
                                >
                                <TableBodyCell class="oneliner"
                                    ><a
                                        class=" text-secondary-600"
                                        target="_blank"
                                        href="https://dashboard.internetcomputer.org/network/centers/{node[
                                            'DC'
                                        ]}">{node["DC"]}</a
                                    ></TableBodyCell
                                >
                                <TableBodyCell
                                    >{node["Upgraded?"]}</TableBodyCell
                                >
                                <TableBodyCell
                                    >{node["Alerting?"]}</TableBodyCell
                                >
                            </TableBodyRow>
                        {/each}
                    </TableBody>
                </RegularTable>
            </TabItem>
        {/if}
        <TabItem title="Planned nodes">
            <InfoBlock>
                These are the nodes originally planned to be rolled out at the
                beginning of the rollout. They may differ from the nodes
                actually targeted once this batch has begun to do work. The
                status per node shown here corresponds to the status of the node
                at the beginning of the rollout.
            </InfoBlock>
            <RegularTable striped={true}>
                <TableHead>
                    <TableHeadCell>Node</TableHeadCell>
                    <TableHeadCell>Provider</TableHeadCell>
                    <TableHeadCell>DC</TableHeadCell>
                    <TableHeadCell>Assignment</TableHeadCell>
                    <TableHeadCell>Status</TableHeadCell>
                </TableHead>
                <TableBody>
                    {#each planned_items as node}
                        <TableBodyRow>
                            <TableBodyCell class="oneliner"
                                ><a
                                    class="text-secondary-600"
                                    target="_blank"
                                    href="https://dashboard.internetcomputer.org/network/nodes/{node[
                                        'Node'
                                    ]}">{node["Node"]}</a
                                ></TableBodyCell
                            >
                            <TableBodyCell class="oneliner"
                                ><a
                                    class="text-secondary-600"
                                    target="_blank"
                                    href="https://dashboard.internetcomputer.org/network/providers/{node[
                                        'Provider'
                                    ]}">{node["Provider"]}</a
                                ></TableBodyCell
                            >
                            <TableBodyCell class="oneliner"
                                ><a
                                    class=" text-secondary-600"
                                    target="_blank"
                                    href="https://dashboard.internetcomputer.org/network/centers/{node[
                                        'DC'
                                    ]}">{node["DC"]}</a
                                ></TableBodyCell
                            >
                            <TableBodyCell
                                >{#if node["Assignment"] != "—" && node["Assignment"] != "API boundary"}<a
                                        class="oneliner text-secondary-600"
                                        target="_blank"
                                        href="https://dashboard.internetcomputer.org/network/subnets/${node[
                                            'Assignment'
                                        ]}">{node["Assignment"].split("-")}</a
                                    >{:else}{node["Assignment"].split(
                                        "-",
                                    )}{/if}</TableBodyCell
                            >
                            <TableBodyCell>{node["Status"]}</TableBodyCell>
                        </TableBodyRow>
                    {/each}
                </TableBody>
            </RegularTable>
        </TabItem>
    </Tabs>
</div>
