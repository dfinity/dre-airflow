<script lang="ts">
    import { hostOsBatchStateIcon, type HostOsBatchResponse } from "./types";
    import { Table } from "@flowbite-svelte-plugins/datatable";
    import {
        Table as RegularTable,
        TableBodyCell,
        TableBodyRow,
        TableHeadCell,
    } from "flowbite-svelte";
    import { Tabs, TabItem } from "flowbite-svelte";
    import { hostOsBatchStateName } from "./types";
    import { cap } from "./lib";

    interface Props {
        dag_run_id: string;
        batch: HostOsBatchResponse;
    }

    let { dag_run_id, batch }: Props = $props();

    let planned_items = batch.planned_nodes.map((val) => ({
        Node: val.node_id,
        Provider: val.node_provider_id,
        DC: val.dc_id,
        Subnet: val.subnet_id === null ? "—" : val.subnet_id,
        Status: val.status,
    }));

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

    let actual_items =
        batch.actual_nodes !== null
            ? batch.actual_nodes.map((val) => ({
                  Node: val.node_id,
                  Provider: val.node_provider_id,
                  DC: val.dc_id,
                  Subnet: val.subnet_id === null ? "—" : val.subnet_id,
                  "Upgraded?": getUpgradeStatus(val.node_id),
                  "Alerting?": getAlertStatus(val.node_id),
              }))
            : null;

    function reducer(akku: Record<string, number>, val: string) {
        let old_count = akku[val];
        if (old_count === undefined) {
            akku[val] = 1;
        } else {
            akku[val] = old_count + 1;
        }
        return akku;
    }

    let upgraded_nodes_summary =
        batch.upgraded_nodes === null
            ? null
            : Object.entries(batch.upgraded_nodes)
                  .map(([k, v]) => v)
                  .reduce(reducer, {});

    let alerting_nodes_summary =
        batch.alerting_nodes === null
            ? null
            : Object.entries(batch.alerting_nodes)
                  .map(([k, v]) => v)
                  .reduce(reducer, {});

    let options = {
        columns: [
            {
                select: [0],
                type: "string",
                render: function (cellData: string, td: Node) {
                    return `<a class="oneliner text-secondary-600" target="_blank" href="https://dashboard.internetcomputer.org/network/nodes/${cellData}">${cellData}</div>`;
                },
            },
            {
                select: [1],
                type: "string",
                render: function (cellData: string, td: Node) {
                    return `<a class="oneliner text-secondary-600" target="_blank" href="https://dashboard.internetcomputer.org/network/providers/${cellData}">${cellData}</div>`;
                },
            },
            {
                select: [2],
                type: "string",
                render: function (cellData: string, td: Node) {
                    return `<a class="oneliner text-secondary-600" target="_blank" href="https://dashboard.internetcomputer.org/network/centers/${cellData}">${cellData}</div>`;
                },
            },
            {
                select: [3],
                type: "string",
                render: function (cellData: string, td: Node) {
                    if (cellData != "—") {
                        return `<a class="oneliner text-secondary-600" target="_blank" href="https://dashboard.internetcomputer.org/network/subnets/${cellData}">${cellData.split("-")[0]}</div>`;
                    }
                },
            },
        ],
        perPage: 20,
    };
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
                                {cap(hostOsBatchStateName(batch))}</a
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
                <Table items={actual_items} dataTableOptions={options} />
            </TabItem>
            <TabItem title="Planned nodes">
                <Table items={planned_items} dataTableOptions={options} />
            </TabItem>
        {:else}
            <TabItem title="Planned nodes">
                <Table items={planned_items} dataTableOptions={options} />
            </TabItem>
        {/if}
    </Tabs>
</div>
