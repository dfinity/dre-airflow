<script lang="ts">
    import { type HostOsBatchDetail } from "./types";
    import { Table } from "@flowbite-svelte-plugins/datatable";
    import { Tabs, TabItem } from "flowbite-svelte";

    interface Props {
        batch: HostOsBatchDetail;
    }

    let { batch }: Props = $props();

    let planned_items = batch.planned_nodes.map((val) => ({
        Node: val.node_id,
        Provider: val.node_provider_id,
        DC: val.dc_id,
        Status: val.status,
        Subnet: val.subnet_id === null ? "—" : val.subnet_id,
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
                  "Upgraded?": getUpgradeStatus(val.node_id),
                  "Alerting?": getAlertStatus(val.node_id),
              }))
            : null;

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
                select: [4],
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

<Tabs tabStyle="underline">
    {#if actual_items !== null}
        <TabItem open title="Actual nodes">
            <Table items={actual_items} dataTableOptions={options} />
        </TabItem>
        <TabItem title="Planned nodes">
            <Table items={planned_items} dataTableOptions={options} />
        </TabItem>
    {:else}
        <TabItem open title="Planned nodes">
            <Table items={planned_items} dataTableOptions={options} />
        </TabItem>
    {/if}
</Tabs>
