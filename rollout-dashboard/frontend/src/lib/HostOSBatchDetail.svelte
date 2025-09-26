<script lang="ts">
    import {
        hostOsBatchStateIcon,
        type AlertStatus,
        type UpgradeStatus,
    } from "./types";
    import {
        Popover,
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
    import WarningBlock from "./WarningBlock.svelte";
    import ExternalLinkIcon from "./ExternalLinkIcon.svelte";
    import Selectors from "./Selectors.svelte";
    import { batch_view_with_cancellation, type FullState } from "./stores";
    import { onDestroy } from "svelte";
    import { Heading, NavHamburger } from "flowbite-svelte";
    import { Navbar, NavLi, NavUl, NavBrand } from "flowbite-svelte";
    import { AngleLeftOutline, GridOutline } from "flowbite-svelte-icons";
    import LoadingBlock from "./LoadingBlock.svelte";
    import ErrorBlock from "./ErrorBlock.svelte";
    import HostOsRollout from "./HostOSRollout.svelte";
    import type { Writable } from "svelte/store";

    function reducer(akku: Record<string, number>, val: string) {
        let old_count = akku[val];
        if (old_count === undefined) {
            akku[val] = 1;
        } else {
            akku[val] = old_count + 1;
        }
        return akku;
    }

    interface Props {
        dag_run_id: string;
        stage_name: string;
        batch_number: number;
        rollouts_view: Writable<FullState>;
    }

    let { dag_run_id, stage_name, batch_number, rollouts_view }: Props =
        $props();

    let [batch, cancel] = batch_view_with_cancellation(
        dag_run_id,
        stage_name,
        batch_number,
    );

    onDestroy(() => {
        cancel();
    });

    function getUpgradeStatus(
        upgraded_nodes: { [key: string]: UpgradeStatus } | null,
        node_id: string,
    ): string {
        if (upgraded_nodes !== null) {
            if (upgraded_nodes[node_id]) {
                return upgraded_nodes[node_id];
            } else {
                return "—";
            }
        } else {
            return "—";
        }
    }

    function getAlertStatus(
        alerting_nodes: { [key: string]: AlertStatus } | null,
        node_id: string,
    ): string {
        if (alerting_nodes !== null) {
            if (alerting_nodes[node_id]) {
                return alerting_nodes[node_id];
            } else {
                return "—";
            }
        } else {
            return "—";
        }
    }

    let planned_items = $derived(
        "state" in $batch
            ? $batch.planned_nodes.map((val) => ({
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
              }))
            : null,
    );
    let actual_items = $derived(
        "state" in $batch && $batch.actual_nodes !== null
            ? $batch.actual_nodes.map((val) => ({
                  Node: val.node_id,
                  Provider: val.node_provider_id,
                  DC: val.dc_id,
                  Assignment:
                      val.assignment === null
                          ? "—"
                          : val.assignment === "API boundary"
                            ? "API boundary"
                            : val.assignment,
                  "Upgraded?": getUpgradeStatus(
                      $batch.upgraded_nodes,
                      val.node_id,
                  ),
                  "Alerting?": getAlertStatus(
                      $batch.alerting_nodes,
                      val.node_id,
                  ),
              }))
            : null,
    );

    let upgraded_nodes_summary = $derived(
        "state" in $batch && $batch.upgraded_nodes !== null
            ? Object.entries($batch.upgraded_nodes)
                  .map(([k, v]) => v)
                  .reduce(reducer, {})
            : null,
    );

    let alerting_nodes_summary = $derived(
        "state" in $batch && $batch.alerting_nodes !== null
            ? Object.entries($batch.alerting_nodes)
                  .map(([k, v]) => v)
                  .reduce(reducer, {})
            : null,
    );
</script>

<div id="header">
    <Navbar>
        <NavBrand href="/">
            <img
                src="/favicon-512x512.png"
                class="me-3 h-6 sm:h-9"
                alt="IC Logo"
            />
            <span
                class="self-center whitespace-nowrap text-xl font-semibold dark:text-white"
                >Rollouts</span
            >
        </NavBrand>
        <NavHamburger />
        <NavUl>
            <NavLi href="javascript:window.history.back();"
                ><AngleLeftOutline
                    class="text-primary-800 dark:text-white inline"
                />Back</NavLi
            >
            <NavLi href="javascript:window.history.back();"
                >Other batches <GridOutline
                    class="text-primary-800 dark:text-white inline"
                /></NavLi
            >

            <Popover class="cursor-pointer w-4/5">
                <div>
                    {#each $rollouts_view.rollouts as rollout}
                        {#if rollout.name == dag_run_id && rollout.kind === "rollout_ic_os_to_mainnet_nodes"}
                            <HostOsRollout
                                {rollout}
                                paused={$rollouts_view.rollout_engine_states[
                                    "rollout_ic_os_to_mainnet_nodes"
                                ] === "paused"}
                                {rollouts_view}
                            />
                        {/if}
                    {/each}
                </div>
            </Popover>
        </NavUl>
    </Navbar>
</div>

{#if "code" in $batch}
    {#if $batch.code === 204}
        <LoadingBlock />
    {:else}
        <ErrorBlock>
            <span class="font-medium">Cannot retrieve batch data:</span>
            {$batch.message}
        </ErrorBlock>
    {/if}
{:else}
    {#key $batch}
        <Heading>{cap($batch.stage)} batch {$batch.batch_number}</Heading>
        <div>
            <Tabs tabStyle="underline">
                <TabItem open title="Batch information">
                    <RegularTable striped={true}>
                        <TableBodyRow
                            ><TableHeadCell>Part of rollout</TableHeadCell
                            ><TableBodyCell style="white-space: normal"
                                >{dag_run_id}</TableBodyCell
                            ></TableBodyRow
                        >
                        <TableBodyRow
                            ><TableHeadCell>Stage</TableHeadCell><TableBodyCell
                                style="white-space: normal"
                                >{cap($batch.stage)}</TableBodyCell
                            ></TableBodyRow
                        >
                        <TableBodyRow
                            ><TableHeadCell>Planned at</TableHeadCell
                            ><TableBodyCell style="white-space: normal"
                                >{$batch.planned_start_time.toString()}</TableBodyCell
                            ></TableBodyRow
                        >
                        {#if $batch.actual_start_time !== null}
                            <TableBodyRow
                                ><TableHeadCell>Started at</TableHeadCell
                                ><TableBodyCell style="white-space: normal"
                                    >{$batch.actual_start_time.toString()}</TableBodyCell
                                ></TableBodyRow
                            >
                        {/if}
                        {#if $batch.end_time !== null}
                            <TableBodyRow
                                ><TableHeadCell>Started at</TableHeadCell
                                ><TableBodyCell style="white-space: normal"
                                    >{$batch.end_time.toString()}</TableBodyCell
                                ></TableBodyRow
                            >
                        {/if}
                        <TableBodyRow
                            ><TableHeadCell>State</TableHeadCell><TableBodyCell
                                style="white-space: normal"
                                >{#if $batch.display_url}<a
                                        rel="external"
                                        title="Open Airflow logs for the most recently updated task"
                                        href={$batch.display_url || ""}
                                        target="_blank"
                                        class="text-secondary-600"
                                        data-sveltekit-preload-data="off"
                                        >{hostOsBatchStateIcon($batch)}
                                        {cap(
                                            hostOsBatchStateName($batch),
                                        )}<ExternalLinkIcon /></a
                                    >{:else}{hostOsBatchStateIcon($batch)}
                                    {cap(
                                        hostOsBatchStateName($batch),
                                    )}{/if}</TableBodyCell
                            ></TableBodyRow
                        >
                        {#if $batch.comment !== null && $batch.comment !== ""}
                            <TableBodyRow
                                ><TableHeadCell>Comment</TableHeadCell
                                ><TableBodyCell style="white-space: normal"
                                    >{$batch.comment}</TableBodyCell
                                ></TableBodyRow
                            >
                        {/if}
                        <TableBodyRow
                            ><TableHeadCell>Selectors</TableHeadCell>
                            <TableBodyCell style="white-space: normal">
                                <div
                                    style="display: flex; flex-grow: 0; justify-content: flex-start;"
                                >
                                    <Selectors selectors={$batch.selectors} />
                                </div>
                            </TableBodyCell>
                        </TableBodyRow>
                        <TableBodyRow
                            ><TableHeadCell>Targets</TableHeadCell>
                            <TableBodyCell style="white-space: normal">
                                {#if actual_items !== null && planned_items !== null}
                                    {planned_items.length} nodes planned, {actual_items.length}
                                    actually targeted.
                                {:else if planned_items !== null}
                                    {planned_items.length} nodes planned.
                                {/if}<br /><br
                                />{#if $batch.tolerance === null}All targets are
                                    required to upgrade successfully and exhibit
                                    zero alerts.{:else}At most {$batch.tolerance}
                                    {#if typeof $batch.tolerance == "string"}of
                                    {/if} targets may fail to upgrade or remain in
                                    alerting state.{/if}</TableBodyCell
                            ></TableBodyRow
                        >
                        {#if upgraded_nodes_summary || $batch.adoption_checks_bypassed}
                            <TableBodyRow
                                ><TableHeadCell
                                    ><span
                                        class="tooltip"
                                        title="This information is only updated during the wait for revision adoption"
                                        >Upgrade status</span
                                    ></TableHeadCell
                                ><TableBodyCell style="white-space: normal"
                                    >{#if upgraded_nodes_summary}{Object.entries(
                                            upgraded_nodes_summary,
                                        )
                                            .map(([k, v]) => `${v} nodes ${k}`)
                                            .join(
                                                ", ",
                                            )}.{/if}{#if $batch.adoption_checks_bypassed}
                                        <WarningBlock shrink_to_content="true">
                                            The check for upgrades was forcibly
                                            skippped in this batch.
                                        </WarningBlock>
                                    {/if}</TableBodyCell
                                ></TableBodyRow
                            >
                        {/if}
                        {#if alerting_nodes_summary || $batch.health_checks_bypassed}
                            <TableBodyRow
                                ><TableHeadCell
                                    ><span
                                        class="tooltip"
                                        title="This information is only updated during the wait for nodes to return to a healthy status"
                                        >Health status</span
                                    ></TableHeadCell
                                ><TableBodyCell style="white-space: normal"
                                    >{#if alerting_nodes_summary}{Object.entries(
                                            alerting_nodes_summary,
                                        )
                                            .map(([k, v]) => `${v} nodes ${k}`)
                                            .join(
                                                ", ",
                                            )}.{/if}{#if $batch.health_checks_bypassed}
                                        <WarningBlock shrink_to_content="true">
                                            The health check on nodes was
                                            forcibly skippped in this batch.
                                        </WarningBlock>
                                    {/if}</TableBodyCell
                                ></TableBodyRow
                            >
                        {/if}
                    </RegularTable>
                </TabItem>
                {#if actual_items !== null}
                    <TabItem title="Actual nodes">
                        <InfoBlock>
                            Nodes listed here may differ from the planned nodes,
                            due to shifts in node availability over time. The
                            upgrade status information ceases to be updated once
                            the batch has moved on to checking node health, and
                            the health information ceases to update after the
                            batch is finished.
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
                        These are the nodes originally planned to be rolled out
                        at the beginning of the rollout. They may differ from
                        the nodes actually targeted once this batch has begun to
                        do work. The status per node shown here corresponds to
                        the status of the node at the beginning of the rollout.
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
                            {#if planned_items !== null}
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
                                                    href="https://dashboard.internetcomputer.org/network/subnets/{node[
                                                        'Assignment'
                                                    ]}"
                                                    >{node["Assignment"].split(
                                                        "-",
                                                    )[0]}</a
                                                >{:else}{node[
                                                    "Assignment"
                                                ].split(
                                                    "-",
                                                )}{/if}</TableBodyCell
                                        >
                                        <TableBodyCell
                                            >{node["Status"]}</TableBodyCell
                                        >
                                    </TableBodyRow>
                                {/each}
                            {/if}
                        </TableBody>
                    </RegularTable>
                </TabItem>
            </Tabs>
        </div>
    {/key}
{/if}
