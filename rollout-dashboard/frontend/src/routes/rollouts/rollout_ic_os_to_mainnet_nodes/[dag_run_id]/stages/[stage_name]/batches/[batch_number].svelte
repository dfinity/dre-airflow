<script lang="ts">
    import { params } from "@roxi/routify";
    import HostOsBatchDetail from "../../../../../../../lib/HostOSBatchDetail.svelte";
    import {
        type HostOsBatchResponse,
        type Error,
    } from "../../../../../../../lib/types";
    import { cap } from "../../../../../../../../src/lib/lib";
    import { Heading } from "flowbite-svelte";
    import { Navbar, NavLi, NavUl, NavBrand } from "flowbite-svelte";
    import { AngleLeftOutline } from "flowbite-svelte-icons";

    let dag_run_id = $params.dag_run_id;
    let stage_name = $params.stage_name;
    let batch_number = $params.batch_number;

    import { batch_view_with_cancellation } from "../../../../../../../lib/stores";
    import { onDestroy } from "svelte";
    import LoadingBlock from "../../../../../../../lib/LoadingBlock.svelte";
    import ErrorBlock from "../../../../../../../lib/ErrorBlock.svelte";

    let [batch, cancel] = batch_view_with_cancellation(
        dag_run_id,
        stage_name,
        batch_number,
    );

    onDestroy(() => {
        cancel();
    });
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
        <NavUl>
            <NavLi href="javascript:window.history.back();"
                ><AngleLeftOutline
                    class="text-primary-800 dark:text-white inline"
                />Go back to main screen</NavLi
            >
        </NavUl>
    </Navbar>
</div>

<Heading>{cap(stage_name)} batch {batch_number}</Heading>

{#if ($batch as Error).code !== undefined}
    {#if ($batch as Error).code === 204}
        <LoadingBlock />
    {:else}
        <!-- note use of me-3 in svg icon to ensure icon actually shows not too stuck to the text -->
        <ErrorBlock>
            <span class="font-medium">Cannot retrieve batch data:</span>
            {($batch as Error).message}
        </ErrorBlock>
    {/if}
{:else}
    <HostOsBatchDetail {dag_run_id} batch={$batch as HostOsBatchResponse}
    ></HostOsBatchDetail>
{/if}
