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
        <div
            class="flex items-center justify-center w-56 h-56 border border-gray-200 rounded-lg bg-gray-50 dark:bg-gray-800 dark:border-gray-700"
            style="margin-left: auto; margin-right: auto;"
        >
            <div role="status">
                <svg
                    aria-hidden="true"
                    class="w-8 h-8 text-gray-200 animate-spin dark:text-gray-600 fill-blue-600"
                    viewBox="0 0 100 101"
                    fill="none"
                    xmlns="http://www.w3.org/2000/svg"
                    ><path
                        d="M100 50.5908C100 78.2051 77.6142 100.591 50 100.591C22.3858 100.591 0 78.2051 0 50.5908C0 22.9766 22.3858 0.59082 50 0.59082C77.6142 0.59082 100 22.9766 100 50.5908ZM9.08144 50.5908C9.08144 73.1895 27.4013 91.5094 50 91.5094C72.5987 91.5094 90.9186 73.1895 90.9186 50.5908C90.9186 27.9921 72.5987 9.67226 50 9.67226C27.4013 9.67226 9.08144 27.9921 9.08144 50.5908Z"
                        fill="currentColor"
                    /><path
                        d="M93.9676 39.0409C96.393 38.4038 97.8624 35.9116 97.0079 33.5539C95.2932 28.8227 92.871 24.3692 89.8167 20.348C85.8452 15.1192 80.8826 10.7238 75.2124 7.41289C69.5422 4.10194 63.2754 1.94025 56.7698 1.05124C51.7666 0.367541 46.6976 0.446843 41.7345 1.27873C39.2613 1.69328 37.813 4.19778 38.4501 6.62326C39.0873 9.04874 41.5694 10.4717 44.0505 10.1071C47.8511 9.54855 51.7191 9.52689 55.5402 10.0491C60.8642 10.7766 65.9928 12.5457 70.6331 15.2552C75.2735 17.9648 79.3347 21.5619 82.5849 25.841C84.9175 28.9121 86.7997 32.2913 88.1811 35.8758C89.083 38.2158 91.5421 39.6781 93.9676 39.0409Z"
                        fill="currentFill"
                    /></svg
                >
                <span class="sr-only">Loading...</span>
            </div>
        </div>
    {:else}
        <!-- note use of me-3 in svg icon to ensure icon actually shows not too stuck to the text -->
        <div
            class="flex items-center p-4 mb-4 text-sm text-red-800 border border-red-300 rounded-lg bg-red-50 dark:bg-gray-800 dark:text-red-400 dark:border-red-800"
            role="alert"
        >
            <svg
                class="w-6 h-6 me-3 text-gray-800 dark:text-white"
                aria-hidden="true"
                xmlns="http://www.w3.org/2000/svg"
                width="24"
                height="24"
                fill="currentColor"
                viewBox="0 0 24 24"
            >
                <path
                    fill-rule="evenodd"
                    d="M2 12C2 6.477 6.477 2 12 2s10 4.477 10 10-4.477 10-10 10S2 17.523 2 12Zm11-4a1 1 0 1 0-2 0v5a1 1 0 1 0 2 0V8Zm-1 7a1 1 0 1 0 0 2h.01a1 1 0 1 0 0-2H12Z"
                    clip-rule="evenodd"
                />
            </svg>
            <span class="sr-only">Error</span>
            <div>
                <span class="font-medium">Cannot retrieve batch data:</span>
                {($batch as Error).message}
            </div>
        </div>
    {/if}
{:else}
    <HostOsBatchDetail {dag_run_id} batch={$batch as HostOsBatchResponse}
    ></HostOsBatchDetail>
{/if}
