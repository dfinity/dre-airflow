<script lang="ts">
    import { onMount } from "svelte";
    import { params } from "@roxi/routify";
    // import { Table } from "@flowbite-svelte-plugins/datatable";

    import { type HostOsBatchDetail } from "../../../../../../../lib/types";

    const API_URL =
        import.meta.env.BACKEND_API_PATH_UNSTABLE || "/api/unstable";

    let dag_run_id: HostOsBatchDetail | null = null;

    onMount(async function () {
        const dag_run_id = $params.dag_run_id;
        const stage_name = $params.stage_name;
        const batch_number = $params.batch_number;
        const resource = `${API_URL}/rollouts/rollout_ic_os_to_mainnet_nodes/${encodeURIComponent(dag_run_id)}/stages/${encodeURIComponent(stage_name)}/batches/${encodeURIComponent(batch_number)}`;
        const response = await fetch(resource); // FIXME add typing.
        const data = await response.json();
        console.log(data);
    });
</script>

<p>Hello world! {dag_run_id}</p>
