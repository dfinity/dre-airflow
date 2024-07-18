import { get, writable } from 'svelte/store'

type RolloutResult = {
    error: string | Object | null;
    rollouts: Array<Object>;
}

export const rollout_query = (() => {
    const store = writable<RolloutResult>({ rollouts: [], error: "loading" })

    let updater = async () => {
        const API_URL = import.meta.env.BACKEND_API_PATH || "/api/v1";
        const url = API_URL + "/rollouts"
        console.log("Hitting URL " + url)
        const res = await fetch(url);
        if (res.ok) {
            let json = await res.json()
            store.set({
                rollouts: json,
                error: null
            })
            setTimeout(updater, 15000)
        } else {
            // Sometimes the API will fail!
            // FIXME: we should handle this with an error shown to the user.
            console.log('Request for rollout data failed: ' + res.ok);
            let responseText = await res.text()
            let errorText = "Status " + res.status + " from server"
            if (responseText) {
                responseText = responseText.split("\n")[0]
                errorText = errorText + " (" + responseText + ")"
            }
            store.set({
                rollouts: get(store).rollouts,
                error: errorText
            })
            setTimeout(updater, 15000)
        }
    }
    setTimeout(updater, 1)

    return store
});
