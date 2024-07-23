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
        const res = await fetch(url);
        if (res.ok) {
            if (res.status == 204) {
                console.log('Data is not yet available.  Retrying soon.')
                store.set({ rollouts: [], error: "loading" })
            } else {
                let json = await res.json()
                store.set({
                    rollouts: json,
                    error: null
                })
            }
            setTimeout(updater, 5000)
        } else {
            // Sometimes the API will fail!
            // FIXME: we should handle this with an error shown to the user.
            let responseText = await res.text()
            let errorText = res.status + " " + res.statusText
            if (responseText) {
                responseText = responseText.split("\n")[0]
                errorText = errorText + ": " + responseText
            } else if (res.status == 500) {
                // An HTTP 500 error without status text from fetch() indicates
                // that the fetch() call could not contact the backend server,
                // rather than an HTTP error proper coming from the backend.
                errorText = "not possible to contact rollout backend (network issues / backend down)"
            }
            console.log('Request for rollout data failed: ' + errorText)
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
