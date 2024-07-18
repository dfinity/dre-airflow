import { writable } from 'svelte/store'

export const rollouts = (() => {
    const store = writable([])

    let updater = async () => {
        const API_URL = import.meta.env.BACKEND_API_PATH || "/api/v1";
        const url = API_URL + "/rollouts"
        console.log("Hitting URL " + url)
        const res = await fetch(url);
        if (res.ok) {
            let json = await res.json()
            store.set(json)
            setTimeout(updater, 5000)
        } else {
            // Sometimes the API will fail!
            // FIXME: we should handle this with an error shown to the user.
            console.log('Request failed: ' + res.ok);
            setTimeout(updater, 5000)
        }
    }
    setTimeout(updater, 1)

    return store
});
