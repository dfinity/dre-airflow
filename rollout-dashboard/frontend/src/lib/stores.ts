import { get, writable, type Writable } from 'svelte/store'
import { type Rollout } from './types'

const BACKEND_TIMEOUT = 15000

export type RolloutResult = {
    error: string | Object | null;
    rollouts: Rollout[];
}


const API_URL = import.meta.env.BACKEND_API_PATH || "/api/v1";
const url = API_URL + "/rollouts/sse"
var evtSource: EventSource;

const rollout_store = writable<RolloutResult>({ rollouts: [], error: "loading" })

function resetupEventSource() {
    evtSource = new EventSource(url);
    evtSource.onmessage = async function (event) {
        var current_rollout_result = JSON.parse(event.data);
        if (current_rollout_result.error !== null) {
            let status = current_rollout_result.error[0];
            if (status == 204) {
                rollout_store.set({ rollouts: [], error: "loading" })
            } else {
                let responseText = current_rollout_result.error[1];
                let errorText = status + " " + responseText;
                if (responseText) {
                    responseText = responseText.split("\n")[0]
                    errorText = errorText + ": " + responseText
                }
                console.log('Request for rollout data failed: ' + errorText)
                rollout_store.set({
                    rollouts: get(rollout_store).rollouts,
                    error: errorText
                })
            }
        } else {
            rollout_store.set({
                rollouts: current_rollout_result.rollouts,
                error: null
            })
        }
    }
    evtSource.onerror = function (e) {
        console.log("Disconnected from event source.  Reconnecting in 5 seconds.")
        evtSource.close();
        var errorText = 'Rollout dashboard is down — reconnecting in 5 seconds'
        rollout_store.set({
            rollouts: get(rollout_store).rollouts,
            error: errorText
        })
        setTimeout(resetupEventSource, 5000)
    }
}

export const rollout_query = ((): Writable<RolloutResult> => {
    resetupEventSource()
    return rollout_store
});
