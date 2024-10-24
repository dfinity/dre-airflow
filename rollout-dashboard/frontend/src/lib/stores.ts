import { get, writable, type Writable } from 'svelte/store'
import { type Rollout } from './types'

const BACKEND_TIMEOUT = 15000

export type RolloutResult = {
    error: string | Object | null;
    rollouts: Rollout[];
}


const API_URL = import.meta.env.BACKEND_API_PATH || "/api/v1";
const url = API_URL + "/rollouts/sse?incremental"
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
            if (current_rollout_result.rollouts !== undefined) {
                rollout_store.set({
                    rollouts: current_rollout_result.rollouts,
                    error: null
                })
            } else {
                var rollouts: Rollout[] = get(rollout_store).rollouts;
                var updated: Rollout[] | undefined = current_rollout_result["updated"];
                var deleted: String[] | undefined = current_rollout_result["deleted"];
                if (updated !== undefined) {
                    for (var i = updated.length - 1; i >= 0; i--) {
                        var found = false;
                        for (var j = rollouts.length - 1; j >= 0; j--) {
                            if (rollouts[j].name == updated[i].name) {
                                found = true;
                                rollouts[j] = updated[i];
                                break;
                            }
                        }
                        if (!found) {
                            rollouts.unshift(updated[i]);
                        }
                    }
                }
                if (deleted !== undefined) {
                    for (const deleted_name of deleted) {
                        for (var j = rollouts.length - 1; j >= 0; j--) {
                            console.log(rollouts[j].name + "    " + deleted_name)
                            if (rollouts[j].name == deleted_name) {
                                rollouts.splice(j, 1);
                                break;
                            }
                        }
                    }
                }
                rollout_store.set({
                    rollouts: rollouts,
                    error: null
                })
            }
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
