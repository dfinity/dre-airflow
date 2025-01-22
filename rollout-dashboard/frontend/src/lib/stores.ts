import { get, writable, type Writable } from 'svelte/store'
import { type Rollout, type RolloutsViewDelta } from './types'

export type RolloutsView = {
    error: [number, string] | string | null;
    rollouts: Rollout[];
    engine_state?: string;
}

const API_URL = import.meta.env.BACKEND_API_PATH || "/api/v1";
const url = API_URL + "/sse/rollouts_view?incremental"
var evtSource: null | EventSource = null;

const airflow_state = writable<RolloutsView>({ rollouts: [], error: "loading" })

function resetupEventSource() {
    if (null !== evtSource) {
        console.log("Dropping existing event source.")
        try {
            evtSource.close();
        }
        catch (e) {
            console.log({ message: "Event source already closed.", error: e })
        }
        evtSource = null;
    }

    // var evtSourceGenerated = new Date();
    evtSource = new EventSource(url);
    evtSource.onmessage = async function (event) {
        var sse_message: RolloutsViewDelta = JSON.parse(event.data);
        if (sse_message.error !== null) {
            let status = sse_message.error[0];
            if (status == 204) {
                airflow_state.set({ rollouts: [], error: "loading" })
            } else {
                let responseText = sse_message.error[1];
                let errorText = status + " " + responseText;
                if (responseText) {
                    responseText = responseText.split("\n")[0]
                    errorText = errorText + ": " + responseText
                }
                console.log('Request for rollout data failed: ' + errorText)
                airflow_state.set({
                    rollouts: get(airflow_state).rollouts,
                    engine_state: get(airflow_state).engine_state,
                    error: errorText
                })
            }
        } else if (sse_message.rollouts !== undefined) {
            console.log("Full sync with " + sse_message.rollouts.length + " rollouts and engine state " + sse_message.engine_state);
            airflow_state.set({
                rollouts: sse_message.rollouts,
                error: null,
                engine_state: sse_message.engine_state,
            })
        } else {
            var rollouts: Rollout[] = get(airflow_state).rollouts;
            var updated: Rollout[] | undefined = sse_message["updated"];
            var deleted: String[] | undefined = sse_message["deleted"];
            if (updated !== undefined) {
                console.log("Update of " + updated.length + " rollouts and engine state " + sse_message.engine_state);
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
                console.log("Removal of " + deleted.length + " rollouts");
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
            airflow_state.set({
                rollouts: rollouts,
                error: null,
                engine_state: sse_message.engine_state,
            })
        }
    }
    evtSource.onerror = function (e) {
        console.log({ message: "Disconnected from event source.  Reconnecting in 5 seconds.", event: e })
        if (evtSource !== null) { evtSource.close(); evtSource = null; }
        var errorText = 'Rollout dashboard is down — reconnecting in 5 seconds'
        airflow_state.set({
            rollouts: get(airflow_state).rollouts,
            error: errorText
        })
        setTimeout(resetupEventSource, 5000)
    }
}

export const rollouts_view = ((): Writable<RolloutsView> => {
    resetupEventSource()
    return airflow_state
});
