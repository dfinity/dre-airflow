import { get, writable, type Writable } from 'svelte/store'
import { type Rollout, type State, type Error, type RolloutsDelta, type RolloutEngineStates, type RolloutKind } from './types'

export type FullState = {
    error: [number, string] | string | null;
    rollouts: Rollout[];
    rollout_engine_states: RolloutEngineStates;
}

const API_URL = import.meta.env.BACKEND_API_PATH || "/api/v2";
const url = API_URL + "/sse"
var evtSource: null | EventSource = null;

const airflow_state = writable<FullState>({ rollouts: [], error: "loading", rollout_engine_states: { "rollout_ic_os_to_mainnet_subnets": "initial", "rollout_ic_os_to_mainnet_api_boundary_nodes": "initial" } })

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
    evtSource.addEventListener("State", (e) => {
        // Full state update.
        var msg: State = JSON.parse(e.data);
        console.log("Full sync with " + msg.rollouts.length + " rollouts");
        airflow_state.set({
            rollouts: msg.rollouts,
            error: null,
            rollout_engine_states: msg.rollout_engine_states,
        })
    });
    evtSource.addEventListener("Error", (e) => {
        // Error.
        var msg: Error = JSON.parse(e.data);
        console.log("Error code " + msg.code + " and message " + msg.message);
        let status = msg.code;
        if (status == 204) {
            airflow_state.set({ rollouts: [], error: "loading", rollout_engine_states: get(airflow_state).rollout_engine_states })
        } else {
            let responseText = msg.message;
            let errorText = status + " " + responseText;
            if (responseText) {
                responseText = responseText.split("\n")[0]
                errorText = errorText + ": " + responseText
            }
            console.log('Request for rollout data failed: ' + errorText)
            airflow_state.set({
                rollouts: get(airflow_state).rollouts,
                rollout_engine_states: get(airflow_state).rollout_engine_states,
                error: errorText
            })
        }
    });
    evtSource.addEventListener("RolloutEngineStates", (e) => {
        // New engine states array.
        console.log("New engine states array");
        var rollout_engine_states: RolloutEngineStates = JSON.parse(e.data);
        airflow_state.set({
            rollouts: get(airflow_state).rollouts,
            error: null,
            rollout_engine_states: rollout_engine_states,
        })

    });
    evtSource.addEventListener("RolloutsDelta", (e) => {
        // Rollout delta info.
        var msg: RolloutsDelta = JSON.parse(e.data); var rollouts_to_update: Rollout[] = get(airflow_state).rollouts;
        const updated = msg.updated;
        const deleted = msg.deleted;
        console.log("Update of " + updated.length + " rollouts with " + deleted.length + " removed");
        for (var i = updated.length - 1; i >= 0; i--) {
            var found = false;
            for (var j = rollouts_to_update.length - 1; j >= 0; j--) {
                if (rollouts_to_update[j].kind.concat(rollouts_to_update[j].name.toString()) == updated[i].kind.concat(updated[i].name.toString())) {
                    found = true;
                    rollouts_to_update[j] = updated[i];
                    break;
                }
            }
            if (!found) {
                rollouts_to_update.unshift(updated[i]);
            }
        }
        console.log("Removal of " + deleted.length + " rollouts");
        for (const current_deleted of deleted) {
            for (var j = rollouts_to_update.length - 1; j >= 0; j--) {
                console.log(rollouts_to_update[j].kind.concat(rollouts_to_update[j].name.toString()) + "    " + current_deleted)
                if (rollouts_to_update[j].kind.concat(rollouts_to_update[j].name.toString()) == current_deleted.kind.concat(current_deleted.name.toString())) {
                    rollouts_to_update.splice(j, 1);
                    break;
                }
            }
        }
        airflow_state.set({
            rollouts: rollouts_to_update,
            error: null,
            rollout_engine_states: get(airflow_state).rollout_engine_states,
        })
    });
    evtSource.onerror = function (e) {
        console.log({ message: "Disconnected from event source.  Reconnecting in 5 seconds.", event: e })
        if (evtSource !== null) { evtSource.close(); evtSource = null; }
        var errorText = 'Rollout dashboard is down — reconnecting in 5 seconds'
        airflow_state.set({
            rollouts: get(airflow_state).rollouts,
            error: errorText,
            rollout_engine_states: get(airflow_state).rollout_engine_states
        })
        setTimeout(resetupEventSource, 5000)
    }
}

export const rollouts_view = ((): Writable<FullState> => {
    resetupEventSource()
    return airflow_state
});
