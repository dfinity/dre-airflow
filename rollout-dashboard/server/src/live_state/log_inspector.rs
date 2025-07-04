use super::{DagID, DagRunID};
use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use log::{debug, trace};
use regex::Regex;
use rollout_dashboard::airflow_client::{AirflowClient, AirflowError, EventLogsResponseFilters};
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::vec::Vec;

const LOG_TARGET: &str = "live_state::log_inspector";

lazy_static! {
    static ref confirmed_or_true_regex: Regex = Regex::new(r".*.confirmed.: .true.*").unwrap();
}

#[derive(Debug, Clone)]
/// DAG run update type.
pub(super) enum DagRunUpdateType {
    /// This DAG run needs all its task instances refreshed.
    AllTaskInstances,
    /// At least these tasks in this DAG run need to be
    /// refreshed, as well as incremental queries of tasks
    /// finished, updated, or started since last query.
    SomeTaskInstances(HashSet<String>),
}

/// Describe what kind of update each DAG run needs.
/// A DAG run listed in this DAG can have one of two update types,
/// defined in `DagRunUpdateType``.
#[derive(Debug)]
pub(super) struct DagRunUpdatesRequired {
    dag_runs: HashMap<DagRunID, DagRunUpdateType>,
}

impl DagRunUpdatesRequired {
    fn new() -> Self {
        Self {
            dag_runs: HashMap::new(),
        }
    }

    /// Returns the `DagRunUpdateType` for a given DAG run.
    /// If the DAG run is unknown to this function, then
    /// SomeTaskInstances(vec[]) is returned.
    pub(super) fn update_type(&self, dag_run_id: &DagRunID) -> DagRunUpdateType {
        match self.dag_runs.get(dag_run_id) {
            None => DagRunUpdateType::SomeTaskInstances(HashSet::new()),
            Some(t) => t.clone(),
        }
    }
}
#[derive(Clone, Default)]
/// Inspects the Airflow log every time its incrementally_detect_dag_updates
/// function is called.
pub(super) struct AirflowIncrementalLogInspector {
    pub(super) last_event_log_update: Option<DateTime<Utc>>,
}

impl AirflowIncrementalLogInspector {
    /// Inspect changes to the log, and return a `DagRunUpdatesRequired`
    /// struct based on the contents of the log since its last inspection.
    pub(super) async fn incrementally_detect_dag_updates(
        &self,
        airflow_api: &AirflowClient,
        dag_id: &DagID,
    ) -> Result<(Self, DagRunUpdatesRequired), AirflowError> {
        // FIXME this needs to be by dag run ID too!
        let tgt = &format!("{}::{}", LOG_TARGET, dag_id);
        let mut task_instances_to_update_per_dag = DagRunUpdatesRequired::new();

        let mut last_event_log_update = self.last_event_log_update;

        if last_event_log_update.is_some() {
            // Construct a plan of what tasks will be queried, by using the
            // Airflow event log as a deciding factor.
            let event_logs = airflow_api
                .event_logs(
                    1000,
                    0,
                    &EventLogsResponseFilters {
                        after: last_event_log_update,
                        dag_id: Some(&dag_id.to_string()),
                        ..Default::default()
                    },
                    None,
                )
                .await?;

            // Process log events.
            for event in event_logs.event_logs.iter() {
                // Remember the date of the latest event.
                last_event_log_update = Some(event.when);
                // Ignore events with no dag ID or wrong dag ID.
                match &event.dag_id {
                    Some(d) => match *d == dag_id.to_string() {
                        true => d,
                        false => continue,
                    },
                    None => continue,
                };
                // Ignore events that have no run ID.
                let event_run_id = match &event.run_id {
                    Some(r) => r,
                    None => continue,
                };
                // Ignore events that just change the DAG run note.  We already
                // retrieve the full DAG (not necessarily its tasks or instances),
                // so this event is not interesting.
                if event.event == "ui.set_dag_run_note" {
                    continue;
                }
                // Also ignore UI confirmation events to mark tasks as failed/success.
                if event.event == "confirm" {
                    continue;
                }
                // Also ignore UI clearing events of tasks not yet confirmed.
                if event.event == "clear" {
                    match &event.extra {
                        None => continue,
                        Some(extra) => {
                            if confirmed_or_true_regex.captures(extra.as_str()).is_none() {
                                // No confirmation.  We continue.
                                continue;
                            }
                            // We found it.  We won't continue.
                        }
                    }
                }

                // Under the following circumstances, the whole rollout has to be refreshed because
                // administrative action was taken to clear / fail / succeed tasks that may not in
                // fact appear listed in the log as such.
                let force_refresh_all_tasks = (event.event == "success" && event.extra.is_some())
                    || (event.event == "failed" && event.extra.is_some())
                    || (event.event == "clear" && event.extra.is_some());

                trace!(target: tgt, "Processing event:\n{:#?}\n", event);

                match task_instances_to_update_per_dag
                    .dag_runs
                    .entry(DagRunID::from_str(event_run_id).unwrap())
                {
                    // No entry.  Let's initialize it (all tasks if event has no run_id or forced, else the single task).
                    Vacant(ventry) => {
                        ventry.insert(match (&event.task_id, force_refresh_all_tasks) {
                (Some(t), false) => {
                    trace!(target: tgt, "{}: initializing plan with a request to update task {}", event_run_id, t);
                    let mut init = HashSet::new();
                    init.insert(t.clone());
                    DagRunUpdateType::SomeTaskInstances(init)
                },
                _ => {
                    trace!(target: tgt, "{}: initializing plan with a request to update all tasks", event_run_id);
                    DagRunUpdateType::AllTaskInstances
                },
            });
                    }
                    // There's an entry.  Update to all tasks if this event has no run_id.
                    Occupied(mut entry) => match (&event.task_id, force_refresh_all_tasks) {
                        (Some(t), false) => {
                            if let DagRunUpdateType::SomeTaskInstances(thevec) = entry.get_mut() {
                                let ts = t.to_string();
                                if !thevec.contains(&ts) {
                                    trace!(target: tgt, "{}: adding task {} to plan", event_run_id, ts);
                                    thevec.insert(ts);
                                }
                            }
                        }
                        _ => {
                            if let DagRunUpdateType::SomeTaskInstances(_) = entry.get() {
                                trace!(target: tgt, "{}: switching plan to request to update all tasks", event_run_id);
                                entry.insert(DagRunUpdateType::AllTaskInstances);
                            }
                        }
                    },
                }
            }

            // Now that we have a plan, we know what data to fetch from Airflow, minimizing the load on the server.
            for (k, v) in task_instances_to_update_per_dag.dag_runs.iter() {
                debug!(target: tgt, "{:?}: tasks that will be updated: {}", k, match v {
                    DagRunUpdateType::AllTaskInstances => "all tasks".to_string(),
                    DagRunUpdateType::SomeTaskInstances(set_of_tasks) => set_of_tasks.iter().cloned().collect::<Vec<String>>().join(", "),
                });
            }
            if !event_logs.event_logs.is_empty()
                && !task_instances_to_update_per_dag.dag_runs.is_empty()
            {
                debug!(
                    target: tgt, "Setting incremental refresh date to {:?}",
                    last_event_log_update
                )
            };
        } else {
            let event_logs = airflow_api
                .event_logs(
                    1,
                    0,
                    &EventLogsResponseFilters {
                        after: last_event_log_update,
                        dag_id: Some(&dag_id.to_string()),
                        ..Default::default()
                    },
                    Some("-event_log_id".to_string()),
                )
                .await?;
            for event in event_logs.event_logs.iter() {
                last_event_log_update = Some(event.when);
            }
            if !event_logs.event_logs.is_empty() {
                debug!(target: tgt, "Setting initial refresh date to {:?}", last_event_log_update);
            }
        }

        Ok((
            Self {
                last_event_log_update,
            },
            task_instances_to_update_per_dag,
        ))
    }
}
