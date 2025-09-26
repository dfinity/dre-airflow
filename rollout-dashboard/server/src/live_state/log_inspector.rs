use crate::airflow_client::EventLogsResponseItem;

use super::super::airflow_client::{AirflowClient, AirflowError, EventLogsResponseFilters};
use super::{DagID, DagRunID};
use chrono::{DateTime, Utc};
use log::{debug, trace};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::vec::Vec;

const LOG_TARGET: &str = "live_state::log_inspector";

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

impl DagRunUpdateType {
    fn nothing() -> Self {
        Self::SomeTaskInstances(HashSet::new())
    }

    fn task_id(task_id: &str) -> Self {
        Self::SomeTaskInstances(HashSet::from_iter(vec![task_id.to_owned()]))
    }
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
        let tgt = &format!("{LOG_TARGET}::{dag_id}");
        let mut task_instances_to_update_per_dag = DagRunUpdatesRequired::new();
        let mut last_event_log_update = self.last_event_log_update;

        if last_event_log_update.is_some() {
            // Construct a plan of what tasks will be queried, by using the
            // Airflow event log as a deciding factor.
            let event_logs = airflow_api
                .event_logs(
                    100000,
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
                    Some(r) => DagRunID::from_str(r).unwrap(),
                    None => continue,
                };

                trace!(target: tgt, "Processing event:\n{event:#?}\n");

                // Under the following circumstances, the whole rollout has to be refreshed because
                // administrative action was taken to clear / fail / succeed tasks that may not in
                // fact appear listed in the log as such.  The circumstances are explained below.
                fn event_impact(event: &EventLogsResponseItem) -> DagRunUpdateType {
                    match (&event.task_id, event.event.as_str()) {
                        (None, "success" | "failed" | "clear" | "dagrun_failed") => {
                            // 1. the event is success / failed / clear / dagrun_failed involving the whole DAG.
                            DagRunUpdateType::AllTaskInstances
                        }
                        (Some(task_id), "success" | "failed" | "clear") => {
                            // 1. the event is success / failed / clear, for a specific task,
                            // and there is some extra data.
                            match &event.extra {
                                Some(extra_str) => {
                                    match serde_json::from_str::<HashMap<&str, &str>>(extra_str) {
                                        // 2. the extra data successfully decoded to a JSON dict.
                                        Ok(extra) => {
                                            // 3. the extra data says the event is confirmed
                                            match extra.get("confirmed") == Some(&"true") {
                                                true => {
                                                    // 4. the extra data says any of past, future, upstream
                                                    // or downstream tasks were affected
                                                    if extra.get("past") == Some(&"true")
                                                        || extra.get("future") == Some(&"true")
                                                        || extra.get("upstream") == Some(&"true")
                                                        || extra.get("downstream") == Some(&"true")
                                                    {
                                                        DagRunUpdateType::AllTaskInstances
                                                    } else {
                                                        DagRunUpdateType::task_id(task_id)
                                                    }
                                                }
                                                false => {
                                                    // The event was not confirmed, so nothing changed.
                                                    DagRunUpdateType::nothing()
                                                }
                                            }
                                        }
                                        Err(_) => {
                                            // The extra data cannot be deserialized as dict.
                                            // Does not affect other tasks.
                                            DagRunUpdateType::nothing()
                                        }
                                    }
                                }
                                None => {
                                    // There is no extra data.
                                    // Does not affect other tasks.
                                    DagRunUpdateType::task_id(task_id)
                                }
                            }
                        }
                        (Some(task_id), _) => {
                            // Standard success / failed / clear or some other kind of event we do not handle.
                            // Does not affect other tasks.
                            DagRunUpdateType::task_id(task_id)
                        }
                        _ => DagRunUpdateType::nothing(),
                    }
                }

                let update_type = task_instances_to_update_per_dag
                        .dag_runs
                        .entry(event_run_id.clone())
                        .or_insert_with(|| {
                            let impact = event_impact(event);
                            match &impact {
                                DagRunUpdateType::AllTaskInstances => {
                                    trace!(target: tgt, "{event_run_id}: needs an update of all tasks")
                                }
                                DagRunUpdateType::SomeTaskInstances(sumtasks) => {
                                    trace!(target: tgt, "{event_run_id}: needs an update of tasks {sumtasks:?}")
                                }
                            };
                            impact
                        });

                match update_type {
                    DagRunUpdateType::AllTaskInstances => {}
                    DagRunUpdateType::SomeTaskInstances(existing) => match event_impact(event) {
                        DagRunUpdateType::AllTaskInstances => {
                            trace!(target: tgt, "{event_run_id}: switching plan to request to update all tasks");
                            *update_type = DagRunUpdateType::AllTaskInstances;
                        }
                        DagRunUpdateType::SomeTaskInstances(new) => {
                            trace!(target: tgt, "{event_run_id}: adding {new:?} to tasks that need updating");
                            let union = new.union(existing);
                            *update_type =
                                DagRunUpdateType::SomeTaskInstances(union.cloned().collect());
                        }
                    },
                }

                // Remember the date of the latest event.
                last_event_log_update = Some(event.when);
            }

            // Now that we have a plan, we know what data to fetch from Airflow, minimizing the load on the server.
            for (k, v) in task_instances_to_update_per_dag.dag_runs.iter() {
                debug!(target: tgt, "{:?}: tasks that will be updated: {}", k, match v {
                    DagRunUpdateType::AllTaskInstances => "all tasks".to_string(),
                    DagRunUpdateType::SomeTaskInstances(set_of_tasks) => set_of_tasks.iter().cloned().collect::<Vec<String>>().join(", "),
                });
            }

            debug!(
                target: tgt, "Setting incremental refresh date to {last_event_log_update:?}"
            )
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
            if !event_logs.event_logs.is_empty() {
                for event in event_logs.event_logs.iter() {
                    last_event_log_update = Some(event.when);
                }
                debug!(target: tgt, "Setting initial refresh date to {last_event_log_update:?}");
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
