use crate::airflow_client::{EventLogsResponseFilters, EventLogsResponseItem};

use super::super::airflow_client::{AirflowClient, AirflowError, TaskInstancesResponseItem};
use chrono::{DateTime, Utc};
use futures::Future;
use log::{trace, warn};
use serde::{Serialize, de::DeserializeOwned};
use std::{fmt::Display, str::FromStr, sync::Arc};

use super::python::PythonDeserializer;

const LOG_TARGET: &str = "live_state::plan";

pub enum PlanQueryResult<P> {
    NotFound,
    Error(AirflowError),
    Invalid,
    Found(P),
}

#[derive(Clone, Serialize)]
pub enum PlanStateForTask<P> {
    Missing,
    Invalid,
    Valid(P),
}

#[derive(Clone, Default, Serialize)]
pub enum PlanCache<P> {
    #[default]
    Unretrieved,
    RetrievedAtTaskState {
        try_number: usize,
        latest_date: DateTime<Utc>,
        state: PlanStateForTask<P>,
    },
}

impl<P> PlanCache<P>
where
    P: Clone,
    P: FromStr,
    <P as std::str::FromStr>::Err: Display,
{
    /// Get (or fetch if absent) a P from either cache or
    /// reconstituted via FromStr from the suppplied fetcher
    /// that returns a String.
    /// Returns PlanQueryResult::{Found(P)|NotFound|Error}.
    /// Cache results of fetch (whether OK or Error) and return
    /// it whenever the cache key is up to date.
    pub async fn get_from_str(
        &mut self,
        associated_task_instance: &TaskInstancesResponseItem,
        fetcher: impl Future<Output = Result<String, AirflowError>>,
    ) -> PlanQueryResult<P> {
        match self {
            PlanCache::Unretrieved => (),
            PlanCache::RetrievedAtTaskState {
                try_number: t,
                latest_date: l,
                state: kind,
            } => {
                // From this branch we always return early.
                if *t == associated_task_instance.try_number
                    && *l == associated_task_instance.latest_date()
                {
                    match kind {
                        PlanStateForTask::Valid(plan) => {
                            return PlanQueryResult::Found(plan.clone());
                        }
                        PlanStateForTask::Invalid => {
                            return PlanQueryResult::Invalid;
                        }
                        PlanStateForTask::Missing => return PlanQueryResult::NotFound,
                    }
                }
            }
        };

        // Plan is stale or has not yet been retrieved.  Let's go!
        trace!(target: LOG_TARGET, "{} associated with task {} of DAG {} run {} is outdated; requerying.",
        std::any::type_name::<P>(), associated_task_instance.task_id, associated_task_instance.dag_id, associated_task_instance.dag_run_id);
        match fetcher.await {
            Ok(schedule) => match P::from_str(&schedule) {
                Ok(plan) => {
                    *self = Self::RetrievedAtTaskState {
                        try_number: associated_task_instance.try_number,
                        latest_date: associated_task_instance.latest_date(),
                        state: PlanStateForTask::Valid(plan.clone()),
                    };
                    PlanQueryResult::Found(plan)
                }
                Err(e) => {
                    warn!(target: LOG_TARGET, "Could not parse {} from XCom of task {} of DAG {} run {}: {}", std::any::type_name::<P>(), associated_task_instance.task_id, associated_task_instance.dag_id, associated_task_instance.dag_run_id, e);
                    *self = Self::RetrievedAtTaskState {
                        try_number: associated_task_instance.try_number,
                        latest_date: associated_task_instance.latest_date(),
                        state: PlanStateForTask::Invalid,
                    };
                    PlanQueryResult::Invalid
                }
            },
            Err(AirflowError::StatusCode(reqwest::StatusCode::NOT_FOUND)) => {
                *self = Self::RetrievedAtTaskState {
                    try_number: associated_task_instance.try_number,
                    latest_date: associated_task_instance.latest_date(),
                    state: PlanStateForTask::Missing,
                };
                PlanQueryResult::NotFound
            }
            Err(e) => PlanQueryResult::Error(e),
        }
    }
}

impl<P> PlanCache<P>
where
    P: Clone,
    P: DeserializeOwned,
{
    /// Get (or fetch if absent) a P from either the cache
    /// or serde::deserialized from the supplied fetcher that
    /// returns a String.
    /// Returns PlanQueryResult::{Found(P)|NotFound|Error}.
    /// Cache results of fetch (whether OK or Error) and return
    /// it whenever the cache key is up to date.
    pub async fn get(
        &mut self,
        associated_task_instance: &TaskInstancesResponseItem,
        fetcher: impl Future<Output = Result<String, AirflowError>>,
    ) -> PlanQueryResult<P> {
        match self {
            PlanCache::Unretrieved => (),
            PlanCache::RetrievedAtTaskState {
                try_number: t,
                latest_date: l,
                state: kind,
            } => {
                // From this branch we always return early.
                if *t == associated_task_instance.try_number
                    && *l == associated_task_instance.latest_date()
                {
                    match kind {
                        PlanStateForTask::Valid(plan) => {
                            return PlanQueryResult::Found(plan.clone());
                        }
                        PlanStateForTask::Invalid => {
                            return PlanQueryResult::Invalid;
                        }
                        PlanStateForTask::Missing => return PlanQueryResult::NotFound,
                    }
                }
            }
        };

        // Plan is stale or has not yet been retrieved.  Let's go!
        trace!(target: LOG_TARGET, "{} associated with task {} of DAG {} run {} is outdated; requerying.",
        std::any::type_name::<P>(), associated_task_instance.task_id, associated_task_instance.dag_id, associated_task_instance.dag_run_id);
        match fetcher.await {
            Ok(schedule) => match P::deserialize(&mut PythonDeserializer::from_str(&schedule)) {
                Ok(plan) => {
                    *self = Self::RetrievedAtTaskState {
                        try_number: associated_task_instance.try_number,
                        latest_date: associated_task_instance.latest_date(),
                        state: PlanStateForTask::Valid(plan.clone()),
                    };
                    PlanQueryResult::Found(plan)
                }
                Err(e) => {
                    warn!(target: LOG_TARGET, "Could not parse {} from XCom of task {} of DAG {} run {}: {}", std::any::type_name::<P>(), associated_task_instance.task_id, associated_task_instance.dag_id, associated_task_instance.dag_run_id, e);
                    *self = Self::RetrievedAtTaskState {
                        try_number: associated_task_instance.try_number,
                        latest_date: associated_task_instance.latest_date(),
                        state: PlanStateForTask::Invalid,
                    };
                    PlanQueryResult::Invalid
                }
            },
            Err(AirflowError::StatusCode(reqwest::StatusCode::NOT_FOUND)) => {
                *self = Self::RetrievedAtTaskState {
                    try_number: associated_task_instance.try_number,
                    latest_date: associated_task_instance.latest_date(),
                    state: PlanStateForTask::Missing,
                };
                PlanQueryResult::NotFound
            }
            Err(e) => PlanQueryResult::Error(e),
        }
    }
}

impl<P> PlanCache<P>
where
    P: Clone,
{
    /// Get (or fetch if absent / outdated) a P from either
    /// the cache or the supplied fetcher.
    /// Returns PlanQueryResult::{Found(P)|NotFound|Error}.
    /// Cache results of fetch (whether OK or Error) and return
    /// it whenever the cache key is up to date.
    pub async fn get_struct(
        &mut self,
        associated_task_instance: &TaskInstancesResponseItem,
        fetcher: impl Future<Output = Result<P, AirflowError>>,
    ) -> PlanQueryResult<P> {
        match self {
            PlanCache::Unretrieved => {
                trace!(target: LOG_TARGET, "{} associated with task {} of DAG {} run {} has not been retrieved; querying.",
                std::any::type_name::<P>(), associated_task_instance.task_id, associated_task_instance.dag_id, associated_task_instance.dag_run_id);
            }
            PlanCache::RetrievedAtTaskState {
                try_number: t,
                latest_date: l,
                state: kind,
            } => {
                // From this branch we always return early.
                if *t == associated_task_instance.try_number
                    && *l == associated_task_instance.latest_date()
                {
                    match kind {
                        PlanStateForTask::Valid(plan) => {
                            return PlanQueryResult::Found(plan.clone());
                        }
                        PlanStateForTask::Invalid => {
                            return PlanQueryResult::Invalid;
                        }
                        PlanStateForTask::Missing => return PlanQueryResult::NotFound,
                    }
                } else {
                    trace!(target: LOG_TARGET, "{} associated with task {} of DAG {} run {} is outdated ({},{} != {},{}; requerying.",
                    std::any::type_name::<P>(), associated_task_instance.task_id, associated_task_instance.dag_id, associated_task_instance.dag_run_id,
                    t,
                    l,
                    associated_task_instance.try_number,
                    associated_task_instance.latest_date());
                }
            }
        };

        // Plan is stale or has not yet been retrieved.  Let's go!
        match fetcher.await {
            Ok(schedule) => {
                *self = Self::RetrievedAtTaskState {
                    try_number: associated_task_instance.try_number,
                    latest_date: associated_task_instance.latest_date(),
                    state: PlanStateForTask::Valid(schedule.clone()),
                };
                trace!(target: LOG_TARGET, "{} associated with task {} of DAG {} run {} has been stored.",
                std::any::type_name::<P>(), associated_task_instance.task_id, associated_task_instance.dag_id, associated_task_instance.dag_run_id);
                PlanQueryResult::Found(schedule)
            }
            Err(AirflowError::StatusCode(reqwest::StatusCode::NOT_FOUND)) => {
                *self = Self::RetrievedAtTaskState {
                    try_number: associated_task_instance.try_number,
                    latest_date: associated_task_instance.latest_date(),
                    state: PlanStateForTask::Missing,
                };
                PlanQueryResult::NotFound
            }
            Err(e) => PlanQueryResult::Error(e),
        }
    }
}

pub async fn fetch_xcom(
    airflow_api: Arc<AirflowClient>,
    dag_id: &str,
    dag_run_id: &str,
    task_instance_id: &str,
    task_instance_map_index: Option<usize>,
    xcom_key: &str,
) -> Result<String, AirflowError> {
    Ok(airflow_api
        .xcom_entry(
            dag_id,
            dag_run_id,
            task_instance_id,
            task_instance_map_index,
            xcom_key,
        )
        .await?
        .value)
}

pub async fn fetch_audit_logs(
    airflow_api: Arc<AirflowClient>,
    dag_id: &str,
    dag_run_id: &str,
    task_instance_id: &str,
) -> Result<Vec<EventLogsResponseItem>, AirflowError> {
    // We retrieve all task instances regardless of map index,
    // because the caller wants to know if a task was marked as
    // successful, and this can be done per-map-index or for all
    // map indexes (indicated with map_index None).
    //
    // Tasks are retrieved from the most recent to the earliest,
    // but the caller may want to elect to process them in a
    // different order.
    Ok(airflow_api
        .event_logs(
            10,
            0,
            &EventLogsResponseFilters {
                dag_id: Some(&dag_id.to_string()),
                run_id: Some(&dag_run_id.to_string()),
                task_id: Some(&task_instance_id.to_string()),
                ..Default::default()
            },
            Some("-event_log_id".to_string()),
        )
        .await?
        .event_logs)
}
