use chrono::{DateTime, Utc};
use futures::Future;
use log::{trace, warn};
use rollout_dashboard::airflow_client::{AirflowClient, AirflowError, TaskInstancesResponseItem};
use serde::{Serialize, de::DeserializeOwned};
use std::{fmt::Display, str::FromStr, sync::Arc};

use crate::live_state::python::PythonDeserializer;

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
    /// Get (or fetch if absent) the rollout plan.
    /// Returns Ok(None) when no plan is found.
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
    /// Get (or fetch if absent) the rollout plan.
    /// Returns Ok(None) when no plan is found.
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
