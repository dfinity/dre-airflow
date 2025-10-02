use super::types::v2::serialize_status_code;
use async_recursion::async_recursion;
use chrono::{DateTime, TimeDelta, Utc};
use log::{debug, error, trace, warn};
use regex::Regex;
use reqwest::StatusCode;
use reqwest::cookie::Jar;
use reqwest::header::{ACCEPT, CONTENT_TYPE, REFERER};
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize};
use std::cmp::min;
use std::convert::TryFrom;
use std::error::Error as ErrorTrait;
use std::fmt;
use std::fmt::Display;
use std::future::Future;
use std::pin::Pin;
use std::string::FromUtf8Error;
use std::sync::Arc;
use std::time::Duration;
use std::vec::Vec;
use tokio::time::Instant;
use urlencoding::decode;

/// Default maximum batch size for paged requests in Airflow.
const MAX_BATCH_SIZE: usize = 400;

pub(crate) trait Pageable {
    fn len(&self) -> usize;
    /// Append all elements from other into self (to the end
    /// of the storage), replacing any element that already
    /// exists in self with the corresponding element in other.
    fn merge(&mut self, other: Self);
    /// Remove all elements from the end beyond usize - 1.
    /// The modified structure has maximum max_entries elements.
    fn truncate(&mut self, max_entries: usize);
    fn total_entries(&self) -> usize;
}

macro_rules! pageable_impl {
    ($structname:ident, $containermember:ident) => {
        impl Pageable for $structname {
            fn len(&self) -> usize {
                self.$containermember.len()
            }
            fn merge(&mut self, other: Self) {
                for v in other.$containermember.clone().into_iter() {
                    let id = v.unique_id();
                    match self.position_cache.get(&id) {
                        Some(pos) => {
                            //debug!(target: "processing", "Replacing {} at position {}", id, pos);
                            self.$containermember[*pos] = v;
                        }
                        None => {
                            //debug!(target: "processing", "Consuming {}", id);
                            self.position_cache.insert(id, self.$containermember.len());
                            self.$containermember.push(v);
                        }
                    }
                }
                self.total_entries = other.total_entries;
            }
            fn truncate(&mut self, max_entries: usize) {
                if self.$containermember.len() > max_entries {
                    self.$containermember.truncate(max_entries)
                }
            }
            fn total_entries(&self) -> usize {
                self.total_entries
            }
        }
    };
}
pub(crate) use pageable_impl;

pub use v1::{
    AirflowClient,
    dag_runs::{
        QueryFilter as DagRunsQueryFilter, QueryOrder as DagRunsQueryOrder,
        Response as DagRunsResponse, ResponseItem as DagRunsResponseItem, State as DagRunState,
    },
    dags::{
        QueryFilter as DagsQueryFilter, Response as DagsResponse, ResponseItem as DagsResponseItem,
    },
    event_logs::{
        QueryFilter as EventLogsQueryFilter, QueryOrder as EventLogsQueryOrder,
        Response as EventLogsResponse, ResponseItem as EventLogsResponseItem,
    },
    task_instances::{
        QueryFilter as TaskInstancesQueryFilter, Response as TaskInstancesResponse,
        ResponseItem as TaskInstancesResponseItem, State as TaskInstanceState,
    },
    tasks::{Response as TasksResponse, ResponseItem as TasksResponseItem},
    xcom_entries::Response as XComEntryResponse,
};

mod v1 {
    use serde::Serialize;
    use std::sync::Arc;
    use std::time::Duration;
    use std::vec::Vec;

    use super::{
        AirflowClientCreationError, AirflowError, AirflowHTTPClient, PagingParameters, URLizable,
        paged_get, typed_get, typed_post,
    };

    // API sub-URL for Airflow.
    const API_SUBURL: &str = "api/v1/";

    /// DAGs query and response.
    pub(super) mod dags {
        use super::super::{OrderBy, Pageable, pageable_impl};
        use chrono::{DateTime, Utc};
        use serde::{Deserialize, Serialize};
        use std::collections::HashMap;

        #[derive(Default, Serialize, Clone)]
        pub struct QueryFilter {
            #[serde(skip_serializing_if = "Option::is_none")]
            dag_id_pattern: Option<String>,
            #[serde(skip_serializing_if = "OrderBy::is_unordered")]
            order_by: OrderBy<QueryOrder>,
        }

        #[derive(Clone, strum::Display, Serialize)]
        #[strum(serialize_all = "snake_case")]
        #[serde(rename_all = "snake_case")]
        pub enum QueryOrder {
            DagId,
        }

        impl QueryFilter {
            pub fn dag_id_pattern(mut self, pattern: String) -> Self {
                self.dag_id_pattern = Some(pattern);
                self
            }
            pub fn order_by(mut self, order_by: OrderBy<QueryOrder>) -> Self {
                self.order_by = order_by;
                self
            }
        }

        #[derive(Debug, Deserialize, Clone)]
        pub struct ResponseItem {
            /// dag_id is unique, enforced by Airflow.
            pub dag_id: String,
            #[allow(dead_code)]
            pub dag_display_name: String,
            pub is_paused: bool,
            pub is_active: bool,
            pub has_import_errors: bool,
            pub last_parsed_time: Option<DateTime<Utc>>,
        }

        impl ResponseItem {
            fn unique_id(&self) -> String {
                self.dag_id.clone()
            }
        }

        #[derive(Debug, Deserialize, Default)]
        pub struct Response {
            pub dags: Vec<ResponseItem>,
            #[serde(skip_serializing, skip_deserializing)]
            position_cache: HashMap<String, usize>,
            total_entries: usize,
        }

        pageable_impl!(Response, dags);

        #[cfg(test)]
        mod tests {
            use super::super::super::{OrderBy, URLizable};
            use super::{QueryFilter, QueryOrder};

            #[tokio::test]
            async fn test_serialize_dag_filters() {
                let testcases = vec![
                    (
                        QueryFilter::default()
                            .dag_id_pattern("abcdef".into())
                            .order_by(OrderBy::Ascending(QueryOrder::DagId)),
                        "/api/v2?dag_id_pattern=abcdef&order_by=dag_id",
                    ),
                    (
                        QueryFilter::default()
                            .dag_id_pattern("abcdef".into())
                            .order_by(OrderBy::Descending(QueryOrder::DagId)),
                        "/api/v2?dag_id_pattern=abcdef&order_by=-dag_id",
                    ),
                ];
                for (q, exp) in testcases {
                    let res = q.add_to_url("/api/v2".into());
                    assert_eq!(res, exp);
                }
            }
        }
    }

    /// DAG runs query and response.
    pub(super) mod dag_runs {
        use super::super::{OrderBy, Pageable, airflow_date, pageable_impl};
        use chrono::{DateTime, Utc};
        use indexmap::IndexMap;
        use serde::{Deserialize, Serialize};
        use std::collections::HashMap;

        #[derive(Default, Serialize)]
        pub struct QueryFilter {
            #[serde(serialize_with = "airflow_date")]
            #[serde(skip_serializing_if = "Option::is_none")]
            updated_at_lte: Option<DateTime<Utc>>,
            #[serde(serialize_with = "airflow_date")]
            #[serde(skip_serializing_if = "Option::is_none")]
            updated_at_gte: Option<DateTime<Utc>>,
            #[serde(skip_serializing_if = "OrderBy::is_unordered")]
            order_by: OrderBy<QueryOrder>,
        }

        #[derive(Clone, strum::Display, Serialize)]
        #[strum(serialize_all = "snake_case")]
        #[serde(rename_all = "snake_case")]
        pub enum QueryOrder {
            DagRunId,
            DagId,
            StartDate,
            EndDate,
        }

        impl QueryFilter {
            /// Updated on or before.
            pub fn updated_at_lte(mut self, date: DateTime<Utc>) -> Self {
                self.updated_at_lte = Some(date);
                self
            }
            /// Updated on or after.
            pub fn updated_at_gte(mut self, date: DateTime<Utc>) -> Self {
                self.updated_at_gte = Some(date);
                self
            }
            pub fn order_by(mut self, order_by: OrderBy<QueryOrder>) -> Self {
                self.order_by = order_by;
                self
            }
        }

        #[derive(Debug, Serialize, Deserialize, Clone)]
        #[serde(rename_all = "snake_case")]
        pub enum State {
            Queued,
            Running,
            Success,
            Failed,
        }

        #[derive(Debug, Serialize, Deserialize, Clone)]
        pub struct ResponseItem {
            pub conf: IndexMap<String, serde_json::Value>,
            /// dag_run_id is unique, enforced by Airflow.
            pub dag_run_id: String,
            pub dag_id: String,
            pub logical_date: DateTime<Utc>,
            #[allow(dead_code)]
            pub start_date: Option<DateTime<Utc>>,
            #[allow(dead_code)]
            pub end_date: Option<DateTime<Utc>>,
            pub last_scheduling_decision: Option<DateTime<Utc>>,
            pub state: State,
            pub note: Option<String>,
        }

        impl ResponseItem {
            fn unique_id(&self) -> String {
                self.dag_id.clone() + self.dag_run_id.as_str()
            }
        }

        #[derive(Debug, Deserialize, Default)]
        pub struct Response {
            pub dag_runs: Vec<ResponseItem>,
            #[serde(skip_serializing, skip_deserializing)]
            position_cache: HashMap<String, usize>,
            total_entries: usize,
        }

        pageable_impl!(Response, dag_runs);

        #[cfg(test)]
        mod tests {
            use super::super::super::URLizable;
            use super::QueryFilter;
            use chrono::NaiveDateTime;

            #[tokio::test]
            async fn test_serialize_dag_run_filters() {
                let res = QueryFilter::default()
                    .updated_at_lte(
                        NaiveDateTime::parse_from_str("2025-01-01T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ")
                            .unwrap()
                            .and_utc(),
                    )
                    .add_to_url("/api/v2".into());
                let exp = "/api/v2?updated_at_lte=2025-01-01T00%3A00%3A00Z";
                assert_eq!(res, exp);
            }
        }
    }

    /// Task instances query and response.
    pub(super) mod task_instances {
        use super::super::{Pageable, airflow_date, negative_and_null_are_none, pageable_impl};
        use chrono::{DateTime, Utc};
        use serde::{Deserialize, Serialize};
        use std::collections::HashMap;
        use strum::Display;

        #[derive(Default, Serialize)]
        pub struct QueryFilter {
            #[serde(serialize_with = "airflow_date")]
            #[serde(skip_serializing_if = "Option::is_none")]
            execution_date_lte: Option<DateTime<Utc>>,
            #[serde(serialize_with = "airflow_date")]
            #[serde(skip_serializing_if = "Option::is_none")]
            execution_date_gte: Option<DateTime<Utc>>,
            #[serde(serialize_with = "airflow_date")]
            #[serde(skip_serializing_if = "Option::is_none")]
            updated_at_lte: Option<DateTime<Utc>>,
            #[serde(serialize_with = "airflow_date")]
            #[serde(skip_serializing_if = "Option::is_none")]
            updated_at_gte: Option<DateTime<Utc>>,
            #[serde(serialize_with = "airflow_date")]
            #[serde(skip_serializing_if = "Option::is_none")]
            end_date_lte: Option<DateTime<Utc>>,
            #[serde(serialize_with = "airflow_date")]
            #[serde(skip_serializing_if = "Option::is_none")]
            end_date_gte: Option<DateTime<Utc>>,
            #[serde(serialize_with = "airflow_date")]
            #[serde(skip_serializing_if = "Option::is_none")]
            start_date_lte: Option<DateTime<Utc>>,
            #[serde(serialize_with = "airflow_date")]
            #[serde(skip_serializing_if = "Option::is_none")]
            start_date_gte: Option<DateTime<Utc>>,
            // Airflow 3-only.
            //#[serde(skip_serializing_if = "OrderBy::is_unordered")]
            //order_by: OrderBy<TaskInstancesQueryOrder>,
        }

        // Airflow 3-only
        //#[derive(Clone, strum::Display, Serialize)]
        //#[strum(serialize_all = "snake_case")]
        //#[serde(rename_all = "snake_case")]
        //pub enum TaskInstancesQueryOrder {
        //    ExecutionDate,
        //    StartDate,
        //    EndDate,
        //    TaskId,
        //    MapIndex,
        //}

        impl QueryFilter {
            #[allow(dead_code)]
            /// Executed on or before.
            pub fn execution_date_lte(mut self, date: DateTime<Utc>) -> Self {
                self.execution_date_lte = Some(date);
                self
            }
            /// Executed on or after.
            pub fn execution_date_gte(mut self, date: DateTime<Utc>) -> Self {
                self.execution_date_gte = Some(date);
                self
            }
            /// Updated on or before.
            pub fn updated_at_lte(mut self, date: DateTime<Utc>) -> Self {
                self.updated_at_lte = Some(date);
                self
            }
            /// Updated on or after.
            pub fn updated_at_gte(mut self, date: DateTime<Utc>) -> Self {
                self.updated_at_gte = Some(date);
                self
            }
            /// Ended on or before.
            pub fn end_date_lte(mut self, date: DateTime<Utc>) -> Self {
                self.end_date_lte = Some(date);
                self
            }
            /// Ended on or after.
            pub fn end_date_gte(mut self, date: DateTime<Utc>) -> Self {
                self.end_date_gte = Some(date);
                self
            }
            /// Started on or before.
            pub fn start_date_lte(mut self, date: DateTime<Utc>) -> Self {
                self.start_date_lte = Some(date);
                self
            }
            /// Started on or after.
            pub fn start_date_gte(mut self, date: DateTime<Utc>) -> Self {
                self.start_date_gte = Some(date);
                self
            }
            // Airflow 3-only.
            //pub fn order_by(mut self, order_by: OrderBy<TaskInstancesQueryOrder>) -> Self {
            //    self.order_by = order_by;
            //    self
            //}
        }

        #[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Display)]
        #[serde(rename_all = "snake_case")]
        pub enum State {
            Success,
            Running,
            Failed,
            UpstreamFailed,
            Skipped,
            UpForRetry,
            UpForReschedule,
            Queued,
            Scheduled,
            Deferred,
            Removed,
            Restarting,
        }

        #[derive(Debug, Serialize, Deserialize, Clone)]
        pub struct ResponseItem {
            pub task_id: String,
            #[allow(dead_code)]
            pub task_display_name: String,
            pub dag_id: String,
            pub dag_run_id: String,
            #[allow(dead_code)]
            pub execution_date: DateTime<Utc>,
            #[allow(dead_code)]
            pub start_date: Option<DateTime<Utc>>,
            pub end_date: Option<DateTime<Utc>>,
            #[allow(dead_code)]
            pub duration: Option<f64>,
            pub state: Option<State>,
            #[allow(dead_code)]
            pub try_number: usize,
            #[serde(deserialize_with = "negative_and_null_are_none")]
            pub map_index: Option<usize>,
            #[allow(dead_code)]
            pub max_tries: usize,
            #[allow(dead_code)]
            pub operator: Option<String>,
            #[allow(dead_code)]
            pub rendered_map_index: Option<String>,
            #[allow(dead_code)]
            pub note: Option<String>,
        }

        impl ResponseItem {
            #[allow(dead_code)]
            pub fn latest_date(&self) -> DateTime<Utc> {
                let mut d = self.execution_date;
                if let Some(dd) = self.start_date {
                    if dd > d {
                        d = dd
                    }
                }
                if let Some(dd) = self.end_date {
                    if dd > d {
                        d = dd
                    }
                }
                d
            }
            #[allow(dead_code)]
            pub fn earliest_date(&self) -> DateTime<Utc> {
                let mut d = self.execution_date;
                if let Some(dd) = self.start_date {
                    if dd < d {
                        d = dd
                    }
                }
                if let Some(dd) = self.end_date {
                    if dd < d {
                        d = dd
                    }
                }
                d
            }

            fn unique_id(&self) -> String {
                self.dag_id.clone()
                    + self.dag_run_id.as_str()
                    + self.task_id.as_str()
                    + format!("{:?}", self.map_index).as_str()
            }
        }

        #[derive(Debug, Deserialize, Default)]
        pub struct Response {
            pub task_instances: Vec<ResponseItem>,
            #[serde(skip_serializing, skip_deserializing)]
            position_cache: HashMap<String, usize>,
            total_entries: usize,
        }

        pageable_impl!(Response, task_instances);
    }

    /// Event logs query and response.
    pub(super) mod event_logs {
        use super::super::{
            OrderBy, Pageable, airflow_date, negative_and_null_are_none, pageable_impl,
        };
        use chrono::{DateTime, Utc};
        use serde::{Deserialize, Serialize};
        use std::collections::HashMap;

        #[derive(Default, Serialize)]
        pub struct QueryFilter {
            #[serde(skip_serializing_if = "Option::is_none")]
            pub dag_id: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            pub task_id: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            pub run_id: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            pub map_index: Option<usize>,
            #[serde(skip_serializing_if = "Option::is_none")]
            pub try_number: Option<usize>,
            #[serde(skip_serializing_if = "Option::is_none")]
            pub event: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            pub owner: Option<String>,
            #[serde(serialize_with = "airflow_date")]
            #[serde(skip_serializing_if = "Option::is_none")]
            pub before: Option<DateTime<Utc>>,
            #[serde(serialize_with = "airflow_date")]
            #[serde(skip_serializing_if = "Option::is_none")]
            pub after: Option<DateTime<Utc>>,
            #[serde(skip_serializing_if = "OrderBy::is_unordered")]
            order_by: OrderBy<QueryOrder>,
        }

        #[derive(Clone, strum::Display, Serialize)]
        #[strum(serialize_all = "snake_case")]
        #[serde(rename_all = "snake_case")]
        pub enum QueryOrder {
            EventLogId,
            When,
            DagId,
            TaskId,
            RunId,
            MapIndex,
            ExecutionDate,
        }

        impl QueryFilter {
            pub fn dag_id(mut self, s: String) -> Self {
                self.dag_id = Some(s);
                self
            }
            pub fn task_id(mut self, s: String) -> Self {
                self.task_id = Some(s);
                self
            }
            pub fn run_id(mut self, s: String) -> Self {
                self.run_id = Some(s);
                self
            }
            pub fn before(mut self, s: DateTime<Utc>) -> Self {
                self.before = Some(s);
                self
            }
            pub fn after(mut self, s: DateTime<Utc>) -> Self {
                self.after = Some(s);
                self
            }
            pub fn order_by(mut self, order_by: OrderBy<QueryOrder>) -> Self {
                self.order_by = order_by;
                self
            }
        }

        #[derive(Debug, Serialize, Deserialize, Clone)]
        pub struct ResponseItem {
            pub event_log_id: u64,
            pub when: DateTime<Utc>,
            pub dag_id: Option<String>,
            pub task_id: Option<String>,
            pub run_id: Option<String>,
            #[serde(deserialize_with = "negative_and_null_are_none")]
            #[serde(default)]
            pub map_index: Option<usize>,
            pub try_number: Option<usize>,
            pub event: String,
            pub execution_date: Option<DateTime<Utc>>,
            pub owner: Option<String>,
            pub extra: Option<String>,
        }

        impl ResponseItem {
            fn unique_id(&self) -> u64 {
                self.event_log_id
            }

            pub fn extra_hash(&self) -> Option<HashMap<&str, &str>> {
                match &self.extra {
                    Some(extra_str) => serde_json::from_str::<HashMap<&str, &str>>(extra_str).ok(),
                    None => None,
                }
            }
        }

        #[derive(Debug, Deserialize, Default)]
        pub struct Response {
            pub event_logs: Vec<ResponseItem>,
            #[serde(skip_serializing, skip_deserializing)]
            position_cache: HashMap<u64, usize>,
            total_entries: usize,
        }

        pageable_impl!(Response, event_logs);
    }

    /// Tasks of DAG query and response.
    pub(super) mod tasks {
        use super::super::airflow_timedelta;
        use chrono::{DateTime, TimeDelta, Utc};
        use serde::Deserialize;

        #[derive(Debug, Deserialize, Clone)]
        #[serde(rename_all = "snake_case")]
        pub enum TriggerRule {
            AllSuccess,
            AllFailed,
            AllDone,
            AllDoneSetupSuccess,
            OneSuccess,
            OneFailed,
            OneDone,
            NoneFailed,
            NoneSkipped,
            NoneFailedOrSkipped,
            NoneFailedMinOneSuccess,
            Dummy,
            AllSkipped,
            Always,
        }

        #[derive(Debug, Deserialize, Clone)]
        pub struct ResponseItem {
            pub task_id: String,
            #[allow(dead_code)]
            pub task_display_name: String,
            #[allow(dead_code)]
            pub owner: String,
            #[allow(dead_code)]
            pub start_date: Option<DateTime<Utc>>,
            #[allow(dead_code)]
            pub end_date: Option<DateTime<Utc>>,
            #[allow(dead_code)]
            pub trigger_rule: TriggerRule,
            #[allow(dead_code)]
            pub is_mapped: bool,
            #[allow(dead_code)]
            pub wait_for_downstream: bool,
            #[allow(dead_code)]
            pub retries: f64,
            #[serde(deserialize_with = "airflow_timedelta")]
            #[allow(dead_code)]
            pub execution_timeout: Option<TimeDelta>,
            #[serde(deserialize_with = "airflow_timedelta")]
            #[allow(dead_code)]
            pub retry_delay: Option<TimeDelta>,
            #[allow(dead_code)]
            pub retry_exponential_backoff: bool,
            #[allow(dead_code)]
            pub ui_color: String,
            #[allow(dead_code)]
            pub ui_fgcolor: String,
            #[allow(dead_code)]
            pub template_fields: Vec<String>,
            pub downstream_task_ids: Vec<String>,
        }

        #[derive(Debug, Deserialize, Default, Clone)]
        pub struct Response {
            pub tasks: Vec<ResponseItem>,
        }
    }

    /// XCom entry query and response.
    pub(super) mod xcom_entries {
        use super::super::negative_and_null_are_none;
        use chrono::{DateTime, Utc};
        use serde::{Deserialize, Serialize};

        #[derive(Debug, Deserialize, Serialize, Clone)]
        pub struct Response {
            #[allow(dead_code)]
            pub key: String,
            #[allow(dead_code)]
            pub timestamp: DateTime<Utc>,
            #[allow(dead_code)]
            pub execution_date: DateTime<Utc>,
            #[serde(deserialize_with = "negative_and_null_are_none")]
            #[allow(dead_code)]
            pub map_index: Option<usize>,
            #[allow(dead_code)]
            pub task_id: String,
            #[allow(dead_code)]
            pub dag_id: String,
            pub value: String,
        }
    }

    pub struct AirflowClient {
        pub url: reqwest::Url,
        http: Arc<AirflowHTTPClient>,
    }

    impl AirflowClient {
        pub fn new(
            airflow_url: reqwest::Url,
            timeout: Duration,
        ) -> Result<Self, AirflowClientCreationError> {
            let mut censored_url = airflow_url.clone();
            let _ = censored_url.set_username("");
            let _ = censored_url.set_password(None);
            Ok(Self {
                url: censored_url,
                http: Arc::new(AirflowHTTPClient::new(
                    airflow_url,
                    API_SUBURL.to_string(),
                    timeout,
                )?),
            })
        }

        /// Return DAG info.
        pub async fn dags(
            &self,
            paging: PagingParameters,
            filter: dags::QueryFilter,
        ) -> Result<dags::Response, AirflowError> {
            paged_get(filter.add_to_url("dags".into()), paging, |x| {
                self.http.get_logged_in(x)
            })
            .await
        }

        /// Return DAG runs.
        pub async fn dag_runs(
            &self,
            dag_id: &str,
            paging: PagingParameters,
            filter: dag_runs::QueryFilter,
        ) -> Result<dag_runs::Response, AirflowError> {
            paged_get(
                filter.add_to_url(format!("dags/{dag_id}/dagRuns")),
                paging,
                |x| self.http.get_logged_in(x),
            )
            .await
        }

        /// Return a DAG run.
        pub async fn dag_run(
            &self,
            dag_id: &str,
            dag_run_id: &str,
        ) -> Result<dag_runs::ResponseItem, AirflowError> {
            let url = format!("dags/{dag_id}/dagRuns/{dag_run_id}");
            match typed_get(url, |x| self.http.get_logged_in(x)).await {
                Err(e) => Err(e),
                Ok((res, _)) => Ok(res),
            }
        }

        /// Return event logs matching the filters specified.
        /// While you should not rely on this observation, Airflow returns by default
        /// events in chronological order (old to new).
        pub async fn event_logs(
            &self,
            paging: PagingParameters,
            filter: event_logs::QueryFilter,
        ) -> Result<event_logs::Response, AirflowError> {
            paged_get(filter.add_to_url("eventLogs".to_string()), paging, |x| {
                self.http.get_logged_in(x)
            })
            .await
        }

        /// Return TaskInstances for a DAG ID and run.
        pub async fn task_instances(
            &self,
            dag_id: &str,
            dag_run_id: &str,
            paging: PagingParameters,
            filter: task_instances::QueryFilter,
        ) -> Result<task_instances::Response, AirflowError> {
            paged_get(
                filter.add_to_url(format!("dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances")),
                paging,
                |x| self.http.get_logged_in(x),
            )
            .await
        }

        /// Return listed TaskInstances for a number of DAG IDs and DAG runs.
        pub async fn task_instances_batch(
            &self,
            dag_ids: Option<Vec<String>>,
            dag_run_ids: Option<Vec<String>>,
            task_ids: Option<Vec<String>>,
        ) -> Result<task_instances::Response, AirflowError> {
            if let Some(dag_ids) = &dag_ids {
                if dag_ids.is_empty() {
                    return Ok(task_instances::Response::default());
                }
            }
            if let Some(dag_run_ids) = &dag_run_ids {
                if dag_run_ids.is_empty() {
                    return Ok(task_instances::Response::default());
                }
            }
            if let Some(task_instances) = &task_ids {
                if task_instances.is_empty() {
                    return Ok(task_instances::Response::default());
                }
            }
            let url = "dags/~/dagRuns/~/taskInstances/list";
            #[derive(Serialize)]
            struct TaskInstancesRequest {
                #[serde(skip_serializing_if = "Option::is_none")]
                dag_ids: Option<Vec<String>>,
                #[serde(skip_serializing_if = "Option::is_none")]
                dag_run_ids: Option<Vec<String>>,
                #[serde(skip_serializing_if = "Option::is_none")]
                task_ids: Option<Vec<String>>,
            }
            let tr = TaskInstancesRequest {
                dag_ids,
                dag_run_ids,
                task_ids,
            };
            typed_post(url.to_string(), &tr, |x, c| self.http.post_logged_in(x, c)).await
        }

        /// Return Tasks for a DAG run.
        pub async fn tasks(&self, dag_id: &str) -> Result<tasks::Response, AirflowError> {
            match typed_get(format!("dags/{dag_id}/tasks"), |x| {
                self.http.get_logged_in(x)
            })
            .await
            {
                Ok((res, _)) => Ok(res),
                Err(e) => Err(e),
            }
        }

        /// Return XCom entry of a (possibly mapped) task instance in a DAG run.
        pub async fn xcom_entry(
            &self,
            dag_id: &str,
            dag_run_id: &str,
            task_instance_id: &str,
            map_index: Option<usize>,
            xcom_key: &str,
        ) -> Result<xcom_entries::Response, AirflowError> {
            let suburl = format!(
                "dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_instance_id}/xcomEntries/{xcom_key}"
            ) + match map_index {
                Some(i) => format!("&map_index={i}"),
                None => "".to_string(),
            }
            .as_str();
            match typed_get(suburl, |x| self.http.get_logged_in(x)).await {
                Err(e) => Err(e),
                Ok((res, _)) => Ok(res),
            }
        }
    }
}

fn negative_and_null_are_none<'de, D>(deserializer: D) -> Result<Option<usize>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<i64> = Option::deserialize(deserializer)?;
    match s {
        None => Ok(None),
        Some(s) => {
            if s < 0 {
                Ok(None)
            } else {
                match usize::try_from(s) {
                    Ok(ss) => Ok(Some(ss)),
                    Err(e) => Err(D::Error::custom(format!("cannot fit {s} in usize: {e}"))),
                }
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct AirflowTimeDelta {
    days: i64,
    microseconds: u32,
    seconds: i64,
}

fn airflow_timedelta<'de, D>(deserializer: D) -> Result<Option<TimeDelta>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = <Option<AirflowTimeDelta>>::deserialize(deserializer)?;
    Ok(match s {
        None => None,
        Some(d) => TimeDelta::new(d.days * 86400 + d.seconds, d.microseconds * 1000),
    })
}

fn airflow_date<S>(date: &Option<DateTime<Utc>>, s: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let dfmt = "%Y-%m-%dT%H:%M:%S%.fZ";
    let string = if let Some(date) = date {
        date.format(dfmt).to_string()
    } else {
        "".to_string()
    };
    s.serialize_str(string.as_str())
}

#[derive(Default, Clone, Serialize)]
#[serde(into = "String")]
pub enum OrderBy<T: Clone + Display> {
    #[default]
    Unordered,
    Ascending(T),
    Descending(T),
}

impl<T> From<OrderBy<T>> for String
where
    T: Display,
    T: Clone,
{
    fn from(value: OrderBy<T>) -> Self {
        match value {
            OrderBy::Unordered => "".to_string(),
            OrderBy::Ascending(x) => x.to_string(),
            OrderBy::Descending(x) => format!("-{}", x),
        }
    }
}

impl<T> OrderBy<T>
where
    T: Clone,
    T: Display,
{
    fn is_unordered(&self) -> bool {
        matches!(self, Self::Unordered)
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
/// Problem communicating with Airflow.
///
/// When returned by a REST endpoint, this is serialized as an error HTTP
/// status code and a brief explanation.
pub enum AirflowError {
    /// The dashboard client to Airflow could not connect to Airflow.
    /// Serialized when responding to a REST request as the bad gateway HTTP status code.
    ReqwestError(String),
    #[serde(serialize_with = "serialize_status_code")]
    /// The dashboard client to Airflow received this status code as reply.
    /// Serialized when responding to a REST request as the contained HTTP status.
    StatusCode(reqwest::StatusCode),
    /// Airflow responded with malformed JSON to the dashboard's request.
    /// Serialized when responding to a REST request as an HTTP internal server error.
    JSONDecodeError {
        explanation: String,
        payload: String,
    },
    /// The contents of A POST request could not be serialized into JSON.
    JSONEncodeError { explanation: String },
    /// Airflow responded with an unexpected JSON data structure to the dashboard's request.
    /// Serialized when responding to a REST request as an HTTP internal server error.
    DeserializeError {
        explanation: String,
        payload: String,
    },
    /// Cannot communicate with Airflow because Airflow rejects the dashboard's
    /// requests as unauthorized.
    /// Serialized when responding to a REST request as an HTTP internal server error.
    AuthenticationError(String),
}

impl From<reqwest::Error> for AirflowError {
    fn from(err: reqwest::Error) -> AirflowError {
        let mut explanation = format!("Cannot contact Airflow: {err}");
        let mut err = err.source();
        loop {
            match err {
                None => break,
                Some(e) => {
                    explanation = format!("{} -> {}", explanation.as_str(), e);
                    err = e.source();
                }
            }
        }
        AirflowError::ReqwestError(explanation)
    }
}

impl From<AirflowError> for (StatusCode, String) {
    fn from(e: AirflowError) -> (reqwest::StatusCode, String) {
        match e {
            AirflowError::StatusCode(c) => (c, format!("{e}")),
            AirflowError::ReqwestError(_) => (StatusCode::BAD_GATEWAY, format!("{e}")),
            AirflowError::AuthenticationError(_)
            | AirflowError::DeserializeError { .. }
            | AirflowError::JSONEncodeError { .. }
            | AirflowError::JSONDecodeError { .. } => {
                (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}"))
            }
        }
    }
}

impl Display for AirflowError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::ReqwestError(s) => write!(f, "{s}"),
            Self::StatusCode(s) => write!(f, "Unexpected response code {s} from Airflow"),
            Self::JSONDecodeError { explanation, .. } => {
                write!(
                    f,
                    "Error decoding JSON / UTF-8 from Airflow response: {explanation}"
                )
            }
            Self::JSONEncodeError { explanation, .. } => {
                write!(
                    f,
                    "Error encoding into JSON / UTF-8 for Airflow request: {explanation}"
                )
            }
            Self::DeserializeError { explanation, .. } => {
                write!(
                    f,
                    "Error deserializing structure from Airflow response: {explanation}"
                )
            }
            Self::AuthenticationError(s) => write!(f, "{s}"),
        }
    }
}

trait URLizable {
    fn add_to_url(&self, url: String) -> String;
}

impl<T: Serialize> URLizable for T {
    fn add_to_url(&self, url: String) -> String {
        let parms = serde_url_params::to_string(&self).expect("Serialized correctly to URL params");
        match parms.as_str() {
            "" => url,
            _ => match url.find('?') {
                Some(_) => url + "&" + &parms,
                None => url + "?" + &parms,
            },
        }
    }
}

#[derive(Serialize)]
pub struct PagingParameters {
    limit: usize,
    offset: usize,
}

impl Default for PagingParameters {
    fn default() -> Self {
        Self {
            limit: 10000,
            offset: 0,
        }
    }
}

impl PagingParameters {
    pub fn new(limit: usize, offset: usize) -> Self {
        Self { limit, offset }
    }
}

async fn typed_get<'a, T: Deserialize<'a>, G, Fut>(
    url: String,
    mut getter: G,
) -> Result<(T, G), AirflowError>
where
    G: FnMut(String) -> Fut,
    Fut: Future<Output = Result<serde_json::Value, AirflowError>>,
{
    match getter(url).await {
        Ok(json_value) => match <T>::deserialize(json_value.clone()) {
            Ok(deserialized) => Ok((deserialized, getter)),
            Err(e) => {
                warn!(target: "airflow_client::typed_get" ,"Error deserializing {} ({})\n{:?}", std::any::type_name::<T>(), e, json_value);
                Err(AirflowError::DeserializeError {
                    explanation: format!(
                        "Could not deserialize {}: {}",
                        std::any::type_name::<T>(),
                        e
                    ),
                    payload: json_value.to_string(),
                })
            }
        },
        Err(e) => Err(e),
    }
}

async fn typed_post<'a, T: Deserialize<'a>, W, G, Fut>(
    url: String,
    content: W,
    mut poster: G,
) -> Result<T, AirflowError>
where
    G: FnMut(String, W) -> Fut,
    W: serde::Serialize + Sync + Send,
    Fut: Future<Output = Result<serde_json::Value, AirflowError>>,
{
    trace!(target: "airflow_client::typed_post",
        "posting to and retrieving {url}",
    );

    match poster(url, content).await {
        Ok(json_value) => match <T>::deserialize(json_value.clone()) {
            Ok(deserialized) => Ok(deserialized),
            Err(e) => {
                warn!(target: "airflow_client::typed_post", "Error deserializing {} ({})\n{:?}", std::any::type_name::<T>(), e, json_value);
                Err(AirflowError::DeserializeError {
                    explanation: format!(
                        "Could not deserialize {}: {}",
                        std::any::type_name::<T>(),
                        e
                    ),
                    payload: json_value.to_string(),
                })
            }
        },
        Err(e) => Err(e),
    }
}

async fn paged_get<'a, T: Deserialize<'a> + Pageable + Default, G, Fut>(
    url: String,
    paging: PagingParameters,
    mut getter: G,
) -> Result<T, AirflowError>
where
    G: FnMut(String) -> Fut,
    Fut: Future<Output = Result<serde_json::Value, AirflowError>>,
{
    let mut results = T::default();
    let mut current_offset = paging.offset;
    let target = "airflow_client::paged_get::".to_owned() + url.as_str();

    trace!(
        target: &target,
        "request to get {} instances at offset {}", paging.limit, paging.offset,
    );

    loop {
        // Let's handle our parameters.
        let mut batch_limit = min(paging.limit, MAX_BATCH_SIZE);
        batch_limit = min(
            batch_limit,
            match results.total_entries() {
                0 => batch_limit,
                _ => (results.total_entries()).saturating_sub(current_offset),
            },
        );

        trace!(target: &target,
            "current offset {} and may use limit {}",
            current_offset,
            batch_limit,
        );

        if batch_limit == 0 {
            break;
        }

        let suburl = PagingParameters {
            limit: batch_limit,
            offset: current_offset,
        }
        .add_to_url(url.clone());

        let batch: T;
        (batch, getter) = typed_get::<T, G, Fut>(suburl, getter).await?;
        let batch_len = batch.len();
        let total_entries = batch.total_entries();

        trace!(target: &target,
            "Requested {} instances at offset {}, with {} already retrieved, and obtained {} with {} in the view according to Airflow",
            batch_limit,
            current_offset,
            results.len(),
            batch_len,
            total_entries,
        );

        results.merge(batch);

        if results.len() >= paging.limit {
            trace!(target: &target, "Now we have {} out of {}, will break", results.len(), paging.limit);
            // We have equal or more entries.  We're done querying.
            results.truncate(paging.limit);
            break;
        }
        trace!(target: &target,
            "Now we have {} objects after retrieving with offset {:?}",
            results.len(),
            current_offset
        );

        if batch_len < batch_limit {
            trace!(target: &target, "Received {} after requesting {}, breaking", batch_len, batch_limit);
            // We got to the end of the results this time.  No more, no matter what we try.
            break;
        }

        current_offset += batch_limit;
    }
    Ok(results)
}

fn decode_json_from_bytes(bytes: Vec<u8>) -> Result<serde_json::Value, AirflowError> {
    Ok(match serde_json::from_slice(&bytes) {
        Ok(decoded) => decoded,
        Err(deser_error) => match String::from_utf8(bytes.to_vec()) {
            Ok(text) => {
                warn!(target: "airflow_client::decode_json_from_bytes" ,"Error decoding response to JSON ({deser_error})\n{text}");
                return Err(AirflowError::JSONDecodeError {
                    explanation: format!("{deser_error}"),
                    payload: text,
                });
            }
            Err(decode_error) => {
                warn!(target: "airflow_client::decode_json_from_bytes" ,"Error decoding response to UTF-8 ({decode_error})");
                return Err(AirflowError::JSONDecodeError {
                    explanation: format!("{deser_error}: {decode_error}"),
                    payload: "".to_string(),
                });
            }
        },
    })
}

#[derive(Debug)]
pub enum AirflowClientCreationError {
    Reqwest(reqwest::Error),
    Utf8(FromUtf8Error),
}

impl From<reqwest::Error> for AirflowClientCreationError {
    fn from(err: reqwest::Error) -> Self {
        Self::Reqwest(err)
    }
}

impl From<FromUtf8Error> for AirflowClientCreationError {
    fn from(err: FromUtf8Error) -> Self {
        Self::Utf8(err)
    }
}

impl Display for AirflowClientCreationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Cannot create Airflow client: {}",
            match self {
                Self::Reqwest(e) => {
                    format!("{e}")
                }
                Self::Utf8(e) => {
                    format!("{e}")
                }
            }
        )
    }
}

type ReqwestFutureMaker = Pin<
    Box<
        dyn Fn() -> Pin<Box<dyn Future<Output = Result<reqwest::Response, reqwest::Error>> + Send>>
            + Send
            + Sync,
    >,
>;

struct AirflowHTTPClient {
    url: reqwest::Url,
    username: String,
    password: String,
    client: Arc<reqwest::Client>,
    api_suburl: String,
}

impl AirflowHTTPClient {
    pub fn new(
        airflow_url: reqwest::Url,
        api_suburl: String,
        timeout: Duration,
    ) -> Result<Self, AirflowClientCreationError> {
        let jar = Jar::default();
        let arcjar = Arc::new(jar);
        let c = reqwest::Client::builder()
            .brotli(true)
            .zstd(true)
            .gzip(true)
            .deflate(true)
            .timeout(timeout)
            .cookie_provider(arcjar.clone())
            .build()?;
        let username = decode(airflow_url.username())?.into_owned();
        let password = decode(airflow_url.password().unwrap_or(""))?.into_owned();
        let mut censored_url = airflow_url.clone();
        let _ = censored_url.set_username("");
        let _ = censored_url.set_password(None);

        Ok(Self {
            client: Arc::new(c),
            url: censored_url,
            api_suburl,
            username,
            password,
        })
    }

    async fn get_logged_in(&self, suburl: String) -> Result<serde_json::Value, AirflowError> {
        let suburl = self.api_suburl.to_string() + &suburl;

        // Next one cannot fail because self.url has already succeeded.
        let url = self.url.join(suburl.as_str()).unwrap();
        let urlc = url.clone();

        let c = self.client.clone();

        let f: ReqwestFutureMaker = Box::pin(move || {
            Box::pin(
                c.get(url.clone())
                    .header(ACCEPT, "application/json")
                    .header(CONTENT_TYPE, "application/json")
                    .send(),
            )
        });

        let res = self._request_or_login_and_request("GET".to_string(), urlc, f, true);
        res.await
    }

    async fn post_logged_in<T>(
        &self,
        suburl: String,
        content: &T,
    ) -> Result<serde_json::Value, AirflowError>
    where
        T: Serialize + Sync + Send,
    {
        let suburl = self.api_suburl.to_string() + &suburl;

        // Next one cannot fail because self.url has already succeeded.
        let url = self.url.join(suburl.as_str()).unwrap();
        let urlc = url.clone();

        let c = self.client.clone();

        let data = Arc::new(match serde_json::to_vec(content) {
            Ok(body) => body,
            Err(err) => {
                return Err(AirflowError::JSONEncodeError {
                    explanation: format!("failure serializing: {}", err),
                });
            }
        });

        let f: ReqwestFutureMaker = Box::pin(move || {
            let data = data.clone();
            Box::pin(
                c.post(url.clone())
                    .header(ACCEPT, "application/json")
                    .header(CONTENT_TYPE, "application/json")
                    .body(Arc::unwrap_or_clone(data))
                    .send(),
            )
        });

        let res = self._request_or_login_and_request("GET".to_string(), urlc, f, true);
        res.await
    }

    #[async_recursion]
    async fn _request_or_login_and_request(
        &self,
        method: String,
        url: reqwest::Url,
        req: ReqwestFutureMaker,
        attempt_login: bool,
    ) -> Result<serde_json::Value, AirflowError> {
        let start_time = Instant::now();
        let res = match req().await {
            Ok(resp) => {
                let status = resp.status();
                match status {
                    reqwest::StatusCode::OK => {
                        let bytes = match resp.bytes().await {
                            Ok(bytes) => bytes,
                            Err(fetcherror) => {
                                return Err(AirflowError::from(fetcherror));
                            }
                        };
                        debug!(target: "airflow_client::AirflowHTTPClient", "{method} {url} HTTP {status} got {} bytes after {:?}", bytes.len(), Instant::now() - start_time);
                        decode_json_from_bytes(bytes.to_vec())
                    }
                    reqwest::StatusCode::FORBIDDEN | reqwest::StatusCode::UNAUTHORIZED => {
                        if attempt_login {
                            debug!(target: "airflow_client::AirflowHTTPClient", "Attempting to log in with supplied credentials after server returned status {status} after {:?}", Instant::now() - start_time);
                            match self._login().await {
                                Ok(..) => {
                                    self._request_or_login_and_request(method, url, req, false)
                                        .await
                                }
                                Err(err) => return Err(err),
                            }
                        } else {
                            Err(AirflowError::AuthenticationError(
                                "Proxy could not log into Airflow with its credentials (forbidden)"
                                    .into(),
                            ))
                        }
                    }
                    reqwest::StatusCode::NOT_FOUND => {
                        debug!(target: "airflow_client::AirflowHTTPClient", "{method} {url} HTTP {status} after {:?}", Instant::now() - start_time);
                        Err(AirflowError::StatusCode(reqwest::StatusCode::NOT_FOUND))
                    }
                    other => {
                        error!(target: "airflow_client::AirflowHTTPClient", "{method} {url} HTTP {status} after {:?}", Instant::now() - start_time);
                        Err(AirflowError::StatusCode(other))
                    }
                }
            }
            Err(err) => {
                debug!(target: "airflow_client::AirflowHTTPClient", "{method} {url} failed after {:?}", Instant::now() - start_time);
                Err(AirflowError::from(err))
            }
        };
        trace!(target: "airflow_client::AirflowHTTPClient", "Result: {res:#?}");
        res
    }

    async fn _login(&self) -> Result<(), AirflowError> {
        let login_url = self.url.join("login/").unwrap();
        let c = self.client.clone();

        let page_text = match c.get(login_url.clone()).send().await {
            Ok(resp) => match resp.status() {
                reqwest::StatusCode::OK => match resp.text().await {
                    Ok(text) => text,
                    Err(err) => {
                        return Err(AirflowError::AuthenticationError(format!(
                            "Error retrieving text that contains CSRF token: {err}"
                        )));
                    }
                },
                _ => {
                    return Err(AirflowError::AuthenticationError(format!(
                        "Error retrieving page for CSRF token: {}",
                        resp.status()
                    )));
                }
            },
            Err(err) => {
                return Err(AirflowError::from(err));
            }
        };

        let re = Regex::new(
            "<input(?:\\s+(?:(?:type|name|id)\\s*=\\s*\"[^\"]*\"\\s*)+)?\\s+value=\"([^\"]+)\">",
        )
        .unwrap();

        let mut csrf_tokens = vec![];
        for (_, [res]) in re.captures_iter(page_text.as_str()).map(|c| c.extract()) {
            csrf_tokens.push(res);
        }
        if csrf_tokens.is_empty() {
            return Err(AirflowError::AuthenticationError(
                "Could not find CSRF token in login page".into(),
            ));
        }

        let rb = c
            .post(login_url.clone())
            .header(REFERER, login_url.to_string())
            .form(&[
                ("csrf_token", csrf_tokens[0]),
                ("username", self.username.as_str()),
                ("password", self.password.as_str()),
            ]);
        match rb.send().await {
            Ok(resp) => match resp.status() {
                reqwest::StatusCode::OK => match resp.text().await {
                    Ok(text) => text,
                    Err(err) => {
                        return Err(AirflowError::AuthenticationError(format!(
                            "Error retrieving logged-in cookie: {err}"
                        )));
                    }
                },
                _ => {
                    let (status, text) = (resp.status(), resp.text().await);
                    return Err(AirflowError::AuthenticationError(format!(
                        "Server rejected login with status {status} and text {text:?}"
                    )));
                }
            },
            Err(err) => {
                return Err(AirflowError::from(err));
            }
        };

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use reqwest::Url;
    use serde::Serialize;
    use serde_json::{from_str, json};
    use std::collections::VecDeque;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn test_serialize_paging() {
        let p = PagingParameters {
            limit: 150,
            offset: 20,
        };
        let res: String = p.add_to_url("/api/v2".into());
        assert_eq!(res, "/api/v2?limit=150&offset=20");
    }

    #[tokio::test]
    async fn test_serialize_no_paging() {
        let p: Option<PagingParameters> = None;
        let pp = serde_url_params::to_string(&p).unwrap();
        assert_eq!(pp, "");
    }

    #[derive(Default, Deserialize, Serialize, Debug)]
    struct Response {
        max_elements: usize,
        elements: VecDeque<String>,
    }

    impl Pageable for Response {
        fn len(&self) -> usize {
            self.elements.len()
        }
        fn merge(&mut self, other: Response) {
            for v in other.elements.clone().into_iter() {
                let id = v.clone();
                let mut found = false;
                for q in self.elements.iter() {
                    if *q == id {
                        found = true;
                        break;
                    }
                }
                if !found {
                    self.elements.push_back(v)
                }
            }
            self.max_elements = other.max_elements;
        }
        fn truncate(&mut self, max_entries: usize) {
            if self.elements.len() > max_entries {
                self.elements.truncate(max_entries)
            }
        }
        fn total_entries(&self) -> usize {
            self.max_elements
        }
    }

    fn lst(slc: &[&str]) -> VecDeque<String> {
        VecDeque::from_iter(slc.iter().map(|v| v.to_string()))
    }

    async fn thousand_elements(
        suburl: String,
        maxcount: usize,
        reverse: bool,
    ) -> Result<serde_json::Value, AirflowError> {
        trace!(target:"thousand_elements", "{maxcount} {reverse} {suburl}");
        let concat = "http://localhost/".to_string() + &suburl;
        let url = Url::parse(concat.as_str()).unwrap();
        let mut offset: usize = 0;
        let mut limit: usize = maxcount;
        for (k, v) in url.query_pairs() {
            if k == "limit" {
                limit = from_str::<usize>(&v).unwrap();
            } else if k == "offset" {
                offset = from_str::<usize>(&v).unwrap();
            }
        }
        let mut unserded = Response {
            max_elements: maxcount,
            elements: (0..maxcount)
                .map(|v| v.to_string())
                .collect::<VecDeque<_>>(),
        };
        if reverse {
            unserded.elements = unserded.elements.into_iter().rev().collect();
        }
        let slc: Vec<_> = unserded.elements.into_iter().collect();
        let slc2 = &slc[offset..min(maxcount, offset + limit)];
        unserded.elements = slc2.iter().cloned().collect();
        let serded = serde_json::value::to_value(unserded).unwrap();
        Ok(serded)
    }

    struct MakeSlidingResponse {
        added: usize,
    }
    unsafe impl Send for MakeSlidingResponse {}

    impl MakeSlidingResponse {
        fn new() -> Self {
            Self { added: 0 }
        }

        async fn produce_response(
            &mut self,
            suburl: String,
        ) -> Result<serde_json::Value, AirflowError> {
            let res = thousand_elements(suburl, 1000 + self.added, true).await;
            if self.added < 5 {
                trace!(target:"MakeSlidingResponse", "maliciously adding one more element at the head");
                self.added += 1;
            }
            res
        }
    }

    #[tokio::test]
    async fn test_sliding_responder() {
        let mut m = MakeSlidingResponse::new();
        let rs = <Response>::deserialize(
            m.produce_response("?limit=1&offset=0".into())
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(rs.max_elements, 1000);
        assert_eq!(rs.elements, lst(&["999"]));
        let rs = <Response>::deserialize(
            m.produce_response("?limit=1&offset=0".into())
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(rs.max_elements, 1001);
        assert_eq!(rs.elements, lst(&["1000"]));
    }

    #[tokio::test]
    async fn test_paged_empty() {
        async fn getter(_suburl: String) -> Result<serde_json::Value, AirflowError> {
            let serded = json!({
                "max_elements": 0,
                "elements": []
            });
            Ok(serded)
        }

        let response: Response = paged_get(
            "".to_string(),
            PagingParameters {
                limit: 1,
                offset: 0,
            },
            getter,
        )
        .await
        .unwrap();
        assert_eq!(response.max_elements, 0);
        assert_eq!(response.elements, VecDeque::default());
    }

    #[tokio::test]
    async fn test_paged_onepage() {
        let response: Response = paged_get(
            "".to_string(),
            PagingParameters {
                limit: 1,
                offset: 0,
            },
            |suburl| thousand_elements(suburl, 1000, false),
        )
        .await
        .unwrap();
        assert_eq!(response.max_elements, 1000);
        assert_eq!(response.elements, lst(&["0"]));
    }

    #[tokio::test]
    async fn test_paged_onepage_2_elements() {
        let response: Response = paged_get(
            "".to_string(),
            PagingParameters {
                limit: 2,
                offset: 0,
            },
            |suburl| thousand_elements(suburl, 1000, false),
        )
        .await
        .unwrap();
        assert_eq!(response.elements, lst(&["0", "1"]));
    }

    #[tokio::test]
    async fn test_paged_onepage_2_elements_offset_3() {
        let response: Response = paged_get(
            "".to_string(),
            PagingParameters {
                limit: 2,
                offset: 3,
            },
            |suburl| thousand_elements(suburl, 1000, false),
        )
        .await
        .unwrap();
        assert_eq!(response.elements, lst(&["3", "4"]));
    }

    #[tokio::test]
    async fn test_paged_onepage_2_elements_offset_998() {
        let response: Response = paged_get(
            "".to_string(),
            PagingParameters {
                limit: 2,
                offset: 998,
            },
            |suburl| thousand_elements(suburl, 1000, false),
        )
        .await
        .unwrap();
        assert_eq!(response.elements, lst(&["998", "999"]));
    }

    #[tokio::test]
    async fn test_paged_onepage_12_elements_offset_998() {
        let response: Response = paged_get(
            "".to_string(),
            PagingParameters {
                limit: 12,
                offset: 998,
            },
            |x| thousand_elements(x, 1000, false),
        )
        .await
        .unwrap();
        assert_eq!(response.elements, lst(&["998", "999"]));
    }

    #[tokio::test]
    async fn test_paged_slides() {
        let c = move |m| {
            let r = Arc::new(Mutex::new(MakeSlidingResponse::new()));
            async move { r.lock().await.produce_response(m).await }.boxed()
        };
        let response: Response = paged_get(
            "".to_string(),
            PagingParameters {
                limit: 1,
                offset: 0,
            },
            c,
        )
        .await
        .unwrap();
        //println!("{:?}", response);
        assert_eq!(response.max_elements, 1000);
        assert_eq!(response.elements, lst(&["999"]));
    }

    #[tokio::test]
    async fn test_paged_slides2() {
        let responder = Arc::new(Mutex::new(MakeSlidingResponse::new()));
        let c = move |m| {
            let r = responder.clone();
            async move { r.lock().await.produce_response(m).await }.boxed()
        };
        let response: Response = paged_get(
            "".to_string(),
            PagingParameters {
                limit: 1000,
                offset: 0,
            },
            c,
        )
        .await
        .unwrap();
        assert_eq!(response.max_elements, 1005);
        assert_eq!(response.len(), 1000);
        assert_eq!(response.elements[0], "999");
        assert_eq!(response.elements[999], "0");
    }
}
