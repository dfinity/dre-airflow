use super::types::v2::serialize_status_code;
use async_recursion::async_recursion;
use chrono::{DateTime, TimeDelta, Utc};
use indexmap::IndexMap;
use log::{debug, error, trace, warn};
use regex::Regex;
use reqwest::StatusCode;
use reqwest::cookie::Jar;
use reqwest::header::{ACCEPT, CONTENT_TYPE, REFERER};
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize};
use std::cmp::min;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::error::Error as ErrorTrait;
use std::fmt::Display;
use std::future::Future;
use std::string::FromUtf8Error;
use std::sync::Arc;
use std::time::Duration;
use std::{f64, fmt};
use std::{vec, vec::Vec};
use strum::Display;
use tokio::time::Instant;
use urlencoding::{decode, encode};

/// Default maximum batch size for paged requests in Airflow.
const MAX_BATCH_SIZE: usize = 100;
/// Exists to mitigate <https://github.com/apache/airflow/issues/41283> .
/// After upgrade to Airflow >= 2.10.5 in prod this code should be deleted, and we can then stop relying on this.  Then we can use ordering (order_by) to deterministically return task instances by start_date (backwards) in batches, using the normal paged_get mechanism.  We also may want to consider raising MAX_BATCH_SIZE to 1000 if there are no negative effects.
const MAX_TASK_INSTANCE_BATCH_SIZE: usize = 1000;
// API sub-URL for Airflow.
const API_SUBURL: &str = "api/v1/";

fn add_date_parm(url: String, parm_name: &str, date: Option<DateTime<Utc>>) -> String {
    let dfmt = "%Y-%m-%dT%H:%M:%S%.fZ";
    if let Some(date) = date {
        url.clone()
            + match url.find('?') {
                Some(_) => "&",
                None => "?",
            }
            + &format!("{}={}", parm_name, date.format(dfmt))
    } else {
        url
    }
}

fn add_updated_parameters(
    url: String,
    updated_at_lte: Option<DateTime<Utc>>,
    updated_at_gte: Option<DateTime<Utc>>,
) -> String {
    add_date_parm(
        add_date_parm(url, "updated_at_lte", updated_at_lte),
        "updated_at_gte",
        updated_at_gte,
    )
}

fn add_executed_parameters(
    url: String,
    execution_date_lte: Option<DateTime<Utc>>,
    execution_date_gte: Option<DateTime<Utc>>,
) -> String {
    add_date_parm(
        add_date_parm(url, "execution_date_lte", execution_date_lte),
        "execution_date_gte",
        execution_date_gte,
    )
}

fn add_ended_parameters(
    url: String,
    end_date_lte: Option<DateTime<Utc>>,
    end_date_gte: Option<DateTime<Utc>>,
) -> String {
    add_date_parm(
        add_date_parm(url, "end_date_lte", end_date_lte),
        "end_date_gte",
        end_date_gte,
    )
}

trait Pageable {
    fn len(&self) -> usize;
    /// Append all elements from other into self (to the end
    /// of the storage), replacing any element that already
    /// exists in self with the corresponding element in other.
    fn merge(&mut self, other: Self);
    /// Remove all elements from the end beyond usize - 1.
    /// The modified structure has maximum max_entries elements.
    fn truncate(&mut self, max_entries: usize);
}

#[derive(Debug, Deserialize, Clone)]
pub struct DagsResponseItem {
    /// dag_id is unique, enforced by Airflow.
    pub dag_id: String,
    #[allow(dead_code)]
    pub dag_display_name: String,
    pub is_paused: bool,
    pub is_active: bool,
    pub has_import_errors: bool,
}

#[derive(Debug, Deserialize, Default)]
pub struct DagsResponse {
    pub dags: Vec<DagsResponseItem>,
    #[serde(skip_serializing, skip_deserializing)]
    position_cache: HashMap<String, usize>,
    total_entries: usize,
}

impl Pageable for DagsResponse {
    fn len(&self) -> usize {
        self.dags.len()
    }
    fn merge(&mut self, other: Self) {
        for v in other.dags.clone().into_iter() {
            let id = v.dag_id.clone();
            match self.position_cache.get(&id) {
                Some(pos) => {
                    //debug!(target: "processing", "Replacing {} at position {}", id, pos);
                    self.dags[*pos] = v;
                }
                None => {
                    //debug!(target: "processing", "Consuming {}", id);
                    self.position_cache.insert(id, self.dags.len());
                    self.dags.push(v);
                }
            }
        }
        self.total_entries = other.total_entries;
    }
    fn truncate(&mut self, max_entries: usize) {
        if self.dags.len() > max_entries {
            self.dags.truncate(max_entries)
        }
    }
}

#[allow(dead_code)]
#[derive(Default)]
pub struct DagsQueryFilter<'a> {
    pub dag_id_pattern: Option<&'a String>,
}

impl<'a> DagsQueryFilter<'a> {
    fn as_queryparams(&self) -> Vec<(&str, String)> {
        let shit = [self
            .dag_id_pattern
            .as_ref()
            .map(|v| ("dag_id_pattern", (*v).clone()))];
        let res: Vec<_> = shit
            .iter()
            .flatten()
            .map(|(k, v)| (*k, (*v).clone()))
            .collect();
        res
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum DagRunState {
    Queued,
    Running,
    Success,
    Failed,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DagRunsResponseItem {
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
    pub state: DagRunState,
    pub note: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub struct DagRunsResponse {
    pub dag_runs: Vec<DagRunsResponseItem>,
    #[serde(skip_serializing, skip_deserializing)]
    position_cache: HashMap<String, usize>,
    total_entries: usize,
}

impl Pageable for DagRunsResponse {
    fn len(&self) -> usize {
        self.dag_runs.len()
    }
    fn merge(&mut self, other: Self) {
        for v in other.dag_runs.clone().into_iter() {
            let id = v.dag_id.clone() + v.dag_run_id.as_str();
            match self.position_cache.get(&id) {
                Some(pos) => {
                    //debug!(target: "processing", "Replacing {} at position {}", id, pos);
                    self.dag_runs[*pos] = v;
                }
                None => {
                    //debug!(target: "processing", "Consuming {}", id);
                    self.position_cache.insert(id, self.dag_runs.len());
                    self.dag_runs.push(v);
                }
            }
        }
        self.total_entries = other.total_entries;
    }
    fn truncate(&mut self, max_entries: usize) {
        if self.dag_runs.len() > max_entries {
            self.dag_runs.truncate(max_entries)
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct XComEntryResponse {
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

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Display)]
#[serde(rename_all = "snake_case")]
pub enum TaskInstanceState {
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TaskInstancesResponseItem {
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
    pub state: Option<TaskInstanceState>,
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

impl TaskInstancesResponseItem {
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
}

#[derive(Debug, Deserialize, Default)]

pub struct TaskInstancesResponse {
    pub task_instances: Vec<TaskInstancesResponseItem>,
    #[serde(skip_serializing, skip_deserializing)]
    position_cache: HashMap<String, usize>,
    total_entries: usize,
}

impl Pageable for TaskInstancesResponse {
    fn len(&self) -> usize {
        self.task_instances.len()
    }
    fn merge(&mut self, other: Self) {
        for v in other.task_instances.clone().into_iter() {
            let id = v.dag_id.clone()
                + v.dag_run_id.as_str()
                + v.task_id.as_str()
                + format!("{:?}", v.map_index).as_str();
            match self.position_cache.get(&id) {
                Some(pos) => {
                    //debug!(target: "processing", "Replacing {} at position {}", id, pos);
                    self.task_instances[*pos] = v;
                }
                None => {
                    //debug!(target: "processing", "Consuming {}", id);
                    self.position_cache.insert(id, self.task_instances.len());
                    self.task_instances.push(v);
                }
            }
        }
        self.total_entries = other.total_entries;
    }
    fn truncate(&mut self, max_entries: usize) {
        if self.task_instances.len() > max_entries {
            self.task_instances.truncate(max_entries)
        }
    }
}

#[derive(Default)]
pub struct TaskInstanceRequestFilters {
    executed_at_lte: Option<DateTime<Utc>>,
    executed_at_gte: Option<DateTime<Utc>>,
    updated_at_lte: Option<DateTime<Utc>>,
    updated_at_gte: Option<DateTime<Utc>>,
    ended_at_lte: Option<DateTime<Utc>>,
    ended_at_gte: Option<DateTime<Utc>>,
}

impl TaskInstanceRequestFilters {
    #[allow(dead_code)]
    pub fn executed_on_or_before(mut self, date: Option<DateTime<Utc>>) -> Self {
        self.executed_at_lte = date;
        self
    }
    pub fn executed_on_or_after(mut self, date: Option<DateTime<Utc>>) -> Self {
        self.executed_at_gte = date;
        self
    }
    #[allow(dead_code)]
    pub fn updated_on_or_before(mut self, date: Option<DateTime<Utc>>) -> Self {
        self.updated_at_lte = date;
        self
    }
    pub fn updated_on_or_after(mut self, date: Option<DateTime<Utc>>) -> Self {
        self.updated_at_gte = date;
        self
    }
    #[allow(dead_code)]
    pub fn ended_on_or_before(mut self, date: Option<DateTime<Utc>>) -> Self {
        self.ended_at_lte = date;
        self
    }
    pub fn ended_on_or_after(mut self, date: Option<DateTime<Utc>>) -> Self {
        self.ended_at_gte = date;
        self
    }
}

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
pub struct TasksResponseItem {
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

#[derive(Debug, Deserialize, Default)]

pub struct TasksResponse {
    pub tasks: Vec<TasksResponseItem>,
    #[serde(skip_serializing, skip_deserializing)]
    position_cache: HashMap<String, usize>,
    total_entries: usize,
}

impl Pageable for TasksResponse {
    fn len(&self) -> usize {
        self.tasks.len()
    }
    fn merge(&mut self, other: Self) {
        for v in other.tasks.clone().into_iter() {
            let id = v.task_id.clone();
            match self.position_cache.get(&id) {
                Some(pos) => {
                    //debug!(target: "processing", "Replacing {} at position {}", id, pos);
                    self.tasks[*pos] = v;
                }
                None => {
                    //debug!(target: "processing", "Consuming {}", id);
                    self.position_cache.insert(id, self.tasks.len());
                    self.tasks.push(v);
                }
            }
        }
        self.total_entries = other.total_entries;
    }
    fn truncate(&mut self, max_entries: usize) {
        if self.tasks.len() > max_entries {
            self.tasks.truncate(max_entries)
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EventLogsResponseItem {
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

impl EventLogsResponseItem {
    pub fn extra_hash(&self) -> Option<HashMap<&str, &str>> {
        match &self.extra {
            Some(extra_str) => serde_json::from_str::<HashMap<&str, &str>>(extra_str).ok(),
            None => None,
        }
    }
}
#[derive(Debug, Deserialize, Default)]

pub struct EventLogsResponse {
    pub event_logs: Vec<EventLogsResponseItem>,
    #[serde(skip_serializing, skip_deserializing)]
    position_cache: HashMap<u64, usize>,
    total_entries: usize,
}

#[allow(dead_code)]
#[derive(Default)]
pub struct EventLogsResponseFilters<'a> {
    pub dag_id: Option<&'a String>,
    pub task_id: Option<&'a String>,
    pub run_id: Option<&'a String>,
    pub map_index: Option<usize>,
    pub try_number: Option<usize>,
    pub event: Option<&'a String>,
    pub owner: Option<&'a String>,
    pub before: Option<DateTime<Utc>>,
    pub after: Option<DateTime<Utc>>,
}

fn querify(parms: Vec<(&str, &str)>) -> String {
    let mut s = "".to_string();
    for (k, v) in parms {
        let kencoded = encode(k).to_string();
        let vencoded = encode(v).to_string();
        s += format!("{}={}&", kencoded, vencoded).as_str()
    }
    s
}

impl<'a> EventLogsResponseFilters<'a> {
    fn as_queryparams(&self) -> Vec<(&str, String)> {
        let dfmt = "%Y-%m-%dT%H:%M:%S%.fZ";
        let shit = [
            self.dag_id.as_ref().map(|v| ("dag_id", (*v).clone())),
            self.task_id.as_ref().map(|v| ("task_id", (*v).clone())),
            self.run_id.as_ref().map(|v| ("run_id", (*v).clone())),
            self.map_index
                .as_ref()
                .map(|v| ("map_index", format!("{v}").to_owned())),
            self.try_number
                .as_ref()
                .map(|v| ("try_number", format!("{v}").to_owned())),
            self.event.as_ref().map(|v| ("event", (*v).clone())),
            self.owner.as_ref().map(|v| ("owner", (*v).clone())),
            self.before
                .as_ref()
                .map(|v| ("before", format!("{}", v.format(dfmt)).to_owned())),
            self.after
                .as_ref()
                .map(|v| ("after", format!("{}", v.format(dfmt)).to_owned())),
        ];
        let res: Vec<_> = shit
            .iter()
            .flatten()
            .map(|(k, v)| (*k, (*v).clone()))
            .collect();
        res
    }
}

impl Pageable for EventLogsResponse {
    fn len(&self) -> usize {
        self.event_logs.len()
    }
    fn merge(&mut self, other: Self) {
        for v in other.event_logs.clone().into_iter() {
            let id = v.event_log_id;
            match self.position_cache.get(&id) {
                Some(pos) => {
                    //debug!(target: "processing", "Replacing {} at position {}", id, pos);
                    self.event_logs[*pos] = v;
                }
                None => {
                    //debug!(target: "processing", "Consuming {}", id);
                    self.position_cache.insert(id, self.event_logs.len());
                    self.event_logs.push(v);
                }
            }
        }
        self.total_entries = other.total_entries;
    }
    fn truncate(&mut self, max_entries: usize) {
        if self.event_logs.len() > max_entries {
            self.event_logs.truncate(max_entries)
        }
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

struct PagingParameters {
    limit: usize,
    offset: usize,
}

async fn _paged_get<'a, T: Deserialize<'a> + Pageable + Default, G, Fut>(
    url: String,
    order_by: Option<String>,
    paging_parameters: Option<PagingParameters>,
    max_batch_size: usize,
    mut getter: G,
) -> Result<T, AirflowError>
where
    G: FnMut(String) -> Fut,
    Fut: Future<Output = Result<serde_json::Value, AirflowError>>,
{
    let mut results = T::default();
    let mut current_offset: usize = match &paging_parameters {
        Some(p) => p.offset,
        None => 0,
    };
    loop {
        let batch_limit = match &paging_parameters {
            Some(p) => min(p.limit, max_batch_size),
            None => 0,
        };
        // Let's handle our parameters.
        let mut suburl = url.clone();
        // First the paging.
        if paging_parameters.is_some() {
            for (k, v) in [("limit", batch_limit), ("offset", current_offset)].iter() {
                suburl = suburl.clone()
                    + match suburl.find('?') {
                        Some(_) => "&",
                        None => "?",
                    }
                    + format!("{k}={v}").as_str();
            }
        };
        // Then the order by.
        match &order_by {
            None => (),
            Some(order_by) => {
                suburl = suburl.clone()
                    + match suburl.find('?') {
                        Some(_) => "&",
                        None => "?",
                    }
                    + format!("{}={}", "order_by", order_by.as_str()).as_str();
            }
        }

        trace!(target: "airflow_client::paged_get",
            "retrieving {} instances of {} at offset {}, with {} already retrieved",
            batch_limit,
            suburl,
            current_offset,
            results.len(),
        );

        let batch = match getter(suburl).await {
            Ok(json_value) => match <T>::deserialize(json_value.clone()) {
                Ok(deserialized) => deserialized,
                Err(e) => {
                    warn!(target: "airflow_client::paged_get" ,"Error deserializing {} ({})\n{:?}", std::any::type_name::<T>(), e, json_value);
                    return Err(AirflowError::DeserializeError {
                        explanation: format!(
                            "Could not deserialize {}: {}",
                            std::any::type_name::<T>(),
                            e
                        ),
                        payload: json_value.to_string(),
                    });
                }
            },
            Err(e) => return Err(e),
        };
        let batch_len = batch.len();
        trace!(target: "airflow_client::paged_get", "Got {batch_len} before discarding");
        results.merge(batch);
        trace!(target: "airflow_client::paged_get",
            "Now we have {} objects after retrieving with offset {:?}",
            results.len(),
            current_offset
        );

        // If this is not a paged request, or we have consumed enough, break now.
        let batch_limit = min(batch_len, batch_limit);
        match &paging_parameters {
            None => break,
            Some(p) => {
                if results.len() >= p.limit {
                    // We have more results than we need.
                    results.truncate(p.limit);
                    break;
                }
                if batch_limit == 0 {
                    // Server returned no results.
                    break;
                }
            }
        }
        current_offset += batch_limit;
    }
    Ok(results)
}

async fn _post<'a, T: Deserialize<'a> + Pageable + Default, W, G, Fut>(
    url: String,
    content: W,
    mut poster: G,
) -> Result<T, AirflowError>
where
    G: FnMut(String, W) -> Fut,
    W: serde::Serialize + Sync + Send,
    Fut: Future<Output = Result<serde_json::Value, AirflowError>>,
{
    let mut results = T::default();
    trace!(target: "airflow_client::paged_post",
        "posting to and retrieving {url}",
    );

    let batch = match poster(url, content).await {
        Ok(json_value) => match <T>::deserialize(json_value.clone()) {
            Ok(deserialized) => deserialized,
            Err(e) => {
                warn!(target: "airflow_client::paged_post", "Error deserializing {} ({})\n{:?}", std::any::type_name::<T>(), e, json_value);
                return Err(AirflowError::DeserializeError {
                    explanation: format!(
                        "Could not deserialize {}: {}",
                        std::any::type_name::<T>(),
                        e
                    ),
                    payload: json_value.to_string(),
                });
            }
        },
        Err(e) => return Err(e),
    };
    let batch_len = batch.len();
    trace!(target: "airflow_client::paged_post", "Got {batch_len}");
    results.merge(batch);
    trace!(target: "airflow_client::paged_post",
        "Now we have {} objects after retrieving",
        results.len(),
    );
    Ok(results)
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

pub struct AirflowClient {
    pub url: reqwest::Url,
    username: String,
    password: String,
    client: Arc<reqwest::Client>,
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

impl AirflowClient {
    pub fn new(
        airflow_url: reqwest::Url,
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
            username,
            password,
        })
    }

    async fn _get_logged_in(&self, suburl: String) -> Result<serde_json::Value, AirflowError> {
        let suburl = API_SUBURL.to_string() + &suburl;
        self._get_or_login_and_get(suburl, true).await
    }

    async fn _post_logged_in<T>(
        &self,
        suburl: String,
        content: &T,
    ) -> Result<serde_json::Value, AirflowError>
    where
        T: Serialize + Sync + Send,
    {
        let suburl = API_SUBURL.to_string() + &suburl;
        self._post_or_login_and_post(suburl, content, true).await
    }

    #[async_recursion]
    async fn _get_or_login_and_get(
        &self,
        suburl: String,
        attempt_login: bool,
    ) -> Result<serde_json::Value, AirflowError> {
        // Next one cannot fail because self.url has already succeeded.
        let url = self.url.join(suburl.as_str()).unwrap();

        let c = self.client.clone();

        let start_time = Instant::now();
        let res = match c
            .get(url.clone())
            .header(ACCEPT, "application/json")
            .header(CONTENT_TYPE, "application/json")
            .send()
            .await
        {
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
                        debug!(target: "airflow_client::http_client", "GET {url} HTTP {status} after {:?}", Instant::now() - start_time);
                        decode_json_from_bytes(bytes.to_vec())
                    }
                    reqwest::StatusCode::FORBIDDEN | reqwest::StatusCode::UNAUTHORIZED => {
                        if attempt_login {
                            debug!(target: "airflow_client::http_client", "Attempting to log in with supplied credentials after server returned status {status} after {:?}", Instant::now() - start_time);
                            match self._login().await {
                                Ok(..) => (),
                                Err(err) => return Err(err),
                            };
                            self._get_or_login_and_get(suburl, false).await
                        } else {
                            Err(AirflowError::AuthenticationError(
                                "Proxy could not log into Airflow with its credentials (forbidden)"
                                    .into(),
                            ))
                        }
                    }
                    reqwest::StatusCode::NOT_FOUND => {
                        debug!(target: "airflow_client::http_client", "GET {url} HTTP {status} after {:?}", Instant::now() - start_time);
                        Err(AirflowError::StatusCode(reqwest::StatusCode::NOT_FOUND))
                    }
                    other => {
                        error!(target: "airflow_client::http_client", "GET {url} HTTP {status} after {:?}", Instant::now() - start_time);
                        Err(AirflowError::StatusCode(other))
                    }
                }
            }
            Err(err) => {
                debug!(target: "airflow_client::http_client", "GET {url} failed after {:?}", Instant::now() - start_time);
                Err(AirflowError::from(err))
            }
        };
        trace!(target: "airflow_client::http_client", "Result: {res:#?}");
        res
    }

    // FIXME: deduplicate with get_or_login_and_get
    #[async_recursion]
    async fn _post_or_login_and_post<T>(
        &self,
        suburl: String,
        content: &T,
        attempt_login: bool,
    ) -> Result<serde_json::Value, AirflowError>
    where
        T: Serialize + Sync + Send,
    {
        // Next one cannot fail because self.url has already succeeded.
        let url = self.url.join(suburl.as_str()).unwrap();

        let c = self.client.clone();

        let start_time = Instant::now();
        let res = match c
            .post(url.clone())
            .header(ACCEPT, "application/json")
            .header(CONTENT_TYPE, "application/json")
            .json(content)
            .send()
            .await
        {
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
                        debug!(target: "airflow_client::http_client", "POST {url} HTTP {status} after {:?}", Instant::now() - start_time);
                        decode_json_from_bytes(bytes.to_vec())
                    }
                    reqwest::StatusCode::FORBIDDEN => {
                        if attempt_login {
                            debug!(target: "airflow_client::http_client", "Attempting to log in with supplied credentials after server returned status {status} after {:?}", Instant::now() - start_time);
                            match self._login().await {
                                Ok(..) => (),
                                Err(err) => return Err(err),
                            };
                            self._post_or_login_and_post(suburl, content, false).await
                        } else {
                            Err(AirflowError::AuthenticationError(
                                "Proxy could not log into Airflow with its credentials (forbidden)"
                                    .into(),
                            ))
                        }
                    }
                    reqwest::StatusCode::NOT_FOUND => {
                        debug!(target: "airflow_client::http_client", "GET {url} HTTP {status} after {:?}", Instant::now() - start_time);
                        Err(AirflowError::StatusCode(reqwest::StatusCode::NOT_FOUND))
                    }
                    other => {
                        error!(target: "airflow_client::http_client", "GET {url} HTTP {status} after {:?}", Instant::now() - start_time);
                        Err(AirflowError::StatusCode(other))
                    }
                }
            }
            Err(err) => {
                debug!(target: "airflow_client::http_client", "GET {url} failed after {:?}", Instant::now() - start_time);
                Err(AirflowError::from(err))
            }
        };
        trace!(target: "airflow_client::http_client", "Result: {res:#?}");
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

    /// Return DAG info, by default in alphabetical order.
    #[allow(dead_code)]
    pub async fn dags(
        &self,
        limit: usize,
        offset: usize,
        filter: &DagsQueryFilter<'_>,
        order_by: Option<String>,
    ) -> Result<DagsResponse, AirflowError> {
        let qpairs = filter.as_queryparams();
        let qparams: Vec<_> = qpairs.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let suburl = "dags".to_string()
            + (if !qparams.is_empty() {
                "?".to_string() + querify(qparams).as_str()
            } else {
                "".to_string()
            })
            .as_str();
        _paged_get(
            suburl,
            match order_by {
                Some(x) => Some(x),
                None => Some("dag_id".into()),
            },
            Some(PagingParameters { limit, offset }),
            MAX_BATCH_SIZE,
            |x| self._get_logged_in(x),
        )
        .await
    }

    /// Return DAG runs from newest to oldest.
    /// Optionally only return DAG runs updated between a certain time frame.
    pub async fn dag_runs(
        &self,
        dag_id: &str,
        limit: usize,
        offset: usize,
        updated_at_lte: Option<DateTime<Utc>>,
        updated_at_gte: Option<DateTime<Utc>>,
    ) -> Result<DagRunsResponse, AirflowError> {
        let mut url = format!("dags/{dag_id}/dagRuns");
        url = add_updated_parameters(url, updated_at_lte, updated_at_gte);
        _paged_get(
            url,
            Some("-execution_date".into()),
            Some(PagingParameters { limit, offset }),
            MAX_BATCH_SIZE,
            |x| self._get_logged_in(x),
        )
        .await
    }

    /// Return DAG runs from newest to oldest.
    /// Optionally only return DAG runs updated between a certain time frame.
    pub async fn dag_run(
        &self,
        dag_id: &str,
        dag_run_id: &str,
    ) -> Result<DagRunsResponseItem, AirflowError> {
        let url = format!("dags/{dag_id}/dagRuns/{dag_run_id}");
        let json_value = self._get_logged_in(url).await?;
        match <DagRunsResponseItem>::deserialize(json_value.clone()) {
            Ok(deserialized) => Ok(deserialized),
            Err(e) => {
                warn!(target: "airflow_client::dag_run" ,"Error deserializing ({e})\n{json_value:?}");
                Err(AirflowError::DeserializeError {
                    explanation: format!(
                        "Could not deserialize {}: {}",
                        std::any::type_name::<DagRunsResponseItem>(),
                        e
                    ),
                    payload: json_value.to_string(),
                })
            }
        }
    }

    /// Return TaskInstances for a DAG ID and run.
    pub async fn task_instances(
        &self,
        dag_id: &str,
        dag_run_id: &str,
        limit: usize,
        offset: usize,
        filters: TaskInstanceRequestFilters,
    ) -> Result<TaskInstancesResponse, AirflowError> {
        let mut url = format!("dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances");
        url = add_executed_parameters(url, filters.executed_at_lte, filters.executed_at_gte);
        url = add_updated_parameters(url, filters.updated_at_lte, filters.updated_at_gte);
        url = add_ended_parameters(url, filters.ended_at_lte, filters.ended_at_gte);
        _paged_get(
            url,
            None,
            Some(PagingParameters { limit, offset }),
            MAX_TASK_INSTANCE_BATCH_SIZE,
            |x| self._get_logged_in(x),
        )
        .await
    }

    /// Return listed TaskInstances for a number of DAG IDs and DAG runs.
    pub async fn task_instances_batch(
        &self,
        dag_ids: Option<Vec<String>>,
        dag_run_ids: Option<Vec<String>>,
        task_ids: Option<Vec<String>>,
    ) -> Result<TaskInstancesResponse, AirflowError> {
        if let Some(dag_ids) = &dag_ids {
            if dag_ids.is_empty() {
                return Ok(TaskInstancesResponse::default());
            }
        }
        if let Some(dag_run_ids) = &dag_run_ids {
            if dag_run_ids.is_empty() {
                return Ok(TaskInstancesResponse::default());
            }
        }
        if let Some(task_instances) = &task_ids {
            if task_instances.is_empty() {
                return Ok(TaskInstancesResponse::default());
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
        _post(url.to_string(), &tr, |x, c| self._post_logged_in(x, c)).await
    }

    /// Return Tasks for a DAG run.
    pub async fn tasks(&self, dag_id: &str) -> Result<TasksResponse, AirflowError> {
        _paged_get(
            format!("dags/{dag_id}/tasks"),
            None,
            None,
            MAX_BATCH_SIZE,
            |x| self._get_logged_in(x),
        )
        .await
    }

    /// Return XCom entry of a (possibly mapped) task instance in a DAG run.
    pub async fn xcom_entry(
        &self,
        dag_id: &str,
        dag_run_id: &str,
        task_instance_id: &str,
        map_index: Option<usize>,
        xcom_key: &str,
    ) -> Result<XComEntryResponse, AirflowError> {
        let suburl = format!(
            "dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_instance_id}/xcomEntries/{xcom_key}"
        ) + match map_index {
            Some(i) => format!("&map_index={i}"),
            None => "".to_string(),
        }
        .as_str();
        let json_value = match self._get_logged_in(suburl).await {
            Ok(v) => v,
            Err(e) => return Err(e),
        };
        match <XComEntryResponse>::deserialize(json_value.clone()) {
            Ok(deserialized) => Ok(deserialized),
            Err(e) => {
                warn!(target: "airflow_client::xcom_entry", "Error deserializing {} ({})\n{:?}", std::any::type_name::<XComEntryResponse>(), e, json_value);
                Err(AirflowError::DeserializeError {
                    explanation: format!(
                        "Could not deserialize {}: {}",
                        std::any::type_name::<XComEntryResponse>(),
                        e
                    ),
                    payload: json_value.to_string(),
                })
            }
        }
    }

    /// Return event logs matching the filters specified.
    /// Events are returned by default in ID order (smallest to largest)
    /// which corresponds to chronological order.
    pub async fn event_logs(
        &self,
        limit: usize,
        offset: usize,
        filters: &EventLogsResponseFilters<'_>,
        order_by: Option<String>,
    ) -> Result<EventLogsResponse, AirflowError> {
        let qpairs = filters.as_queryparams();
        let qparams: Vec<_> = qpairs.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let suburl = "eventLogs".to_string()
            + (if !qparams.is_empty() {
                "?".to_string() + querify(qparams).as_str()
            } else {
                "".to_string()
            })
            .as_str();
        _paged_get(
            suburl,
            match order_by {
                Some(x) => Some(x),
                None => Some("event_log_id".into()),
            },
            Some(PagingParameters { limit, offset }),
            MAX_BATCH_SIZE,
            |x| self._get_logged_in(x),
        )
        .await
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
    use std::sync::Mutex;

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
    }

    fn lst(slc: &[&str]) -> VecDeque<String> {
        VecDeque::from_iter(slc.iter().map(|v| v.to_string()))
    }

    async fn getter(suburl: String) -> Result<serde_json::Value, AirflowError> {
        let concat = "http://localhost/".to_string() + &suburl;
        let url = Url::parse(concat.as_str()).unwrap();
        let mut offset: usize = 0;
        let mut _limit: usize = 100;
        for (k, v) in url.query_pairs() {
            if k == "limit" {
                _limit = from_str::<usize>(&v).unwrap();
            } else if k == "offset" {
                offset = from_str::<usize>(&v).unwrap();
            }
        }
        let unserded = Response {
            max_elements: 1000,
            elements: (offset..1000)
                .map(|v| v.to_string())
                .collect::<VecDeque<_>>(),
        };
        let serded = serde_json::value::to_value(unserded).unwrap();
        Ok(serded)
    }

    /// Every time this function is called with the same adjustment,
    /// the real offset is slid by one, such that we simulate the
    /// parallel addition of new elements while a paged query is
    /// taking place.  This covers us in the eventuality that e.g.
    /// a rollout is added while we were querying for rollouts in a
    /// paged manner.
    async fn sliding_getter(
        suburl: String,
        adjustment: Arc<Mutex<VecDeque<usize>>>,
    ) -> Result<serde_json::Value, AirflowError> {
        let mut adjustment = adjustment.lock().unwrap();
        let concat = "http://localhost/".to_string() + &suburl;
        let url = Url::parse(concat.as_str()).unwrap();
        let mut offset: usize = 0;
        let mut limit: usize = 10;
        for (k, v) in url.query_pairs() {
            if k == "limit" {
                limit = min(from_str::<usize>(&v).unwrap(), limit);
            } else if k == "offset" {
                offset = from_str::<usize>(&v).unwrap();
            }
        }
        //println!("Request: Offset {}  Limit {}", offset, limit);
        if adjustment.is_empty() {
            adjustment.push_front(1);
        } else {
            let val = adjustment.pop_front().unwrap();
            offset -= val;
            adjustment.push_front(val + 1);
        };
        //println!(
        //    "  Current adjustment is {}, adjusted offset is {}, limit is {}",
        //    current, offset, limit
        //);

        let unserded = Response {
            max_elements: 1000,
            elements: (offset..offset + limit)
                .map(|v| v.to_string())
                .collect::<VecDeque<_>>(),
        };
        //println!(
        //    "Real offset {}  Real limit {}  Returning {:?}",
        //    offset, limit, unserded
        //);
        let serded = serde_json::value::to_value(unserded).unwrap();
        Ok(serded)
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

        let response: Response = _paged_get(
            "".to_string(),
            None,
            Some(PagingParameters {
                limit: 1,
                offset: 0,
            }),
            MAX_BATCH_SIZE,
            getter,
        )
        .await
        .unwrap();
        assert_eq!(response.max_elements, 0);
        assert_eq!(response.elements, VecDeque::default());
    }

    #[tokio::test]
    async fn test_paged_onepage() {
        let response: Response = _paged_get(
            "".to_string(),
            None,
            Some(PagingParameters {
                limit: 1,
                offset: 0,
            }),
            MAX_BATCH_SIZE,
            getter,
        )
        .await
        .unwrap();
        assert_eq!(response.max_elements, 1000);
        assert_eq!(response.elements, lst(&["0"]));

        let response: Response = _paged_get(
            "".to_string(),
            None,
            Some(PagingParameters {
                limit: 2,
                offset: 0,
            }),
            MAX_BATCH_SIZE,
            getter,
        )
        .await
        .unwrap();
        assert_eq!(response.elements, lst(&["0", "1"]));

        let response: Response = _paged_get(
            "".to_string(),
            None,
            Some(PagingParameters {
                limit: 2,
                offset: 3,
            }),
            MAX_BATCH_SIZE,
            getter,
        )
        .await
        .unwrap();
        assert_eq!(response.elements, lst(&["3", "4"]));

        let response: Response = _paged_get(
            "".to_string(),
            None,
            Some(PagingParameters {
                limit: 2,
                offset: 998,
            }),
            MAX_BATCH_SIZE,
            getter,
        )
        .await
        .unwrap();
        assert_eq!(response.elements, lst(&["998", "999"]));

        let response: Response = _paged_get(
            "".to_string(),
            None,
            Some(PagingParameters {
                limit: 12,
                offset: 998,
            }),
            MAX_BATCH_SIZE,
            getter,
        )
        .await
        .unwrap();
        assert_eq!(response.elements, lst(&["998", "999"]));
    }

    #[tokio::test]
    async fn test_paged_slides() {
        let adjustments: Arc<Mutex<VecDeque<usize>>> = Arc::new(Mutex::new(vec![].into()));
        let c = move |m| {
            let adj = adjustments.clone();
            async move { sliding_getter(m, adj).await }.boxed()
        };
        let response: Response = _paged_get(
            "".to_string(),
            None,
            Some(PagingParameters {
                limit: 1,
                offset: 0,
            }),
            MAX_BATCH_SIZE,
            c,
        )
        .await
        .unwrap();
        //println!("{:?}", response);
        assert_eq!(response.max_elements, 1000);
        assert_eq!(response.elements, lst(&["0"]));

        let adjustments: Arc<Mutex<VecDeque<usize>>> = Arc::new(Mutex::new(vec![].into()));
        let c = move |m| {
            let adj = adjustments.clone();
            async move { sliding_getter(m, adj).await }.boxed()
        };
        let response: Response = _paged_get(
            "".to_string(),
            None,
            Some(PagingParameters {
                limit: 4,
                offset: 0,
            }),
            MAX_BATCH_SIZE,
            c,
        )
        .await
        .unwrap();
        assert_eq!(response.elements, lst(&["0", "1", "2", "3"]));

        let adjustments: Arc<Mutex<VecDeque<usize>>> = Arc::new(Mutex::new(vec![].into()));
        let c = move |m| {
            let adj = adjustments.clone();
            async move { sliding_getter(m, adj).await }.boxed()
        };
        let response: Response = _paged_get(
            "".to_string(),
            None,
            Some(PagingParameters {
                limit: 20,
                offset: 0,
            }),
            MAX_BATCH_SIZE,
            c,
        )
        .await
        .unwrap();
        assert_eq!(
            response.elements,
            lst(&[
                "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14",
                "15", "16", "17", "18", "19"
            ])
        );
    }

    #[tokio::test]
    async fn test_max_paged_slides() {
        let adjustments: Arc<Mutex<VecDeque<usize>>> = Arc::new(Mutex::new(vec![].into()));
        let c = move |m| {
            let adj = adjustments.clone();
            async move { sliding_getter(m, adj).await }.boxed()
        };
        let response: Response = _paged_get(
            "".to_string(),
            None,
            Some(PagingParameters {
                limit: 1000,
                offset: 0,
            }),
            MAX_BATCH_SIZE,
            c,
        )
        .await
        .unwrap();
        assert_eq!(response.elements.len(), 1000);
        assert_eq!(response.elements[0], "0");
        assert_eq!(response.elements[10], "10");
        assert_eq!(response.elements[50], "50");
        assert_eq!(response.elements[999], "999");
    }
}
