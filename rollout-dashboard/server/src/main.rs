// FIXME remove all use of unwrap().
// FIXME tolerate other types of error not just AirflowError.
// FIXME make AirflowError more explanatory, not just ::Other()

use async_recursion::async_recursion;
use axum::debug_handler;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use axum::{routing::get, Router};
use axum_server;
use chrono::{DateTime, FixedOffset, TimeDelta, Utc};
use log::{debug, info, trace, warn};
use regex::Regex;
use reqwest::cookie::Jar;
use reqwest::header::{ACCEPT, CONTENT_TYPE, REFERER};
use reqwest::Url;
use serde::de::Error;
use serde::Serialize;
use serde::{Deserialize, Deserializer};
use std::cmp::{min, Ordering};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::future::Future;
use std::net::SocketAddr;
use std::rc::Rc;
use std::str::FromStr;
use std::sync::Arc;
use std::{env, f64};
use std::{vec, vec::Vec};
use topological_sort::TopologicalSort;
use urlencoding::decode;

mod python;

/// Default maximum batch size for paged requests in Airflow.
const MAX_BATCH_SIZE: usize = 100;

trait Pageable {
    fn is_empty(&self) -> bool;
    fn max_entries(&self) -> usize;
    fn len(&self) -> usize;
    fn merge(&mut self, other: Self) -> ();
}

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
enum SubnetRolloutState {
    Pending,
    Waiting,
    Proposing,
    WaitingForElection,
    WaitingForAdoption,
    WaitingForAlertsGone,
    Complete,
    Skipped,
    Error,
}

#[derive(Serialize, Debug)]
struct Subnet {
    subnet_id: String,
    git_revision: String,
    state: SubnetRolloutState,
}

#[derive(Serialize, Debug)]
struct Batch {
    start_time: DateTime<Utc>,
    subnets: Vec<Subnet>,
}

impl Batch {
    fn set_subnet_state(&mut self, state: SubnetRolloutState, index: Option<usize>) {
        match index {
            None => {
                for subnet in self.subnets.iter_mut() {
                    subnet.state = state.clone()
                }
            }
            Some(index) => self.subnets[index].state = state,
        }
    }
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "snake_case")]
enum RolloutState {
    Preparing,
    Waiting,
    UpgradingSubnets,
    UpgradingUnassignedNodes,
    Complete,
    Problem,
    Failed,
}

#[derive(Debug, Serialize)]
struct Rollout {
    name: String,
    note: Option<String>,
    state: RolloutState,
    dispatch_time: DateTime<Utc>,
    batches: HashMap<usize, Batch>,
}

impl Rollout {
    fn new(name: String, note: Option<String>, dispatch_time: DateTime<Utc>) -> Self {
        Self {
            name: name,
            note,
            state: RolloutState::Preparing,
            dispatch_time: dispatch_time,
            batches: HashMap::new(),
        }
    }
}

#[derive(Serialize, Debug)]
struct RolloutPlan {
    batches: HashMap<usize, Batch>,
}

type PythonFormattedRolloutPlan = HashMap<String, (String, Vec<String>)>;

#[derive(Debug)]
enum RolloutPlanParseError {
    PythonParseError(python::ErrorImpl),
    BatchNumberParseError,
    DateTimeParseError,
    SubnetParseError(String),
}

impl RolloutPlan {
    fn from_python_string(value: String) -> Result<Self, RolloutPlanParseError> {
        let subnet_git_revision_re =
            Regex::new("dfinity.ic_types.SubnetRolloutInstance.*@version=0[(]start_at=.*,subnet_id=([0-9-a-z-]+),git_revision=([0-9a-f]+)[)]").unwrap();
        let mut res = RolloutPlan {
            batches: HashMap::new(),
        };
        let python_string_plan: PythonFormattedRolloutPlan = match python::from_str(value.as_str())
        {
            Ok(s) => s,
            Err(e) => return Err(RolloutPlanParseError::PythonParseError(e)),
        };
        for (batch_number_str, (start_time_str, subnets)) in python_string_plan.iter() {
            let batch_number: usize = usize::from_str(batch_number_str).map_err(|e| {
                RolloutPlanParseError::SubnetParseError(format!(
                    "Could not parse batch number: {}",
                    e
                ))
            })? + 1;
            let start_time_fixed_offset: DateTime<FixedOffset> = match DateTime::parse_from_str(
                start_time_str.as_str(),
                "datetime.datetime@version=1(timestamp=%s%.f,tz=UTC)",
            ) {
                Ok(s) => Ok(s),
                Err(e) => match DateTime::parse_from_str(
                    start_time_str.as_str(),
                    "datetime.datetime@version=2(timestamp=%s%.f,tz=(UTC,pendulum.tz.timezone.FixedTimezone,1,True))",
                ) {
                    Err(e) => Err(RolloutPlanParseError::SubnetParseError(format!(
                        "Could not parse date/time: {}",
                        e
                    ))),
                    Ok(s) => Ok(s),
                },
            }?;
            let start_time: DateTime<Utc> =
                DateTime::from_utc(start_time_fixed_offset.naive_utc(), Utc);

            let mut final_subnets: Vec<Subnet> = vec![];
            for subnet in subnets.iter() {
                final_subnets.push(match subnet_git_revision_re.captures(subnet) {
                    Some(capped) => Subnet {
                        subnet_id: capped[1].to_string(),
                        git_revision: capped[2].to_string(),
                        state: SubnetRolloutState::Pending,
                    },
                    None => {
                        return Err(RolloutPlanParseError::SubnetParseError(format!(
                            "Could not obtain subnets from {}",
                            subnet
                        )))
                    }
                });
            }
            let batch = Batch {
                start_time,
                subnets: final_subnets,
            };
            res.batches.insert(batch_number, batch);
        }
        Ok(res)
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
enum DagRunState {
    Queued,
    Running,
    Success,
    Failed,
}

#[derive(Debug, Deserialize, Clone)]
struct DagRunsResponseItem {
    conf: HashMap<String, serde_json::Value>,
    dag_run_id: String,
    dag_id: String,
    logical_date: DateTime<Utc>,
    start_date: Option<DateTime<Utc>>,
    end_date: Option<DateTime<Utc>>,
    last_scheduling_decision: Option<DateTime<Utc>>,
    state: DagRunState,
    note: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct DagRunsResponse {
    dag_runs: Vec<DagRunsResponseItem>,
    #[serde(skip_serializing, skip_deserializing)]
    position_cache: HashMap<String, usize>,
    total_entries: usize,
}

impl Pageable for DagRunsResponse {
    fn is_empty(&self) -> bool {
        self.dag_runs.is_empty()
    }
    fn max_entries(&self) -> usize {
        self.total_entries
    }
    fn len(&self) -> usize {
        self.dag_runs.len()
    }
    fn merge(&mut self, other: Self) -> () {
        for v in other.dag_runs.clone().into_iter() {
            let id = v.dag_id.clone() + v.dag_run_id.as_str();
            match self.position_cache.get(&id) {
                Some(pos) => {
                    debug!(target: "processing", "Replacing {} at position {}", id, pos);
                    self.dag_runs[*pos] = v;
                }
                None => {
                    debug!(target: "processing", "Consuming {}", id);
                    self.position_cache.insert(id, self.dag_runs.len());
                    self.dag_runs.push(v);
                }
            }
        }
        self.total_entries = other.total_entries;
    }
}

#[derive(Debug, Deserialize)]
struct XComEntryResponse {
    key: String,
    timestamp: DateTime<Utc>,
    execution_date: DateTime<Utc>,
    #[serde(deserialize_with = "negative_is_none")]
    map_index: Option<usize>,
    task_id: String,
    dag_id: String,
    value: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
enum TaskInstanceState {
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

fn negative_is_none<'de, D>(deserializer: D) -> Result<Option<usize>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = i64::deserialize(deserializer)?;
    if s < 0 {
        Ok(None)
    } else {
        match usize::try_from(s) {
            Ok(ss) => Ok(Some(ss)),
            Err(e) => Err(D::Error::custom(format!("cannot fit in usize: {}", e))),
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

#[derive(Debug, Deserialize, Clone)]
struct TaskInstancesResponseItem {
    task_id: String,
    task_display_name: String,
    dag_id: String,
    dag_run_id: String,
    execution_date: DateTime<Utc>,
    start_date: Option<DateTime<Utc>>,
    end_date: Option<DateTime<Utc>>,
    duration: Option<f64>,
    state: Option<TaskInstanceState>,
    try_number: usize,
    #[serde(deserialize_with = "negative_is_none")]
    map_index: Option<usize>,
    max_tries: usize,
    operator: Option<String>,
    rendered_map_index: Option<String>,
    note: Option<String>,
}

#[derive(Debug, Deserialize, Default)]

struct TaskInstancesResponse {
    task_instances: Vec<TaskInstancesResponseItem>,
    #[serde(skip_serializing, skip_deserializing)]
    position_cache: HashMap<String, usize>,
    total_entries: usize,
}

impl Pageable for TaskInstancesResponse {
    fn is_empty(&self) -> bool {
        self.task_instances.is_empty()
    }
    fn max_entries(&self) -> usize {
        self.total_entries
    }
    fn len(&self) -> usize {
        self.task_instances.len()
    }
    fn merge(&mut self, other: Self) -> () {
        for v in other.task_instances.clone().into_iter() {
            let id = v.dag_id.clone()
                + v.dag_run_id.as_str()
                + v.task_id.as_str()
                + format!("{:?}", v.map_index).as_str();
            match self.position_cache.get(&id) {
                Some(pos) => {
                    debug!(target: "processing", "Replacing {} at position {}", id, pos);
                    self.task_instances[*pos] = v;
                }
                None => {
                    debug!(target: "processing", "Consuming {}", id);
                    self.position_cache.insert(id, self.task_instances.len());
                    self.task_instances.push(v);
                }
            }
        }
        self.total_entries = other.total_entries;
    }
}

#[derive(Debug, Deserialize, Clone)]
struct TasksResponseItem {
    task_id: String,
    task_display_name: String,
    owner: String,
    start_date: Option<DateTime<Utc>>,
    end_date: Option<DateTime<Utc>>,
    // FIXME: make this into a TriggerRule enum.
    // https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html#operation/get_tasks
    trigger_rule: String,
    is_mapped: bool,
    wait_for_downstream: bool,
    retries: f64,
    #[serde(deserialize_with = "airflow_timedelta")]
    execution_timeout: Option<TimeDelta>,
    #[serde(deserialize_with = "airflow_timedelta")]
    retry_delay: Option<TimeDelta>,
    retry_exponential_backoff: bool,
    ui_color: String,
    ui_fgcolor: String,
    template_fields: Vec<String>,
    downstream_task_ids: Vec<String>,
}

#[derive(Debug, Deserialize, Default)]

struct TasksResponse {
    tasks: Vec<TasksResponseItem>,
    #[serde(skip_serializing, skip_deserializing)]
    position_cache: HashMap<String, usize>,
    total_entries: usize,
}

impl Pageable for TasksResponse {
    fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }
    fn max_entries(&self) -> usize {
        self.total_entries
    }
    fn len(&self) -> usize {
        self.tasks.len()
    }
    fn merge(&mut self, other: Self) -> () {
        for v in other.tasks.clone().into_iter() {
            let id = v.task_id.clone();
            match self.position_cache.get(&id) {
                Some(pos) => {
                    debug!(target: "processing", "Replacing {} at position {}", id, pos);
                    self.tasks[*pos] = v;
                }
                None => {
                    debug!(target: "processing", "Consuming {}", id);
                    self.position_cache.insert(id, self.tasks.len());
                    self.tasks.push(v);
                }
            }
        }
        self.total_entries = other.total_entries;
    }
}

#[derive(Debug)]
enum AirflowError {
    StatusCode(reqwest::StatusCode),
    Other(String),
}

struct PagingParameters {
    limit: usize,
    offset: usize,
}

async fn _paged_get<'a, T: Deserialize<'a> + Pageable + Default, G, Fut>(
    url: String,
    order_by: Option<String>,
    paging_parameters: Option<PagingParameters>,
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
            Some(p) => min(p.limit, MAX_BATCH_SIZE),
            None => 0,
        };
        // Let's handle our parameters.
        let mut suburl = url.clone();
        // First the paging.
        if paging_parameters.is_some() {
            for (k, v) in [("limit", batch_limit), ("offset", current_offset)].iter() {
                suburl = suburl.clone()
                    + match suburl.find("?") {
                        Some(_) => "&",
                        None => "?",
                    }
                    + format!("{}={}", k, v).as_str();
            }
        };
        // Then the order by.
        if order_by.is_some() {
            suburl = suburl.clone()
                + match suburl.find('?') {
                    Some(_) => "&",
                    None => "?",
                }
                + format!("{}={}", "order_by", order_by.clone().unwrap()).as_str()
        }

        trace!(target: "paged_get",
            "retrieving {} instances of {} at offset {}, with {} already retrieved",
            batch_limit,
            suburl,
            current_offset,
            results.len(),
        );

        let mut batch = match getter(suburl).await {
            Ok(json_value) => match <T>::deserialize(json_value.clone()) {
                Ok(deserialized) => deserialized,
                Err(e) => {
                    warn!("Error deserializing ({})\n{:?}", e, json_value); // FIXME; make proper type
                    return Err(AirflowError::Other(format!(
                        "Could not deserialize structure: {}",
                        e
                    )));
                }
            },
            Err(e) => return Err(e),
        };
        trace!(target: "paged_get", "Got {} before discarding", batch.len());
        if batch.len() == 0 {
            trace!(target: "paged_get", "No results, breaking");
            break;
        }
        results.merge(batch);
        trace!(target: "paged_get",
            "Now we have {} objects after retrieving with offset {:?}",
            results.len(),
            current_offset
        );

        // If this is not a paged request, or we have consumed enough, break now.
        current_offset = current_offset + batch_limit;
        match &paging_parameters {
            None => break,
            Some(p) => {
                if results.len() >= p.limit {
                    break;
                }
                if current_offset >= p.limit {
                    break;
                }
            }
        }
    }
    Ok(results)
}

struct AirflowApi {
    url: reqwest::Url,
    username: String,
    password: String,
    client: Arc<reqwest::Client>,
}
impl AirflowApi {
    fn new(airflow_url: reqwest::Url) -> Self {
        let jar = Jar::default();
        let arcjar = Arc::new(jar);
        let c = reqwest::Client::builder()
            .cookie_provider(arcjar.clone())
            .build()
            .unwrap();
        let username = decode(airflow_url.username()).unwrap().into_owned();
        let password = decode(airflow_url.password().unwrap_or(""))
            .unwrap()
            .into_owned();
        let mut censored_url = airflow_url.clone();
        censored_url.set_username("").unwrap();
        censored_url.set_password(None).unwrap();

        AirflowApi {
            client: Arc::new(c),
            url: censored_url,
            username: username,
            password: password,
        }
    }

    async fn _get_logged_in(&self, suburl: String) -> Result<serde_json::Value, AirflowError> {
        let suburl = "api/v1/".to_string() + &suburl;
        self._get_or_login_and_get(suburl, true).await
    }

    #[async_recursion]
    async fn _get_or_login_and_get(
        &self,
        suburl: String,
        attempt_login: bool,
    ) -> Result<serde_json::Value, AirflowError> {
        let url = self.url.join(suburl.as_str()).unwrap();

        let c = self.client.clone();

        info!(target: "http_client", "GET {}", url);
        let res = match c
            .get(url)
            .header(ACCEPT, "application/json")
            .header(CONTENT_TYPE, "application/json")
            .send()
            .await
        {
            Ok(resp) => {
                let status = resp.status();
                info!(target: "http_client", "HTTP status {}", status);
                match status {
                    reqwest::StatusCode::OK => match resp.json().await {
                        Ok(json) => Ok(json),
                        Err(err) => Err(AirflowError::Other(format!(
                            "Could not decode JSON from Airflow: {}",
                            err
                        ))),
                    },
                    reqwest::StatusCode::FORBIDDEN => {
                        if attempt_login {
                            match self._login().await {
                                Ok(..) => (),
                                Err(err) => return Err(err),
                            };
                            self._get_or_login_and_get(suburl, false).await
                        } else {
                            Err(AirflowError::Other(
                                "Forbidden from Airflow -- could not log in".into(),
                            ))
                        }
                    }
                    reqwest::StatusCode::NOT_FOUND => {
                        Err(AirflowError::StatusCode(reqwest::StatusCode::NOT_FOUND))
                    }
                    other => Err(AirflowError::StatusCode(other)),
                }
            }
            Err(err) => Err(AirflowError::Other(format!(
                "Could not retrieve DAG runs: {}",
                err
            ))),
        };
        trace!(target: "http_client", "Result: {:#?}", res);
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
                        return Err(AirflowError::Other(format!(
                            "Error retrieving text that contains CSRF token: {}",
                            err
                        )))
                    }
                },
                _ => {
                    return Err(AirflowError::Other(format!(
                        "Error retrieving page for CSRF token: {}",
                        resp.status()
                    )));
                }
            },
            Err(err) => {
                return Err(AirflowError::Other(format!(
                    "Could not retrieve page for CSRF token: {}",
                    err
                )));
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
            return Err(AirflowError::Other(
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
                        return Err(AirflowError::Other(format!(
                            "Error retrieving logged-in cookie: {}",
                            err
                        )))
                    }
                },
                _ => {
                    let (status, text) = (resp.status(), resp.text().await);
                    return Err(AirflowError::Other(format!(
                        "Server rejected login with status {} and text {:?}",
                        status, text
                    )));
                }
            },
            Err(err) => {
                return Err(AirflowError::Other(format!(
                    "Could not retrieve page for login: {}",
                    err
                )));
            }
        };

        Ok(())
    }

    /// Return DAG runs from newest to oldest.
    async fn dag_runs(
        &self,
        dag_id: &str,
        limit: usize,
        offset: usize,
    ) -> Result<DagRunsResponse, AirflowError> {
        _paged_get(
            format!("dags/{}/dagRuns", dag_id),
            Some("-execution_date".into()),
            Some(PagingParameters { limit, offset }),
            |x| self._get_logged_in(x),
        )
        .await
    }

    /// Return TaskInstances for a DAG run.
    /// Mapped tasks are not returned here.
    async fn task_instances(
        &self,
        dag_id: &str,
        dag_run_id: &str,
        limit: usize,
        offset: usize,
    ) -> Result<TaskInstancesResponse, AirflowError> {
        _paged_get(
            format!("dags/{}/dagRuns/{}/taskInstances", dag_id, dag_run_id),
            None,
            Some(PagingParameters { limit, offset }),
            |x| self._get_logged_in(x),
        )
        .await
    }

    /// Return TaskInstances for a DAG run.
    /// Mapped tasks are not returned here.
    async fn tasks(&self, dag_id: &str) -> Result<TasksResponse, AirflowError> {
        _paged_get(format!("dags/{}/tasks", dag_id), None, None, |x| {
            self._get_logged_in(x)
        })
        .await
    }

    /// Return mapped tasks of a task instance in a DAG run.
    async fn mapped_task_instances(
        &self,
        dag_id: &str,
        dag_run_id: &str,
        task_instance_id: &str,
        limit: usize,
        offset: usize,
    ) -> Result<TaskInstancesResponse, AirflowError> {
        match _paged_get(
            format!(
                "dags/{}/dagRuns/{}/taskInstances/{}/listMapped",
                dag_id, dag_run_id, task_instance_id
            ),
            None,
            Some(PagingParameters { limit, offset }),
            |x| self._get_logged_in(x),
        )
        .await
        {
            Ok(v) => Ok(v),
            Err(e) => match e {
                // Task is not mapped.
                AirflowError::StatusCode(reqwest::StatusCode::NOT_FOUND) => {
                    Ok(TaskInstancesResponse::default())
                }
                _ => Err(e),
            },
        }
    }

    /// Return mapped tasks of a task instance in a DAG run.
    async fn xcom_entry(
        &self,
        dag_id: &str,
        dag_run_id: &str,
        task_instance_id: &str,
        map_index: Option<usize>,
        xcom_key: &str,
    ) -> Result<XComEntryResponse, AirflowError> {
        let suburl = format!(
            "dags/{}/dagRuns/{}/taskInstances/{}/xcomEntries/{}",
            dag_id, dag_run_id, task_instance_id, xcom_key
        ) + match map_index {
            Some(i) => format!("&map_index={}", i),
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
                warn!("Error deserializing ({})\n{:?}", e, json_value);
                return Err(AirflowError::Other(format!(
                    "Could not deserialize structure: {}",
                    e
                )));
            }
        }
    }
}

struct TaskInstanceTopologicalSorter {
    sorted_tasks: Vec<Arc<TasksResponseItem>>,
}

impl TaskInstanceTopologicalSorter {
    fn new(r: TasksResponse) -> Self {
        let mut all_nodes: HashMap<String, Arc<TasksResponseItem>> = HashMap::new();
        let mut ts = TopologicalSort::<String>::new();

        for task in r.tasks.into_iter() {
            let taskid = task.task_id.clone();
            let downstream_taskids: Vec<String> =
                task.downstream_task_ids.iter().map(|x| x.clone()).collect();
            all_nodes.insert(taskid.clone(), Arc::new(task));
            for subtask in downstream_taskids.iter() {
                ts.add_dependency(taskid.clone(), subtask);
            }
        }

        let mut sorted_tasks = vec![];

        loop {
            let round = ts.pop_all();
            if round.len() == 0 {
                if ts.len() != 0 {
                    panic!("cyclic dependencies: {:?}", ts);
                }
                break;
            }
            for taskid in round.iter() {
                sorted_tasks.push(all_nodes.get(taskid).unwrap().clone());
            }
        }

        Self { sorted_tasks }
    }

    fn sort_instances(&self, r: TaskInstancesResponse) -> Vec<TaskInstancesResponseItem> {
        let mut all_task_instances: HashMap<String, Vec<Rc<TaskInstancesResponseItem>>> =
            HashMap::new();

        for task_instance in r.task_instances.into_iter() {
            let taskid = task_instance.task_id.clone();
            let mapindex = task_instance.map_index.clone();
            let tasklist = all_task_instances
                .entry(taskid.clone())
                .or_insert_with(|| vec![]);
            let rctaskinstance = Rc::new(task_instance);
            match tasklist.binary_search_by(|probe| {
                let probe_idx = match probe.map_index {
                    None => 0,
                    Some(i) => i + 1,
                };
                let task_idx = match mapindex {
                    None => 0,
                    Some(i) => i + 1,
                };
                if task_idx == probe_idx {
                    Ordering::Equal
                } else if task_idx > probe_idx {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }) {
                Ok(pos) => {
                    panic!(
                        "Task instance {} {:?} cannot be already in pos {}",
                        taskid, mapindex, pos
                    )
                } // element already in vector @ `pos`
                Err(pos) => tasklist.insert(pos, rctaskinstance),
            }
        }

        let mut sorted_task_instances: Vec<TaskInstancesResponseItem> = vec![];

        for task in self.sorted_tasks.iter() {
            let task_instances = match all_task_instances.get(&task.task_id) {
                Some(t) => t,
                // We could not find a task instance named after the task.
                // That can happen, so we ignore it.
                None => continue,
            };
            for ti in task_instances.into_iter() {
                let task = ti.as_ref();
                sorted_task_instances.push(task.clone());
            }
        }

        sorted_task_instances
    }

    // FIXME: implement actual sort function for TaskInstances.
}

#[derive(Clone)]
struct Proxy {
    airflow_api: Arc<AirflowApi>,
}

#[derive(Debug)]
enum RolloutDataGatherError {
    AirflowError(AirflowError),
    RolloutPlanParseError(RolloutPlanParseError),
}

impl From<AirflowError> for RolloutDataGatherError {
    fn from(err: AirflowError) -> Self {
        Self::AirflowError(err)
    }
}

impl From<RolloutPlanParseError> for RolloutDataGatherError {
    fn from(err: RolloutPlanParseError) -> Self {
        Self::RolloutPlanParseError(err)
    }
}

// FIXME handle unwraps everywhere else!

impl Proxy {
    async fn get_rollout_data(&self) -> Result<Vec<Rollout>, RolloutDataGatherError> {
        let dag_id = "rollout_ic_os_to_mainnet_subnets";
        let dag_runs = self.airflow_api.dag_runs(dag_id, 20, 0).await?;
        let tasks = self.airflow_api.tasks(dag_id.clone()).await?;
        let sorter = TaskInstanceTopologicalSorter::new(tasks);

        let batch_identification_re = Regex::new("batch_([0-9]+)[.](.+)").unwrap();
        let mut res: Vec<Rollout> = vec![];

        for dag_run in dag_runs.dag_runs.iter() {
            let task_instances = self
                .airflow_api
                .task_instances(dag_id, dag_run.dag_run_id.as_str(), 500, 0)
                .await?;
            let sorted_task_instances = sorter.sort_instances(task_instances);

            let mut rollout = Rollout::new(
                dag_run.dag_run_id.to_string(),
                dag_run.note.clone(),
                dag_run.logical_date,
            );

            for task_instance in sorted_task_instances {
                if task_instance.task_id == "schedule" {
                    match task_instance.state {
                        Some(TaskInstanceState::Skipped)
                        | Some(TaskInstanceState::Removed)
                        | None => {
                            ();
                        }
                        Some(TaskInstanceState::UpForRetry)
                        | Some(TaskInstanceState::Restarting) => {
                            rollout.state = RolloutState::Problem;
                        }
                        Some(TaskInstanceState::Failed)
                        | Some(TaskInstanceState::UpstreamFailed) => {
                            rollout.state = RolloutState::Failed;
                        }
                        Some(TaskInstanceState::UpForReschedule)
                        | Some(TaskInstanceState::Running)
                        | Some(TaskInstanceState::Deferred)
                        | Some(TaskInstanceState::Queued)
                        | Some(TaskInstanceState::Scheduled) => {
                            rollout.state = RolloutState::Preparing
                        }
                        Some(TaskInstanceState::Success) => {
                            let schedule_xcom = match self
                                .airflow_api
                                .xcom_entry(
                                    dag_id,
                                    dag_run.dag_run_id.as_str(),
                                    task_instance.task_id.as_str(),
                                    task_instance.map_index,
                                    "return_value",
                                )
                                .await
                            {
                                Ok(schedule) => schedule,
                                Err(AirflowError::StatusCode(reqwest::StatusCode::NOT_FOUND)) => {
                                    rollout.state = RolloutState::Preparing;
                                    continue;
                                }
                                Err(e) => return Err(RolloutDataGatherError::AirflowError(e)),
                            };
                            let schedule =
                                RolloutPlan::from_python_string(schedule_xcom.value.clone())?;
                            rollout.batches = schedule.batches;
                            rollout.state = RolloutState::Waiting;
                        }
                    }
                } else if task_instance.task_id == "wait_for_other_rollouts"
                    || task_instance.task_id == "wait_for_revision_to_be_elected"
                {
                    match task_instance.state {
                        Some(TaskInstanceState::Skipped)
                        | Some(TaskInstanceState::Removed)
                        | None => {
                            ();
                        }
                        Some(TaskInstanceState::UpForRetry)
                        | Some(TaskInstanceState::Restarting) => {
                            rollout.state = RolloutState::Problem;
                        }
                        Some(TaskInstanceState::Failed)
                        | Some(TaskInstanceState::UpstreamFailed) => {
                            rollout.state = RolloutState::Failed;
                        }
                        Some(TaskInstanceState::UpForReschedule)
                        | Some(TaskInstanceState::Running)
                        | Some(TaskInstanceState::Deferred)
                        | Some(TaskInstanceState::Queued)
                        | Some(TaskInstanceState::Scheduled)
                        | Some(TaskInstanceState::Success) => rollout.state = RolloutState::Waiting,
                    }
                } else if let Some(capped) =
                    batch_identification_re.captures(task_instance.task_id.as_str())
                {
                    let (batch, task_name) = (
                        match rollout
                            .batches
                            .get_mut(&usize::from_str(&capped[1]).unwrap())
                        {
                            Some(batch) => batch,
                            None => continue,
                        },
                        &capped[2],
                    );

                    match task_instance.state {
                        Some(TaskInstanceState::Skipped) | Some(TaskInstanceState::Removed) => {
                            batch.set_subnet_state(
                                SubnetRolloutState::Skipped,
                                task_instance.map_index,
                            );
                        }
                        Some(TaskInstanceState::UpForRetry)
                        | Some(TaskInstanceState::Restarting) => {
                            batch.set_subnet_state(
                                SubnetRolloutState::Error,
                                task_instance.map_index,
                            );
                            rollout.state = RolloutState::Problem;
                        }
                        Some(TaskInstanceState::Failed)
                        | Some(TaskInstanceState::UpstreamFailed) => {
                            batch.set_subnet_state(
                                SubnetRolloutState::Error,
                                task_instance.map_index,
                            );
                            rollout.state = RolloutState::Failed;
                        }
                        Some(TaskInstanceState::Success)
                        | Some(TaskInstanceState::UpForReschedule)
                        | Some(TaskInstanceState::Running)
                        | Some(TaskInstanceState::Deferred)
                        | Some(TaskInstanceState::Queued)
                        | Some(TaskInstanceState::Scheduled) => {
                            batch.set_subnet_state(
                                match task_name {
                                    "collect_batch_subnets" => SubnetRolloutState::Pending,
                                    "wait_until_start_time" => SubnetRolloutState::Waiting,
                                    "create_proposal_if_none_exists" | "request_proposal_vote" => {
                                        SubnetRolloutState::Proposing
                                    }
                                    "wait_until_proposal_is_accepted" => {
                                        SubnetRolloutState::WaitingForElection
                                    }
                                    "wait_for_replica_revision" => {
                                        SubnetRolloutState::WaitingForAdoption
                                    }
                                    "wait_until_no_alerts" => {
                                        SubnetRolloutState::WaitingForAlertsGone
                                    }
                                    "join" => match task_instance.state {
                                        Some(TaskInstanceState::Success) => {
                                            SubnetRolloutState::Complete
                                        }
                                        _ => SubnetRolloutState::WaitingForAlertsGone,
                                    },
                                    // Maybe here we just want to log error and continue for robustness?
                                    &_ => panic!("impossible task name {}", task_instance.task_id),
                                },
                                task_instance.map_index,
                            );
                            rollout.state = RolloutState::UpgradingSubnets;
                        }
                        None => (),
                    }
                } else if task_instance.task_id == "upgrade_unassigned_nodes" {
                    match task_instance.state {
                        Some(TaskInstanceState::Skipped)
                        | Some(TaskInstanceState::Removed)
                        | None => {
                            ();
                        }
                        Some(TaskInstanceState::UpForRetry)
                        | Some(TaskInstanceState::Restarting) => {
                            rollout.state = RolloutState::Problem;
                        }
                        Some(TaskInstanceState::Failed)
                        | Some(TaskInstanceState::UpstreamFailed) => {
                            rollout.state = RolloutState::Failed;
                        }
                        Some(TaskInstanceState::UpForReschedule)
                        | Some(TaskInstanceState::Running)
                        | Some(TaskInstanceState::Deferred)
                        | Some(TaskInstanceState::Queued)
                        | Some(TaskInstanceState::Scheduled)
                        | Some(TaskInstanceState::Success) => {
                            rollout.state = RolloutState::UpgradingUnassignedNodes
                        }
                    }
                }
            }

            if let Some(state) = Some(&dag_run.state) {
                match state {
                    DagRunState::Success => rollout.state = RolloutState::Complete,
                    DagRunState::Failed => rollout.state = RolloutState::Failed,
                    _ => (),
                }
            }

            res.push(rollout);
        }

        Ok(res)
    }
}

#[debug_handler]
async fn handler(
    State(state): State<Arc<Proxy>>,
) -> Result<Json<Vec<Rollout>>, (StatusCode, String)> {
    match state.get_rollout_data().await {
        Ok(rollouts) => Ok(Json(rollouts)),
        Err(e) => match e {
            RolloutDataGatherError::AirflowError(AirflowError::StatusCode(c)) => {
                Err((c, "Internal server error".to_string()))
            }
            RolloutDataGatherError::AirflowError(AirflowError::Other(msg)) => {
                Err((StatusCode::INTERNAL_SERVER_ERROR, msg))
            }
            RolloutDataGatherError::RolloutPlanParseError(parse_error) => Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("{:?}", parse_error),
            )),
        },
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let backend_host = env::var("BACKEND_HOST").unwrap_or("127.0.0.1:4174".to_string());
    let airflow_url_str =
        env::var("AIRFLOW_URL").unwrap_or("http://admin:password@localhost:8080/".to_string());
    let airflow_url = Url::parse(&airflow_url_str).unwrap();

    let proxy = Arc::new(Proxy {
        airflow_api: Arc::new(AirflowApi::new(airflow_url)),
    });
    /*
    match proxy.get_rollout_data().await {
        Ok(rollouts) => {
            for rollout in rollouts.iter() {
                println!("\n{:#?}", rollout)
            }
        }
        Err(e) => println!("{:?}", e),
    };*/

    //exit(0);

    let app = Router::new().route("/api/v1/rollouts", get(handler).with_state(proxy));
    let addr: SocketAddr = backend_host.parse().unwrap();

    axum_server::bind(addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use serde_json::{from_str, json};
    use std::collections::VecDeque;
    use std::sync::Mutex;

    #[derive(Default, Deserialize, Serialize, Debug)]
    struct Response {
        max_elements: usize,
        elements: VecDeque<String>,
    }

    impl Pageable for Response {
        fn is_empty(&self) -> bool {
            self.elements.is_empty()
        }
        fn max_entries(&self) -> usize {
            self.max_elements
        }
        fn len(&self) -> usize {
            self.elements.len()
        }
        fn next_id(&self) -> Option<String> {
            match self.elements.is_empty() {
                true => None,
                false => Some(self.elements[0].clone()),
            }
        }
        fn drop_next(&mut self) -> () {
            if !self.elements.is_empty() {
                self.elements.pop_front().unwrap();
            }
        }
        fn has_id(&self, id: &String) -> bool {
            self.elements
                .iter()
                .find(|element| **element == *id)
                .is_some()
        }
        fn consume_next_n(&mut self, n: usize, mut other: Response) -> usize {
            let mut consumed = 0;
            while !other.elements.is_empty() && consumed < n {
                let element = other.elements.pop_front().unwrap();
                self.elements.push_back(element);
                consumed += 1;
            }
            self.max_elements = other.max_elements;
            consumed
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
        /*let current = */
        if adjustment.is_empty() {
            adjustment.push_front(1);
            0
        } else {
            let val = adjustment.pop_front().unwrap();
            offset = offset - val;
            adjustment.push_front(val + 1);
            limit = limit + val;
            val
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
        // println!("  Returning {:?}", unserded);
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
            c,
        )
        .await
        .unwrap();
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
                limit: 11,
                offset: 0,
            }),
            c,
        )
        .await
        .unwrap();
        assert_eq!(
            response.elements,
            lst(&["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"])
        );

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
