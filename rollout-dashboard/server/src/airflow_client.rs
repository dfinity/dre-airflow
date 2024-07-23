// FIXME remove all use of unwrap().
// FIXME tolerate other types of error not just AirflowError.
// FIXME make AirflowError more explanatory, not just ::Other()

use async_recursion::async_recursion;
use chrono::{DateTime, TimeDelta, Utc};
use log::{debug, info, trace, warn};
use regex::Regex;
use reqwest::cookie::Jar;
use reqwest::header::{ACCEPT, CONTENT_TYPE, REFERER};
use serde::de::Error;
use serde::{Deserialize, Deserializer};
use std::cmp::min;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::f64;
use std::future::Future;
use std::sync::Arc;
use std::{vec, vec::Vec};
use urlencoding::decode;
/// Default maximum batch size for paged requests in Airflow.
const MAX_BATCH_SIZE: usize = 100;

trait Pageable {
    fn len(&self) -> usize;
    /// Append all elements from other into self (to the end
    /// of the storage), replacing any element that already
    /// exists in self with the corresponding element in other.
    fn merge(&mut self, other: Self) -> ();
    /// Remove all elements from the end beyond usize - 1.
    /// The modified structure has maximum max_entries elements.
    fn truncate(&mut self, max_entries: usize) -> ();
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum DagRunState {
    Queued,
    Running,
    Success,
    Failed,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DagRunsResponseItem {
    pub conf: HashMap<String, serde_json::Value>,
    pub dag_run_id: String,
    pub dag_id: String,
    pub logical_date: DateTime<Utc>,
    pub start_date: Option<DateTime<Utc>>,
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
    fn merge(&mut self, other: Self) -> () {
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
    fn truncate(&mut self, max_entries: usize) -> () {
        if self.dag_runs.len() > max_entries {
            self.dag_runs.truncate(max_entries)
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct XComEntryResponse {
    pub key: String,
    pub timestamp: DateTime<Utc>,
    pub execution_date: DateTime<Utc>,
    #[serde(deserialize_with = "negative_is_none")]
    pub map_index: Option<usize>,
    pub task_id: String,
    pub dag_id: String,
    pub value: String,
}

#[derive(Debug, Deserialize, Clone)]
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
pub struct TaskInstancesResponseItem {
    pub task_id: String,
    pub task_display_name: String,
    pub dag_id: String,
    pub dag_run_id: String,
    pub execution_date: DateTime<Utc>,
    pub start_date: Option<DateTime<Utc>>,
    pub end_date: Option<DateTime<Utc>>,
    pub duration: Option<f64>,
    pub state: Option<TaskInstanceState>,
    pub try_number: usize,
    #[serde(deserialize_with = "negative_is_none")]
    pub map_index: Option<usize>,
    pub max_tries: usize,
    pub operator: Option<String>,
    pub rendered_map_index: Option<String>,
    pub note: Option<String>,
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
    fn merge(&mut self, other: Self) -> () {
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
    fn truncate(&mut self, max_entries: usize) -> () {
        if self.task_instances.len() > max_entries {
            self.task_instances.truncate(max_entries)
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct TasksResponseItem {
    pub task_id: String,
    pub task_display_name: String,
    pub owner: String,
    pub start_date: Option<DateTime<Utc>>,
    pub end_date: Option<DateTime<Utc>>,
    // FIXME: make this into a TriggerRule enum.
    // https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html#operation/get_tasks
    pub trigger_rule: String,
    pub is_mapped: bool,
    pub wait_for_downstream: bool,
    pub retries: f64,
    #[serde(deserialize_with = "airflow_timedelta")]
    pub execution_timeout: Option<TimeDelta>,
    #[serde(deserialize_with = "airflow_timedelta")]
    pub retry_delay: Option<TimeDelta>,
    pub retry_exponential_backoff: bool,
    pub ui_color: String,
    pub ui_fgcolor: String,
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
    fn merge(&mut self, other: Self) -> () {
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
    fn truncate(&mut self, max_entries: usize) -> () {
        if self.tasks.len() > max_entries {
            self.tasks.truncate(max_entries)
        }
    }
}

#[derive(Debug)]
pub enum AirflowError {
    StatusCode(reqwest::StatusCode),
    ReqwestError(reqwest::Error),
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

        debug!(target: "paged_get",
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
                    warn!("Error deserializing ({})\n{:?}", e, json_value); // FIXME; make proper type
                    return Err(AirflowError::Other(format!(
                        "Could not deserialize structure: {}",
                        e
                    )));
                }
            },
            Err(e) => return Err(e),
        };
        let batch_len = batch.len();
        trace!(target: "paged_get", "Got {} before discarding", batch_len);
        results.merge(batch);
        trace!(target: "paged_get",
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
        current_offset = current_offset + batch_limit;
    }
    Ok(results)
}

pub struct AirflowClient {
    url: reqwest::Url,
    username: String,
    password: String,
    client: Arc<reqwest::Client>,
}
impl AirflowClient {
    pub fn new(airflow_url: reqwest::Url) -> Self {
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

        Self {
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
            Err(err) => Err(AirflowError::ReqwestError(err)),
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
                return Err(AirflowError::ReqwestError(err));
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
                return Err(AirflowError::ReqwestError(err));
            }
        };

        Ok(())
    }

    /// Return DAG runs from newest to oldest.
    pub async fn dag_runs(
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
    pub async fn task_instances(
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
    pub async fn tasks(&self, dag_id: &str) -> Result<TasksResponse, AirflowError> {
        _paged_get(format!("dags/{}/tasks", dag_id), None, None, |x| {
            self._get_logged_in(x)
        })
        .await
    }

    /// Return mapped tasks of a task instance in a DAG run.
    pub async fn mapped_task_instances(
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
    pub async fn xcom_entry(
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
        fn merge(&mut self, other: Response) -> () {
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
        fn truncate(&mut self, max_entries: usize) -> () {
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
            offset = offset - val;
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
