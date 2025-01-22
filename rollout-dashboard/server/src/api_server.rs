use crate::live_state::{AirflowStateSyncer, Live, RolloutInfoCacheResponse, RolloutsView};
use async_stream::try_stream;
use axum::extract::Query;
use axum::http::StatusCode;
use axum::response::sse;
use axum::response::Sse;
use axum::routing::get;
use axum::{Json, Router};
use futures::stream::Stream;
use log::debug;
use rollout_dashboard::types::Rollout;
use rollout_dashboard::types::RolloutEngineState;
use rollout_dashboard::types::RolloutsViewDelta;
use serde::{de, Deserialize, Deserializer};
use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::Infallible;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use tokio::time::Duration;

/// Serde deserialization decorator to map empty Strings to None,
fn empty_value_as_true<'de, D, T>(de: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: FromStr,
    T::Err: fmt::Display,
{
    let opt = Option::<String>::deserialize(de)?;
    match opt.as_deref() {
        Some("") | None => FromStr::from_str("true")
            .map_err(de::Error::custom)
            .map(Some),
        Some(s) => FromStr::from_str(s).map_err(de::Error::custom).map(Some),
    }
}

#[derive(Deserialize)]
struct SseHandlerParameters {
    #[serde(default, deserialize_with = "empty_value_as_true")]
    incremental: Option<bool>,
}

pub(crate) struct ApiServer {
    state_syncer: Arc<AirflowStateSyncer<Live>>,
}

impl ApiServer {
    pub fn new(state_syncer: Arc<AirflowStateSyncer<Live>>) -> Self {
        Self { state_syncer }
    }

    // #[debug_handler]
    async fn get_rollout_data(&self) -> Result<Json<VecDeque<Rollout>>, (StatusCode, String)> {
        match self.state_syncer.get_current_rollout_status().await {
            Ok((_, rollouts)) => Ok(Json(rollouts)),
            Err(e) => Err(e),
        }
    }

    // #[debug_handler]
    async fn get_engine_state(&self) -> Result<Json<RolloutEngineState>, (StatusCode, String)> {
        match self.state_syncer.get_current_rollout_status().await {
            Ok((state, _)) => Ok(Json(state)),
            Err(e) => Err(e),
        }
    }

    // #[debug_handler]
    async fn get_cache(&self) -> Result<Json<Vec<RolloutInfoCacheResponse>>, (StatusCode, String)> {
        Ok(Json(self.state_syncer.get_cache().await))
    }

    /// Produce an SSE stream structured as a dictionary:
    /// * rollouts: appears and contains a list of Rollout when rollouts have been updated and
    ///             the caller did not indicate delta_support.  If the caller indicated
    ///             delta_support, then this appears only on initial connection, or after a
    ///             backend error has been reported in a prior message.
    /// * error: always appears, but only contains an error (is non-null) when there was an
    ///          error polling Airflow.
    /// * updated: when the caller indicates delta_support, this appears and updated rollouts
    ///            are listed here.
    /// * deleted: when the caller indicates delta_support, this appears and lists the names
    ///            of the rollouts that have disappeared.
    /// * engine_state: explains the state of the rollout engine.  Only present if no error
    ///                 is present.
    pub fn produce_rollouts_sse_stream(
        &self,
        delta_support: bool,
    ) -> Sse<impl Stream<Item = Result<sse::Event, Infallible>>> {
        struct DisconnectionGuard {}

        impl Default for DisconnectionGuard {
            fn default() -> Self {
                debug!(target: "server::sse", "New client connected.");
                Self {}
            }
        }

        impl Drop for DisconnectionGuard {
            fn drop(&mut self) {
                debug!(target: "server::sse", "Client disconnected.");
            }
        }

        let mut stream_rx = self.state_syncer.subscribe_to_state_updates();

        let stream = try_stream! {
            // Set up something that will be dropped (thus log) when SSE is disconnected.
            let disconnection_guard = DisconnectionGuard::default();

            // Set an initial message to diff the first broadcast message against.
            let mut last_rollout_data: RolloutsView = Err((StatusCode::OK, "initial, ignored, nothing matches this".to_string()));

            loop {
                let current_rollout_data = stream_rx.borrow_and_update().clone();

                let message = match (&current_rollout_data, &last_rollout_data) {
                    // Error before.  Send full sync.
                    (Ok((engine_state, new_rollouts)), Err(_)) => Some(RolloutsViewDelta::full(engine_state, new_rollouts)),
                    // Last time was a good update.  Send differential sync.
                    (Ok((engine_state, new_rollouts)), Ok((_, old_rollouts))) => {
                        let new_names = new_rollouts.iter().map(|r| r.name.clone()).collect::<HashSet<String>>();
                        let old_rollouts_map = old_rollouts.iter().map(|r| (r.name.clone(), r)).collect::<HashMap<String, &Rollout>>();
                            let updated = new_rollouts.iter().filter_map(|r| match old_rollouts_map.get(&r.name) { None => Some(r.clone()), Some(old_rollout) => match r.update_count != old_rollout.update_count {true => Some(r.clone()), false => None}}).collect::<VecDeque<Rollout>>();
                            let deleted = old_rollouts.iter().filter_map(|r| match new_names.contains(&r.name) { true => None, false => Some(r.name.clone())}).collect::<VecDeque<String>>();
                            match updated.is_empty() && deleted.is_empty() {
                                true => None,
                                false => match delta_support {
                                    false => Some(RolloutsViewDelta::full(engine_state, new_rollouts)),
                                    true => Some(RolloutsViewDelta::partial(
                                        engine_state,
                                        &updated,
                                        &deleted,
                                    )),
                                }
                            }
                    }
                    // Error after a good update.  Send error.
                    (Err(e), Ok(_)) => Some(RolloutsViewDelta::error(e)),
                    // Error after an error.  Only send update if errors differ.
                    (Err(e), Err(olde)) => match e == olde {
                        true => None,
                        false => Some(RolloutsViewDelta::error(e)),
                    },
                };

                match &message {
                    Some(m) => yield sse::Event::default().data(serde_json::to_string(m).unwrap()),
                    None => ()
                }

                last_rollout_data = current_rollout_data;
                if stream_rx.changed().await.is_err() {
                    break;
                }
            }

            // Drop the disconnection guard to log the message that the client disconnected.
            drop(disconnection_guard);
        };

        Sse::new(stream).keep_alive(
            axum::response::sse::KeepAlive::new()
                .interval(Duration::from_secs(5))
                .text("keepalive"),
        )
    }

    fn v1_api(self: Arc<Self>) -> Router {
        let rollouts_handler_ref = self.clone();
        let engine_state_handler_ref = self.clone();
        let compat_sse_handler_ref = self.clone();
        let sse_handler_ref = self.clone();
        Router::new()
            .route(
                "/rollouts",
                get(move || async move { rollouts_handler_ref.get_rollout_data().await }),
            )
            .route(
                "/engine_state",
                get(move || async move { engine_state_handler_ref.get_engine_state().await }),
            )
            .route(
                "/rollouts/sse",
                get(move |options: Query<SseHandlerParameters>| {
                    let options: SseHandlerParameters = options.0;
                    async move {
                        compat_sse_handler_ref
                            .produce_rollouts_sse_stream(options.incremental.unwrap_or_default())
                    }
                }),
            )
            .route(
                "/sse/rollouts_view",
                get(move || async move { sse_handler_ref.produce_rollouts_sse_stream(true) }),
            )
    }

    fn unstable_api(self: Arc<Self>) -> Router {
        let cached_data_handler_ref = self.clone();
        Router::new().route(
            "/cache",
            get(move || async move { cached_data_handler_ref.get_cache().await }),
        )
    }

    pub fn routes(self: Arc<Self>) -> Router {
        Router::new()
            .nest("/api/v1", self.clone().v1_api())
            .nest("/api/unstable", self.unstable_api())
    }
}
