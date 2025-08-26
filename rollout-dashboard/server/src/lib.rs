//! The rollout dashboard is a service that provides up-to-date
//! access to rollout information.  It is composed of two parts:
//!
//! * The frontend (the user interface that probably led you here).
//! * The backend server (used by the frontend to fetch data).
//!
//! ## Using the backend server API
//!
//! The backend server provides several versioned endpoints under
//! `/api`.  The endpoints are unauthenticated.
//! Data returned to the client from non-SSE requests is always encoded
//! as JSON using the serde serialization library.  When SSE requests
//! are sent, responses are in SSE events form with JSON-encoded objects
//! as payload.
//!
//! Consult the [api_server] module to learn about the request endpoints
//! supported by the API.  See the [types] library to understand the
//! various responses that the API provides.
//!
//! Clients are encouraged to use the types available in this library
//! to deserialize responses from the API calls.
//!
//! ## Learn more about the rollout dashboard
//!
//! To learn more about the rollout dashboard, please consult the
//! [README](https://github.com/dfinity/dre-airflow/tree/main/rollout-dashboard)
//! of the project.
#[doc(hidden)]
pub mod airflow_client;
pub mod api_server;
#[doc(hidden)]
pub mod live_state;
pub mod types;
