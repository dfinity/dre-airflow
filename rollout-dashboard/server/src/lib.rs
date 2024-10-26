//! Rollout dashboard interoperability library.
//!
//! The rollout dashboard is a service that provides up-to-date
//! access to rollout information.  This service provides a REST
//! endpoint to obtain that information under `/api/v1`.  The
//! endpoint is unauthenticated and data returned to the client
//! is always encoded as JSON using the serde serialization library.
//! Clients are encouraged to use the types available in this library
//! to deserialize responses from the API calls.
//!
//! The only REST call that exists today is `GET /api/v1/rollouts`.
//!
//! * When successful, it returns a [`types::Rollouts`] instance
//!   within an HTTP 200 response code.
//! * When there are no rollouts available yet (because the service
//!   has just begun loading), an HTTP 204 No Content status is returned.
//! * In case of errors contacting Airflow or processing its data, it will
//!   return an HTTP status code and an appropriate error message.
//!
//! The call also exists in a server-side events streaming flavor,
//! available by appending `/sse` to the rollouts API call.  The server
//! will stream the initial state right away, then stream updates to
//! the rollout data as they happen.
pub mod airflow_client;
pub mod types;
