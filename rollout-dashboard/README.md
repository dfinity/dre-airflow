# Rollout dashboard

This application:

* collects information from Airflow to display in a nice easy-to-use screen,
* provides a REST endpoint for other programs to retrieve the data it collects,
* provides a client library for those programs to use the retrieved data.

In production, it is composed of two distinct pieces:

1. A Rust-based backend that periodically collects information to
   assemble in the right format, and serves it to clients via REST API.
2. A collection of compiled TypeScript and CSS files that form the
   Web client, which (when loaded by the browser) polls the backend
   and displays the data returned by the backend.  This collection of
   files is also served by the backend.

*[Find the live dashboard in production here](https://rollout-dashboard.ch1-rel1.dfinity.network/)*

## Frontend information

See [frontend/README.md](frontend/README.md) for information on the frontend.

## Production information

To upgrade the dashboard in production, simply approve the latest
PR created in the [K8s repo](https://github.com/dfinity-ops/k8s/pulls)
by this repository's release automation.  For more information,
[consult the relevant document](https://dfinity-ops.github.io/k8s/#/bases/apps/rollout-dashboard/).

The backend server must have the following environment variables
set to the correct value (though sometimes the defaults are OK):

1. `AIRFLOW_URL` set to the complete (with user and password)
   URL of the Airflow server to use.
2. `FRONTEND_STATIC_DIR` set to the path that contains the built
   `dist/client` frontend assets.
3. `BACKEND_HOST` set to the host and port you want to serve on
   (the default only listens on localhost, which is not good for
   production -- only for development).
4. `RUST_LOG` set to `info` ideally to observe at least log
   messages of info level and above.
5. `MAX_ROLLOUTS` optionally set to a nonzero positive integer
   to limit the number of rollouts (default 10).  This determines
   the number of rollouts *per rollout kind* fetched during each
   update.
6. `REFRESH_INTERVAL` optionally set to a nonzero positive integer
   as the number of seconds to wait between queries to Airflow.

## Developer information

* [How to use the dashboard API as a developer](doc/api.md)
* [How to develop the dashboard further](doc/dev.md)
