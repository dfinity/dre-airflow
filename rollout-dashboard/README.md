# Rollout dashboard

## Setting up a development environment

Make sure you have set up Airflow on the root of this repository with
program `bin/airflow setup`, and that you know its administrative
user and password.

Make sure your system has the latest Rust stable development environment.

Then set up `nvm` on your user profile:

```sh
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash
# follow the instructions onscreen, then
nvm install 20
```

## Running under development environment

Start your local Airflow on the root of this repository with
`bin/airflow standalone`.  This starts Airflow on port 8080.

Start the backend by setting environment variable `AIRFLOW_URL` to
`localhost:8080` with your local Airflow API including administrative
credentials (e.g. `http://admin:password@localhost:8080/`), then change
into directory `server`, then run `cargo run`.  This will run the backend
on port 4174 and connect to your local Airflow on port 8080.  To test
that the backend operates correctly, query its `/api/v1/rollouts` path
-- if you get a valid list of rollouts in the response, the backend is
operating correctly and communicating successfully with Airflow.

To run the frontend, change into directory `frontend` then run
`npm run dev` to have the HTTP server with the frontend launch on port
5173.  The frontend will contact the backend (via an internal proxy)
running on `localhost:4174` to show you the frontend.  (The JS variable
that controls to which address the internal proxy connects is named
`BASE_URL` and is defined in [vite.config.ts](frontend/vite.config.ts).
The internal proxy is used during development to avoid CORS restrictions;
it is not used in production.)

Open `http://localhost:5173/` in your browser to see the dashboard.
Changes you make to frontend code should reflect instantaneously.

## Building

To build the static parts of the frontend, use
`npm run build`.  Build files will be in folder `build/`.

## To-do

* Optimize merge() because right now it's O(n^2).
* Optimize HTTP requests to happen only when the Airflow API says there has been an update.
  * Further optimize by only querying for data that is known to have changed and no more.
  * Serve the rollout API request from cache unless there have been changes.
  * This will require important changes to the data structures used to hold the rollout state.
  * Parallelize API requests when it makes sense.
* Send change updates to all connected browsers via WebSocket or somesuch instead of making each client XHR repeatedly.
* Provide a way to switch between ongoing, failed and successful rollouts in the UI (probably using tabs or some other selector).
* Fix tests for the paged gets.  If they are broken.  Check for boundary conditions.
* Favicon.ico!
* Handle errors contacting the backend.  Make backend improve errors by returning JSON
  structured errors so the frontend can show reasonable things.
* Actually build production container, which requires making the static
  files built by NPM available to the Rust server so the Rust server
  can serve them to the browser client.

## Things this project uses:

This project uses

* [TailwindCSS](https://tailwindcss.com/)
* [Flowbite:](https://flowbite-svelte.com/docs/components)
