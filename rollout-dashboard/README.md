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

### Necessary for production

* Make Rust server handle serving static content for production.
* Actually build production container, which requires making the static
  files built by NPM available to the Rust server so the Rust server
  can serve them to the browser client.
  * These must be built with a static set of versions for npm and npx
    in a separate container designed to build the static files.
* Solve all Cargo warnings and remove all `unwrap()`s in favor of proper
  error handling.

### Wishlist

* Optimize HTTP requests to happen only when the Airflow API says there has been an update.
  * Further optimize by only querying for data that is known to have changed and no more.
  * This will require important changes to the data structures used to hold the rollout state.
* Parallelize API requests when it makes sense.
* Favicon.ico!
* Send change updates to all connected browsers via WebSocket or somesuch instead of making each client XHR repeatedly.
* Make backend improve errors by returning JSON   structured errors so the frontend can show reasonable things.

## Things this project uses:

This project uses

* [TailwindCSS](https://tailwindcss.com/)
* [Flowbite](https://flowbite-svelte.com/docs/components)
* [Flowbite icons](https://flowbite.com/icons/)