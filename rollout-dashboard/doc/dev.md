# Developing the rollout dashboard

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
on port 4174 so it can talk to your local Airflow on port 8080.  To test
that the backend operates correctly, query its `/api/v1/rollouts` path
-- if you get a valid list of rollouts in the response, the backend is
operating correctly and communicating successfully with Airflow.  You
may elect to set the `RUST_LOG` variable to anything between info, debug
or trace in order to see progressively more log messages.

To run the frontend in development mode, change into directory `frontend`
then run `npm run dev` to have the HTTP server with the frontend launch on
port 5173.  The frontend will contact the backend (via an internal proxy)
running on `localhost:4174` to show you the rollout data.  (The JS variable
that controls to which address the internal proxy connects is named
`BASE_URL` and is defined in [vite.config.ts](frontend/vite.config.ts).
The internal proxy is used during development to avoid CORS restrictions;
it is not used in production.)

Open `http://localhost:5173/` in your browser to see the dashboard.
Changes you make to frontend code should reflect instantaneously thanks
to Vite / NVM / NPM, but sometimes you must reload the whole page.
This is very fast either way.

## Building the frontend static files

To build the static parts of the frontend, within the `frontend` folder
use `npm run build`.  Build files will be in folder `dist/`.

Remember that the environment variable `FRONTEND_STATIC_DIR`, when
running the backend, should point to the folder containing the result
of the build (`dist` as built above).

## Things this project uses

This project uses

* [TailwindCSS](https://tailwindcss.com/)
* [Flowbite](https://flowbite-svelte.com/docs/components)
* [Flowbite icons](https://flowbite.com/icons/)
