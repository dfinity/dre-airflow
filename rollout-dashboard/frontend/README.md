# Rollout dashboard frontend

**Please read [the upper level README.md](../README.md) first.**

## Building the frontend and using it with the backend

To get a frontend built for use with the [rollout dashboard server](../server/),
run `npm run build` in this folder.

You can then point the server to the resulting `dist/` folder using
environment variable `FRONTEND_STATIC_DIR` when running the server
(usually a `cargo run` in the` ../server` folder).

Remember that the environment variable `FRONTEND_STATIC_DIR`, when
running the backend, should point to the folder containing the result
of the build (`dist` as built above).

## Live code editing with hot reload

Run `npm run dev`.  The server will be available at http://localhost:5173/ .
This server expects a local backend running at http://localhost:4174/ .

## Development info

### Artifact size analysiss

[dist/stats.html](dist/stats.html) (after building) contains statistics on how
large each artifact inside the bundle is.
