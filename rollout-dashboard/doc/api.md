# Using the rollout dashboard API

## Learning about the API

To learn about the API, how to use it, and how to interpret the data
served by API calls, please consult the programming documentation
that accompanies the `rollout_dashboard` crate.  This documentation
is served by the rollout dashboard UI and is accessible through the
*Documentation* link at the bottom of the dashboard.  You can also
build the documentation yourself by running `cargo rustdoc --no-deps --open`
within the [`../server/`](../server/) folder -- this will launch
your Web browser with the documentation open.

Please do not proceed with creating a client
of this server application until you have read that documentation.

## Tips and tricks to get useful information from the API

We also have [a document with tips and tricks](./jqtricks.md) to pull
out certain bits of interesting information from the rollouts API, that
uses JQ to massage the data into useful structures.
