# Using the rollout dashboard API

## Learning about the API

To learn about the API, how to use it, and how to interpret the data
served by API calls, please consult the programming documentation
that accompanies the `rollout_dashboard` crate, available under folder
[`../server/`](../server/) by running the `cargo rustdoc` program within that
folder, and then launching the Web page it generates for you.

Please do not proceed with creating a client
of this server application until you have read that documentation.

## Tips and tricks to get useful information from the API

We also have [a document with tips and tricks](./jqtricks.md) to pull
out certain bits of interesting information from the rollouts API, that
uses JQ to massage the data into useful structures.
