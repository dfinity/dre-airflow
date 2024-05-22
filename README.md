# Airflow repository of the DRE team

This repository contains:

* Airflow content artifacts (DAGs, workflows, sensors and operators).
* [A content syncer container](./airflow-content-syncer/) to help deliver these extensions.
* [A customized Airflow container](./airflow-customized/) to enable enhanced Airflow
  behaviors, such as use of CockroachDB in production.  See heading
  *In production* below.
* Documentation for working effectively with this distributed Reliability Team
  Airflow setup.

To effectively contribute code to this repository, you must first
set up a local development environment.  See below for instructions.

[[_TOC_]]

## Airflow content artifacts distributed in this repository

* Our DAGs in this repository are distributed under folder [dags](dags/README.md).
* Our custom sensors and operators are under folder [operators](plugins/operators/README.md) and [sensors](plugins/sensors/README.md).
* Library code is under folder [shared](shared/README.md).
  * See info on [how Airflow finds Python modules](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/modules_management.html)
  * The local runner automatically makes this code available
  * [When are plugins reloaded?](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/plugins.html#plugins-loading)

### Variables and connections required for workflows

The workflows (DAGs) and operators in this repository require a set
of variables and connections to be added to Airflow before they are
executed. These variables are retrieved at runtime.  If a task in
a workflow fails to retrieve a variable or a connection, the
workflow perishes.

Logged in as administrator through the UI, go to
*Top menubar -> Admin -> Variables*.  Then create the following
variables:

*
  * Key: `dfinity.ic_admin.mainnet.proposer_key_file`
  * Val: the contents of the release automation certificate PEM file used
    to roll out IC OS (ask DRE team for information, it should be in
    their vault)
  * Description: The proposal certificate for neuron 80 to be used on mainnet.

Now go to *Top menubar -> Admin -> Connections*.  Then create the following
connections (unless specified otherwise):

*
  * Connection ID: `slack.ic_os_rollout`
  * Connection type: Slack API
  * Description: The connection used for IC OS rollouts.
  * Slack API token: ask the DRE team for the API token to post
    to Slack, which should be in the DRE team vault.
*
  * Connection ID: `google_cloud_default`
  * Connection type: Google Cloud
  * Description: Used to access Google spreadsheets for rollout plan.
  * Keyfile JSON: the contents of the Google API key created
    for Airflow that our team has in the DRE team vault.
  * Scopes: https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/spreadsheets
*
  * Connection ID: `airflow_logging`
  * Connection type: Amazon Web Services
  * Description: Logging storage for Airflow.
  * AWS Access Key ID: the value of `AWS_ACCESS_KEY_ID` in K8s secret `airflow-logging`
  * AWS Secret Access Key: the value of `AWS_SECRET_ACCESS_KEY` in K8s secret `airflow-logging`
  * Extra: `{ "endpoint_url": "http://rook-ceph-rgw-ceph-store.rook-ceph.svc.cluster.local" }`
  * **Only create this connection if running under K8s**

Variables and connections are only visible to administrators.

### Developing and deploying Airflow content robustly

To ensure robust DAG runs, you must be aware of the lifecycle
these artifacts undergo, once they go past quality assurance
and they are deployed.

Delivery is discussed in detail under the *Continuous delivery*
heading, but here is a brief summary:

* You push code changes to this repository.
* Each scheduler, worker and triggerer pod picks up, every five
  minutes, the most up-to-date revision of this repository.
  This is done by the [airflow-content-syncer](./airflow-content-syncer/)
  container running on each Airflow component instance in
  production (more info under the *Continuous delivery* heading).
* When a change is detected, the contents are synced in the
  following order:
  1. the contents of [plugins/operators](plugins/operators)
  2. the contents of [plugins/sensors](plugins/sensors)
  3. the contents of [shared/*](shared/)
  4. the contents of [dags](dags)
* DAGs are loaded, but runs are not started, when the scheduler
  notices DAGs change.  DAG content is reread periodically by
  the DAG processor, and reloaded by the workers when tasks are
  dispatched by the workers.  Therefore, changes directly made
  to DAG files will affect already-running flows.  This has been
  experimentally proven in commit ce5ba7b.
* The worker and triggerer pods always run the *most up to date*
  version of the operator / sensor code you wrote, when a task
  (sensor or operator) is started.
  * The way the worker / triggerer runs a task is by running an
    entirely new process that "rehydrates the DAG" with the key
    parameters of DAG run ID, task ID, possibly a mapped task
    index ID, and possibly the task data produced by previous
    tasks already-executed by the DAG run.
  * Concretely, this is what the worker does:
    `Executing command in Celery: ['airflow', 'tasks', 'run', 'dag_name', 'taskgroup_name.task_name', 'run_id', '--local', '--subdir', 'DAGS_FOLDER/file_containing_dag.py', '--map-index', 'mapindex_int']`
* DAG runs are often long-running (could be days or weeks).

From this, the following facts hold:

* If your DAG requires operator, sensor or shared code, the
  required code better be present and bug-free, otherwise DAG
  load will fail in the scheduler, and you will not be able to
  dispatch new DAG runs or control existing DAG runs (the
  UI will show you *Broken DAG* errors onscreen).
* Any broken operator, sensor, or shared code will induce
  failures on any scheduled, deferred or future tasks that
  will execute after you pushed.
* While any existing DAG runs will *not* alter their graph
  shape, any code change you make to sensors, operators and
  shared code will *take effect immediately* on
  currently-executing DAG runs.  This means if your operator
  did thing X in the past, but now does thing Y, any future
  tasks from already-scheduled DAGs will do thing Y, instead
  of doing thing X.
* "Rehydration" implies that what the task does when it
  "rehydrates" depends on all these parameters discussed above,
  so if the code changes in ways that these parameters "mean
  something else" or otherwise become incompatible with
  currently-scheduled tasks, the currently-scheduled tasks
  will fail in hard-to-debug ways.
* Code changes must take into account that operators, sensors
  and shared code may expect certain parameters and inter-task
  data to be a certain type or shape.  If you have a DAG with
  tasks `A -> B` and `B` expects `A` to return an `int` (or `B`
  pulls `A`'s result from the XCom result table, and expects
  the result to be an `int`), then any pushes that alter `B`
  will cause all running DAGs to fail when task `B` launches.
* Task IDs are very important.  If you change the task ID
  of a task in a DAG, then any already-dispatched currently-
  running DAG runs that will attempt to "rehydrate" tasks will
  fail to "rehydrate" the task, and the DAG will simply be
  marked as failed.
* Mapped task indexes are just as important as task IDs.  If
  e.g. your DAG currently "splits into five" parallel flows
  (usually done with the `expand` method), each one of these
  flows is identified by a task index, and will be "rehydrated"
  upon execution with that task index.  Accordingly, if you
  change a DAG such that the new rendered DAG has four or six
  parallel flows, or the data each index maps to changes or
  is reordered, any currently-executing DAG runs will either get
  the wrong data, or the task will not "rehydrate" successfully
  and the DAG will fail.

Be especially judicious and careful about the changes you make
on code used by DAGs that may be running right now or may be
scheduled in the near future.

### DAGs

DAGs are workflows expressed in Python, which Airflow loads and enables
you to either execute them manually or trigger them under certain conditions.

* DAG developer reference: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html
* You can write DAGs [in the TaskFlow style](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/taskflow.html)
* Browse loaded DAGs: http://localhost:8080/home
* Browse DAG dependencies: http://localhost:8080/dag-dependencies

In production, DAGs are deployed to `$AIRFLOW_HOME/dags`.

#### Working with DAGs

Task dependencies within a DAG are typically specified with the
operators `>>` and `<<`.  Sample that demonstrates perfectly
what that means:

```
first_task >> [second_task, third_task]
third_task << fourth_task

# Results in this order (read from left to right):
#
#                  second_task
#                /
# first_task   --
#                \
# fourth_task  --- third_task
```

The reference documentation has tips on how to specify more complex
dependencies.

A DAG won't run, even if manually started, unless it is enabled.  In
the DAG list, you will see a switch to turn each DAG on or off.

DAGs are automatically reloaded by the standalone Airflow process a few
seconds after you save changes to the files under the DAGs folder.  To
force a reload on the spot:

```
bin/reload-dags
```

The web interface will tell you at the top if there was a problem loading a DAG.

If you rename a DAG or its ID, you will have to delete the old DAG
through the CLI or the web interface.

#### Testing DAGs

By convention, DAGs can be tested by running them so:

```
bin/run-dag dags/<DAG file name> [parameters...]
```

That command would
[test-runs your DAG](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/debug.html).
This works because there's a snippet at the end of each DAG,
somewhat similar to this:

```
if __name__ == "__main__":
    dag.test()
```

**The test run actually executes the operators!**  This means you are
actually testing everything in an integrated fashion.

Test DAG runs are recorded in the local development environment's
database as well.  You can browse DAG runs using the web interface
at http://localhost:8080/dagrun/list/

More on testing DAGs:

* [Test-running DAGs interactively](https://airflow.apache.org/docs/apache-airflow/2.6.1/core-concepts/executor/debug.html).

TBD:

* local mock testing story (test DAG dependencies and dry-run)
* CI/CD testing story

### Operators

Operators are Python classes defined in standalone files under the
[operators](plugins/operators) folder.  They are run by workers after the
tasks that require them get dispatched to the workers.

* Operator developer reference: https://airflow.apache.org/docs/apache-airflow/stable/howto/custom-operator.html
* Useful knowledge on how to develop operators: https://kvirajdatt.medium.com/airflow-writing-custom-operators-and-publishing-them-as-a-package-part-2-3f4603899ec2

In production, operators are deployed to `$AIRFLOW_HOME/plugins/operators`.

#### Working with operators

Your local Airflow instance should reload operator code when you
make changes.  If it does not, simply restart your Airflow instance.

#### Testing operators

Unit tests are in directory [tests](tests/).  You can run them
directly from Visual Studio Code, or run them via Make using
`make test`.

To run an operator, just write code at the bottom of its file
that does something like this:


```py
if __name__ == "__main__":
    import sys

    if sys.argv[1] == "my_operator":
        kn = MyOperator(
            task_id="x",
            arg1=sys.argv[2],
            arg2=sys.argv[3],
        )
        kn.execute({})
```

Then you can run it under the Airflow environment:

```
bin/run-dag plugins/operators/<operator file name>
```

TBD:

* CI/CD testing story

### Sensors

A sensor is a special type of operator which has one job: to wait for
something to happen.

* Sensor reference documentation: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/sensors.html
* Sensor API documentation: https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/sensors/base/index.html
* Useful sensor information: https://marclamberti.com/blog/airflow-sensors/

Our sensors are defined in standalone files under the
[sensors](plugins/sensors) folder.

In production, sensors are deployed to `$AIRFLOW_HOME/plugins/sensors`.

#### Working with sensors

To have your Airflow development instance reload sensors, use the same procedure
as the procedure to have Airflow reload operators.

#### Testing sensors

Everything under the *Testing operators* headline applies.

### Shared code

Library code used by DAGs, operators and sensors is available under
folder [shared](shared/).

In production, each shared folder is deployed to `$AIRFLOW_HOME/plugins`.

## Quality assurance

*TBD*: CI/CD testing story

Targets:

* ruff validation and fixups
* mypy validation
* automated unit tests
* git presubmit implementing all the above
* integration tests
* Gitlab pipeline validating all the above

## Continuous delivery

The artifacts in this repository are delivered to the relevant
Airflow production pods by way of the `airflow-content-syncer`
container [built from this repository](airflow-content-syncer/).
Delivery is not done through the container, but rather by using
`git clone` within the container periodically, running as a
sidecar in all relevant Airflow pods.

In production, the syncer container will check which is the latest
revision of the `main` branch of the repo containing this file, and
if it differs from what is deployed in Airflow, it will redeploy from
the latest `main` branch.  The source tree can be overridden via an
environment variable `CONTENT_SYNCER_GIT_REPO_SOURCE` on the pod,
and the branch can be overridden using variable
`CONTENT_SYNCER_GIT_REPO_BRANCH`.

The container image version is referred to as `syncer_image` in
[this K8s file](https://gitlab.com/dfinity-lab/private/k8s/k8s/-/blob/main/bases/apps/airflow/deps/afvalues.yaml).
When the container image is updated, Airflow must be
[redeployed](https://gitlab.com/dfinity-lab/private/k8s/k8s/-/blob/main/bases/apps/airflow/README.md)
by updating the reference to the content syncer image in the
file linked within this paragraph, then the K8s repository
needs to have the update merged.

To determine if / when the artifacts have been synced, look at the
log of the container `airflow-content-syncer` in any of the
triggerer, scheduler or worker pods of the Airflow deployment (in
namespace `airflow`).  **The artifacts are not delivered
simultaneously to all production pods.**  There might be a divergence
of up to 5 minutes between pods syncing.

*TBD*: test these artifacts!

* Pipeline has to use the exact same version of Python
  that the Airflow container does -- possibly necessarily
  the same container itself!  The Airflow containers
  use an ancient version of Python.  We have set up the
  container with tag 2.6.2-python3.10 to be used for our
  prod setup.

## Local development environment setup

To get the right libraries loaded into your IDE, you will need a
virtual environment with them installed.  Run `bin/airflow setup`
to get yourself set up.  You can then tell your IDE to use the
specific venv `python3` binary under the folder `venv` in this
repository.

To actually run tests or Airflow itself, the `setup` subcommand will
set up an Airflow directory instance.  The folder is `airflow` under
this repository.

Once setup, the `bin/airflow` command can be used anytime you want
to invoke the Airflow CLI.  It takes care of setting up the environment
so everything works as expected.  With it, you can run
`bin/airflow standalone` as well as tests such as DAG tests.

The first time you run the standalone server, it will create an
admin user and password and start by listening on HTTP port 8080.
Note the password that appears on the terminal, and log in.  After
logging in, you can change the admin user password to a simple password,
through the web interface on the top right corner menu.

Note: if you also followed the instructions on how to run Airflow on a VM
as indicated below, and the VM is running, Airflow locally will not be
able to open TCP port 8080, since the VM will have hogged the port.

Now restart any running `bin/airflow standalone` instances.  The demo DAGs
will be gone now.

To actually *run* the DAGs here, you will almost certainly have to
create the required variables on your Airflow instance.  See the
heading *Variables required for workflows* for instructions on
what to create.

### Configuration

The `airflow.cfg` file under folder `$AIRFLOW_HOME` controls most aspects
of the configuration in Airflow.  It is an INI-formatted file.

The complete configuration reference is available [here](https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html).

### CLI

The CLI, named `airflow` lets you manage Airflow objects and do all sorts
of low-level operations.

By default the CLI uses a local client that expects `AIRFLOW_HOME` to point
to the Airflow home directory that contains the database.  This can
be changed in the Airflow configuration file by specifying a different value
for the configuration key `cli.api_client`; for example, the `json_client`
will make the CLI speak to an Airflow API server at the endpoint specified
by the `cli.endpoint_url` configuration key.

See [https://airflow.apache.org/docs/apache-airflow/stable/howto/usage-cli.html]
for general information on the CLI.

### Web interface

The CLI command `airflow webserver` starts a Web interface (by default on
port 8080) which lets you do many of the tasks that you can do with
the Airflow CLI.  You will need an authorized user account to log into
Airflow through the Web interface.

The web server is a Flask application.  The Flask parameters can be controlled
through file `webserver_config.py` in folder `$AIRFLOW_HOME`.  Authentication
and authorization sources can be set up there as well.  Documentation on this
can be found [here](https://airflow.apache.org/docs/apache-airflow/stable/howto/set-config.html#configuring-flask-application-for-airflow-webserver).

### API server

Airflow has an API server embedded in its web server.

References:

* https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html
* https://hevodata.com/learn/airflow-rest-api/#5

### Notes on the operation of Airflow

Changing DAGs, operators, sensors or other shared code on a running system
will not affect the the execution of tasks currently executing on any flow
currently running.  However, they will affect the execution of future tasks
instantiated by any currently-running flow (effectively, each task runs
an `airflow tasks ...` command, which independently loads the DAG,
instantiates the tasks, locates the task it is supposed to execute, and
runs it — all of which is materialized at runtime).  Therefore, to avoid
failures, changes to long-running flows should be careful to be compatible
with any running flows at the time of rollout, or should wait until the
currently-running flows which may be impacted have finished.

### Deploying a bare-bones Airflow instance to a VM

Some people prefer to use VMs for testing.  VMs can also run programs
that may not be available or installable in local development environments.

The following shellcode deploys Airflow (at a specific version) in a
running Fedora 38 VM as root, using the minimalistic SQLite database.

```
cd /opt/airflow || { mkdir -p /opt/airflow && cd /opt/airflow ; }
dnf install -y python3-pip
test -f bin/pip3 || python3 -m venv .
bin/pip3 install "apache-airflow[celery]==2.6.1" \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.6.1/constraints-3.7.txt
export PATH=$PWD/bin:$PATH
export AIRFLOW_HOME="$PWD"/var
airflow db init
```

Maintained up-to-date instructions are available [here](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html#using-pypi).

To run the Airflow web server from a terminal after installation, you must first
create a user account if not already created:

```
cd /opt/airflow
export PATH=$PWD/bin:$PATH
export AIRFLOW_HOME="$PWD"/var
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname Istrator \
  --role Admin \
  --email noreply@dfinity.org
# You will be prompted for a password.  Write it down after typing it.
```

Now you can run and log in (by default runs on port 8080):

```
cd /opt/airflow
export PATH=$PWD/bin:$PATH
export AIRFLOW_HOME="$PWD"/var
airflow standalone &
```

## In production

Airflow in Kubernetes uses [a customized container](./airflow-customized/)
which adds functionality we need, such as the ability to connect to
CockroachDB (our production database).

This container is built and pushed to this repository's container artifact
repo.  When a change that needs deployment must be changed, the
[Airflow on Kubernetes app](https://dfinity-lab.gitlab.io/private/k8s/k8s/#/bases/apps/airflow/)
in the Kubernetes repository must be updated.

See the `defaultAirflowTag` variable on the Airflow Helm chart used by that
Kubernetes app.

## Concepts

### Architecture of Airflow

The [report](docs/report/README.md) contains an outline of the architecture
of Airflow.

### Flows (DAGs)

Flows (DAGs) are directed acyclic graphs written in Python, using Airflow
primitives to put them together.  Airflow executes these flows for you.
Like command executions or processes in a UNIX shell, each DAG can be
instantiated multiple times, and each instantiation creates a *DAG run*.

DAGs can be discovered and loaded from the Python `sys.path` environment.
They are also loaded and discovered from the path specified in the
`core.dags_folder` key on the Airflow configuration file.

You can list all DAGs available using the `airflow dags list` command.
They also appear listed under the *DAGs* tab of the Airflow web interface.

* Here is [an example DAG](https://github.com/apache/airflow/blob/main/airflow/example_dags/example_bash_operator.py).
* Here is a [basic tutorial on how you write DAGs](https://marclamberti.com/blog/airflow-dag-creating-your-first-dag-in-5-minutes/).
* Here is [the official documentation on Airflow dags](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html).

### Tasks

Tasks are the building blocks of DAGs.  Each node in a DAG is a task.

The scheduler dispatches to workers tasks to be executed during DAG runs.
When the scheduler is running a DAG, each task in the DAG run becomes a task
instance in the worker, invoking the corresponding operator.

What the worker runs concretely is what the task directs it to.  Therefore,
resources, inputs and sinks needed for tasks to run must be available to
the worker for the task to succeed.

* [Tasks in Airflow](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html).

### Operators

An operator is a module that specifies an operation which would normally be
a task node in the DAG.  Think of an operator as a template for a task.

* [Operators in Airflow](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/operators.html).
* [Built-in operator index](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/operators/index.html).
