# Airflow setup used by the DRE team

This repository contains code and instructions for working with the
Distributed Reliability Team Airflow instance.

## Artifacts distributed in this repository

The DAGs in this repository are distributed under folder
[dags/](dags/README.md).

The operators are under folder [plugins/operators](plugins/operators/README.md).

### Editing DAGs

With the local development environment instructions below, DAGs are
automatically reloaded a few seconds after you save changes to the files
under the DAGs folder.  To force a reload:

```
venv/bin/python -c "from airflow.models import DagBag; d = DagBag();"
```

If you rename a DAG or its ID, you will have to delete the old DAG
through the CLI or the web interface.

### Testing DAGs

By convention, DAGs can be tested by running them under the Python
venv interpreter that has the Airflow installation (see under
*Local development environment* to get it set up):

```
venv/bin/python dags/<DAG file name>
```

That command would [test-runs your DAG](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/debug.html).
This works because there's a snippet at the end of each DAG:

```
if __name__ == "__main__":
    dag.test()
```

**The test run actually executes the operators!**  This means you are
actually testing everything in an integrated fashion.

Test DAG runs are recorded in the local development environment's
database as well.  You can browse DAG runs using the web interface.

## Local development environment

To get the right libraries loaded into your IDE:

* Create a virtual environment using your local Python interpreter.
  * Customarily you can use the folder `venv` in this project.
  * See example below where how to install Airflow to a VM is explained.
* Run the installation on the venv.  The following sample command gets you
  going with the Airflow instance.  Note that the version may vary as we move
  forward, and you may be required to update it later.

```
venv/bin/pip3 install "apache-airflow[celery]==2.6.1" \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.6.1/constraints-3.7.txt
```

Now you can tell your IDE to use the specific venv `python` binary.

To actually run tests, you will have to create an Airflow home directory,
from which you can then run Airflow or tests, and set a few environment
variables.  By convention the folder is `airflow` under this repository.
Here is how you do that:

```
export PATH=$PWD/venv/bin:$PATH
export AIRFLOW_HOME=$PWD/airflow
mkdir -p "$AIRFLOW_HOME"
ln -sf ../dags airflow/dags # link this repo's DAGs folder here
ln -sf ../plugins airflow/plugins # link this repo's plugins folder
airflow db init
```

Note the password for the `admin` user in the output of `airflow db init`.

You can now run both `airflow standalone` and tests such as DAG tests.
After logging in, you can change the admin user password to a simple
password, through the web interface on the top right corner menu.

Note: if you also followed the instructions on how to run Airflow on a VM
as indicated below, and the VM is running, Airflow locally will not be
able to open TCP port 8080, since the VM will have hogged the port.

### Removing the example DAGs that ship with Airflow

To remove these DAGs:

1. Stop any instance of `airflow standalone`.
2. Edit `core.load_examples` to False in `airflow.cfg` under the
   `airflow` folder.
3. Start your `airflow standalone instance`.

The demo DAGs will be gone.

## Deploying a bare-bones Airflow instance to a VM

The following shellcode deploys Airflow (at a specific version) in a Fedora 38 VM,
using the minimalistic SQLite database.

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

## Testing

* [Testing DAGs](https://airflow.apache.org/docs/apache-airflow/2.6.1/core-concepts/executor/debug.html).

## Administrivia

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
