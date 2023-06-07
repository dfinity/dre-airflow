# Apache Airflow: production viability report

Airflow is a natively distributed Python-based distributed execution engine.

Airflow is stable, mature, it has a decent user interface, it has an API, it has a story for deployment of custom artifacts, and it is well-designed around a robust distributed execution model.  It is absolutely hands-down superior to StackStorm.

The purpose of the work behind this report is to discover the answers
to the following questions:

* How usable, in practice, is Airflow?
* How does one extend / build functionality with Airflow?  How easy are these tasks?
* Does Airflow have fault tolerance?  If so, to what extent?
* What is the deployment story for Airflow-powered functionality (such as workflows)?
* How does it compare to the previous workflow engine we researched (StackStorm)?
* What is the architecture of Airflow, and what are the production implications of that architecture?

Most of the practical findings discovered during the production of this report
are not not directly in the report -- they are in the main [README.md](../../README.md)
file of this repository, as instructions to the users and maintainers of the Airflow
stack within our team.

## Main documentation site

* https://airflow.apache.org/docs/

## Pending items to settle / document / implement

* Learn about Airflow deployment in Kubernetes.
  * Finish SVG graph. https://github.com/apache/airflow/tree/main/chart/templates
  * Come up with concrete story to deploy using Flux
* Decide on a method and pipeline to redeploy custom operators and DAGs.
* Hard test reliability of partial outages of the system (kill executor pod / scheduler)
* Document and implement testability story for workflows.  Local test and CI/CD.
* Rename dre-st2-pack repo to dre-airflow. 

## Architecture

A minimal Airflow setup works as follows:

* There is a web server, which lets authorized users manage various aspects
  of Airflow execution.  The web server connects to the database.  It also
  provides an API, access to which is disabled by default.
* There is a database.  Various database engines are available for use;
  the default is SQLite 3, which is unsuitable for production.
* There is a CLI.  By default, the CLI uses a local client, which touches
  files such as the database.  This can be changed by modifying the `[cli]`
  section of the `airflow.cfg` file under the folder `$AIRFLOW_HOME`.
  With the proper changes, the CLI can do everything both locally and
  remotely via the Airflow API.
* There is a scheduler (of which one can have multiple instances).  Schedulers
  schedule tasks out of flows to be executed, fanning them out to workers
  (in production) or directly executing them (in the default Airflow setup).
* There is a worker (of which one can have multiple instances).  Workers
  execute tasks as directed by schedulers.  In the default Airflow setup,
  the scheduler directly runs an executor.  In production, separate worker
  instances do the job of running tasks.

I have taken it upon myself to build a graph of the complete Airflow
architecture as per the [official Helm chart](https://github.com/apache/airflow/tree/main/chart).
Here it is:

![Airflow architecture in Kubernetes](airflow-architecture-in-kubernetes.svg)

The Apache Airflow site [hosts an architectural description](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/overview.html).

## Building blocks: flows (DAGs), tasks and operators

Flows (DAGs) are directed acyclic graphs written in Python, using Airflow primitives
to put them together.  Nodes in a flow are tasks.

How it works is the scheduler takes a DAG and begins running it, invoking each
task in the order and at the time that the DAG prescribes it.  The scheduler
produces task instances which then get fanned out to executors.  The scheduler
iterates thus over the DAG until the last task instance is finished.

Operators are Python modules that either ship with Airflow or can be installed
separately, which provide functionality like talking to APIs or running
bash / Python programs.  Tasks are implemented by operators. Naturally, one
can create own custom operators.

### How are DAGs found and loaded?

DAGs appear to be discovered and loaded from the Python `sys.path` environment.
There is also a folder specified by configuration where DAGs are loaded from.
The DRE Airflow repository that contains this document also contains
custom DAGs.

DAGs appear to be reloaded automatically when files in the DAGs folder are
changed.  Renamed DAGs leave an old DAG behind which must be deleted by hand.
How often and how fast DAGs are reloaded is controlled by configuration:

```
# after how much time a new DAGs should be picked up from the filesystem
min_file_process_interval = 0
dag_dir_list_interval = 60
```

Under Kubernetes, the workers dispatched by the Kubernetes executor need
access to the DAGs.  It is undecided as of now how we will provide updated
DAGs to the workers, but as per [this document](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/kubernetes.html)
there appears to be several ways of doing it:

* The executor pod image contains the DAGs (implies building a new executor
  pod image, and redeploying, every time a DAG changes).
* The executor pod image mounts the DAGs using a *PersistentVolume*
  (implies updating the *PersistentVolume* every time a DAG changes)
* The executor pod contains a sidecar that periodically (or upon webhook)
  redownloads the DAGs from the repository into the DAGs folder
  (specified in `airflow.cfg`).

DAGs appear to be required by the scheduler node too.  It looks like DAGs
are always [reloaded by the scheduler periodically and also before starting
a new DAG run](https://stackoverflow.com/questions/46418236/can-airflow-load-dags-file-without-restart-scheduler).

### How are operators found and loaded?

In principle, the DAG imports the Operator class and `__init__`s it.
This code runs in the scheduler and also in the process that imports
the DAGs (that creates the DagBag) to the metadata store.

The `execute()` method of the Operator is responsible for running
the process / algorithm that the Operator implements.

Operator classes can be packaged as PyPI packages too.

This all implies that the Operator must be available on both the
scheduler and the workers, under its Python path.  The second-order
implication is that we must redeploy and reload these custom operators
on changes:

* rebuild container images containing any custom operators we build,
  and redeploy them to the Kubernetes cluster (at least for the
  scheduler and the workers), or
* distribute the custom operators as a folder that can be downloaded
  by a sidecar in the relevant pods to the plugins folder (specified
  in `airflow.cfg`).

It is unclear yet if anything special needs to run to reload operator
code in workers or the scheduler when the operator code has changed.
`webserver.reload_on_plugin_change` appears to be a setting for the
webserver component only.  Need to investigate whether the setting
also affects the scheduler and the workers.

* Operator developer reference: https://airflow.apache.org/docs/apache-airflow/stable/howto/custom-operator.html
* Packaging operators: https://kvirajdatt.medium.com/airflow-writing-custom-operators-and-publishing-them-as-a-package-part-2-3f4603899ec2

## Fault tolerance

It appears that dead workers can be spotted by the scheduler.

Scheduler state is recovered from a crash as well.

* Reference: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/kubernetes.html#fault-tolerance

It is not clear at which level retry happens as of now.  I will test this
soon.

## Persistence

We will need to deploy a PostgreSQL instance, provision backups, and either
build fault tolerance for it, or pin it to a host and restore from backups
if it ever crashes.  While the Airflow Helm chart provides that, it is not
fault-tolerant out of the box.

* Chart: https://github.com/apache/airflow/tree/main/chart

## Testability story

* DAGs can be validated as documented here: https://docs.astronomer.io/learn/testing-airflow

## Production recommendations:

* https://airflow.apache.org/docs/helm-chart/stable/production-guide.html

## Appendix A: deployment of trial Airflow instance

For the purposes of testing different aspects of the Airflow deployment,
a test VM was set up.  These are the details on how that was accomplished.

The trial instance of Airflow was [deployed using the PyPI method](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html#using-pypi) on a VM running locally.  The process is documented in this repository's [README.md file](../../README.md).

Everything ran in a Fedora 38 minimal VM under QEMU (specifically using the Virtual
Machine Manager connected to a local session QEMU libvirt  driver), with port
forwardings on a separate network interface just for local use.  The necessary
libvirt XML to get the port forwarding going on the VM is:

```
  <qemu:commandline>
    <qemu:arg value="-netdev"/>
    <qemu:arg value="user,id=localnet0,net=192.168.32.0/24,hostfwd=tcp::22222-:22,hostfwd=tcp::8080-:8080"/>
    <qemu:arg value="-device"/>
    <qemu:arg value="e1000,netdev=localnet0"/>
  </qemu:commandline>
```

The firewall was shut off: `systemctl disable --now firewalld`.
