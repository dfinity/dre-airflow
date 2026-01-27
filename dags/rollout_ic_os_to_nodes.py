# mypy: disable-error-code=unused-ignore
"""
Rollout IC os to subnets in batches.

Each batch runs in parallel.
"""

import datetime
import os
import sys
import typing

import airflow.operators.python as python_operator
import airflow.sensors.python as python_sensor
import pendulum
import sensors.ic_os_rollout as ic_os_sensor
from airflow.decorators import dag, task, task_group
from airflow.models.baseoperator import chain
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from dfinity.ic_types import IC_NETWORKS
from dfinity.rollout_types import HostOSStage
from operators import hostos_rollout as hostos_operators
from operators.ic_os_rollout import RequestProposalVote
from sensors import hostos_rollout as hostos_sensors

from airflow import __version__

# Temporarily add the DAGs folder to import defaults.py.
sys.path.append(os.path.dirname(__file__))
try:
    from defaults import DEFAULT_HOSTOS_ROLLOUT_PLANS as DEFAULT_ROLLOUT_PLANS
finally:
    sys.path.pop()

if "2.9" in __version__:
    # To be deleted when we upgrade to Airflow 2.11.
    from dfinity.ic_os_rollout import PLAN_FORM

    format = dict(custom_html_form=PLAN_FORM)
else:
    format = {"format": "multiline"}

ROLLOUT_PLAN_HELP = """\
Represents the plan that the HostOS rollout will follow.

A HostOS rollout proceeds in stages (none mandatory) that each proceed
batch by batch:

1. `canary` stage (up to `CANARY_BATCH_COUNT` batches which is 5)
2. `main` stage (up to `MAIN_BATCH_COUNT` batches which is 50)
3. `unassigned` stages (up to `UNASSIGNED_BATCH_COUNT` batches which is 15)
4. `stragglers` stage (only one batch)

If a stage key is not specified, the stage is skipped altogether.

At each batch. a selector (or a tree of selectors) decides (from a
pool of targetable HostOS nodes) which nodes are to be rolled out in
that batch:

* one selector per `canary` batch,
* a selector common to all `main` batches,
* a selector common to all `unassigned` batches,
* a selector for the single `stragglers` batch

A selector is either:

* A dictionary that specifies node selection criteria.

  This dictionary specifies at least one key of:

  * `assignment` (unassigned or assigned or API boundary),
  * `owner` (DFINITY or others),
  * `status` (Degraded, Healthy or Down).
  * `datacenter` (a datacenter ID)

  Then, based on those selection keys, a list of nodes is created by
  the batch.

  If the `group_by` property (either `datacenter` or `subnet`) is specified,
  the list of nodes is grouped further into lists based on said property.

  After the (optional) grouping, a number of nodes from each group is selected
  (up to an absolute number if the key `nodes_per_group` is an integer, or a
  0-100 percentage if the key is a number postfixed by %).

  If `subnet_healthy_threshold` is specified (requires `assignment: assigned`),
  only nodes from subnets with enough healthy nodes are included.  The threshold
  can be an integer (e.g. `7` means subnets with more than 7 healthy nodes) or a
  percentage (e.g. `80%` means subnets with more than 80% healthy nodes).

  Finally, the groups (or single group if no `group_by` was specified) are
  collated into a single list of resulting nodes.
* A dictionary that specifies an `intersect` key with a list of selectors.
  Nodes selected by the first selector of the list are used as the starting
  point for the second element of the list, and so on successively until
  all elements have been used to create a final filtered list of nodes that
  the batch will target.  If the list is empty, all nodes are selected.
* A dictionary that specifies a `join` key with a list of selectors.  The
  nodes targeted by each selector in the list are combined to produce a
  final node list that the batch will target.  If the list is empty, no nodes
  are selected.
* A dictionary with a single key `not`, containing a selector.  `not` negates
  whatever the selector within specifies.  In other words, the nodes that will
  be selected by a `not` selector are all the nodes that *do not* match the
  selector.

In a selector tree, order generally matters.  E.g. a `join` of two selectors
each selecting twenty nodes will generally select up to forty nodes, because
the first selector will "grab" twenty nodes from the pool of nodes that the
second selector will tap too.  Similarly, a `join` of a 1-node selector and
a 20%-node selector will not produce the same results as a `join` of a
20%-node selector and a 1-node selector.  The only case in which this does
not apply is within the members of an `intersect` selector, because for
an intersection to make any useful, practical sense, all its components need
to see the same candidates.

The application of selectors for each batch happens as follows:

* The batch starts with all nodes not yet rolled out to as candidates.
* The selector (or collection of selectors) is applied iteratively and recursively,
  finally forming the list of nodes that the batch will roll out, to only the
  nodes matching the selector / selector collection.  This process also returns
  all the remaining nodes that future batches may select.
* For the next batch, the process is repeated with its own selector, but
  the candidates for node selection are limited to the remaining nodes returned
  by the prior batch.

Here is an example of a selector that would select both 1 unassigned node per
datacenter that is owned by DFINITY, and 2 healthy nodes doing duty as API
boundary node:

```
...
... selectors:
      join:
      - assignment: unassigned
        owner: DFINITY
        group_by: datacenter
        nodes_per_group: 1
      - assignment: API boundary
        status: Healthy
        nodes_per_group: 2
...
...
```

In addition, each batch specification can also have an optional `tolerance`
value, which may be either a natural number (including zero), or a percentage
string (e.g. "90%"), which indicates to the rollout how many nodes (or what
percentage of the selected nodes) may fail the upgrade, while still considering
the batch successful.  This is intended to reduce uncertainty during the rollout
such that e.g. one node failing to upgrade won't stop the rollout from making
any progress completely.

The rollout has a fuse built-in that prevents rolling out to more than 160
nodes at once, so if this error takes place, the rollout will abort.  If an
empty list of nodes is the result of all selectors applied, the batch is
simply skipped and the rollout moves to the next batch (or the first batch
of the next stage, if need be).

Putting it all together, here is an abridged example of a rollout that would
only roll out three `canary` batches (to a single node each), and a series
of unassigned batches, with no `main` or `stragglers` stage:

```
stages:
  canary:
    - selectors: # for batch 1
        assignment: unassigned
        nodes_per_group: 1
    - selectors: # for batch 2
        assignment: unassigned
        nodes_per_group: 1
    - selectors: # for batch 3
        assignment: unassigned
        nodes_per_group: 1
  unassigned:
    selectors: # for all batches up to the 15th
      assignment: unassigned
      nodes_per_group: 100
      tolerance: 10% # tolerate up to 10% failures in each batch.
...
```

There are a few other configuration options -- non-stage configuration keys are
required except for `start_day` and `allowed_days`.  Some remarks:

* The `minimum_minutes_per_batch` key indicates how fast we can go per batch.
* The `start_day` key indicates the weekday (in English) when the
  first batch of the rollout should start being rolled out.
  If left unspecified, it corresponds to today.
* Batches are rolled out between the times specified in the
  `resume_at` and the `suspend_at` keys (in HH:MM format).  The time
  window between `resume_at` and `suspend_at` must be large enough
  to fit the `minimum_minutes_per_batch` value.
* All times are UTC.
* If not specified, `allowed_days` will default to weekdays (including Friday!).
"""

for network_name, network in IC_NETWORKS.items():

    @dag(
        dag_id=f"rollout_ic_os_to_{network_name}_nodes",
        schedule=None,
        start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
        catchup=False,
        dagrun_timeout=datetime.timedelta(days=45),
        tags=["rollout", "DRE", "IC OS", "HostOS"],
        render_template_as_native_obj=True,
        params={
            "git_revision": Param(
                "0000000000000000000000000000000000000000",
                type="string",
                pattern="^[a-f0-9]{40}$",
                title="Git revision",
                description="Git revision of the IC OS HostOS release to roll out to"
                " subnets;"
                " the version must have been elected before but the rollout will"
                " verify that before proceeding.",
            ),
            "plan": Param(
                default=DEFAULT_ROLLOUT_PLANS[network_name].strip(),
                type="string",
                title="Rollout plan",
                description_md=ROLLOUT_PLAN_HELP,
                **format,
            ),
            "simulate": Param(
                True,
                type="boolean",
                title="Simulate",
                description="If enabled (the default), all steps will be simulated.",
            ),
        },
    )
    def rollout_ic_os_to_nodes() -> None:
        retries = int(86400 / 60 / 5)  # one day worth of retries

        schedule = task(hostos_operators.schedule)

        timetable = schedule(network=network)  # type: ignore
        batches = []
        batch_name: HostOSStage
        for batch__name, batch_count in [
            ("canary", hostos_operators.CANARY_BATCH_COUNT),
            ("main", hostos_operators.MAIN_BATCH_COUNT),
            ("unassigned", hostos_operators.UNASSIGNED_BATCH_COUNT),
            ("stragglers", hostos_operators.STRAGGLERS_BATCH_COUNT),
        ]:
            batch_name = typing.cast(HostOSStage, batch__name)
            for batch_index in range(batch_count):

                @task_group(
                    group_id=hostos_operators.stage_name(batch_name, batch_index)
                )
                def batch(stage: HostOSStage, batch_index: int) -> None:
                    nodes_xcom_pull = (
                        "{{ ti.xcom_pull('%s.collect_nodes', key='nodes') }}"
                        % (hostos_operators.stage_name(stage, batch_index))
                    )
                    tolerance_xcom_pull = (
                        "{{ ti.xcom_pull('%s.plan', key='tolerance') }}"
                        % (hostos_operators.stage_name(stage, batch_index))
                    )
                    start_time_xcom_pull = """{{
                        ti.xcom_pull(task_ids='%s.plan', key="start_at")
                        | string
                    }}""" % (hostos_operators.stage_name(stage, batch_index))
                    proposal_xcom_pull = """{{
                        ti.xcom_pull(task_ids='%s.create_proposal_if_none_exists')
                    }}""" % (hostos_operators.stage_name(stage, batch_index))

                    plan = python_operator.BranchPythonOperator(
                        task_id="plan",
                        python_callable=hostos_operators.plan,
                        op_args=[stage, batch_index],
                    )
                    wait = ic_os_sensor.CustomDateTimeSensorAsync(
                        task_id="wait_until_start_time",
                        target_time=start_time_xcom_pull,
                        simulate="{{ params.simulate }}",
                    )
                    nodes = python_operator.BranchPythonOperator(
                        task_id="collect_nodes",
                        python_callable=hostos_operators.collect_nodes,
                        op_args=[stage, batch_index, network],
                        retries=retries,
                    )
                    propose = python_operator.PythonOperator(
                        task_id="create_proposal_if_none_exists",
                        python_callable=hostos_operators.create_proposal_if_none_exists,
                        op_args=[nodes_xcom_pull, network],
                        retries=retries,
                        do_xcom_push=True,
                    )
                    announce = RequestProposalVote(
                        task_id="request_proposal_vote",
                        source_task_id="%s.create_proposal_if_none_exists"
                        % hostos_operators.stage_name(stage, batch_index),
                        retries=retries,
                    )
                    accept = python_sensor.PythonSensor(
                        task_id="wait_until_proposal_is_accepted",
                        python_callable=ic_os_sensor.has_proposal_executed,
                        poke_interval=120,
                        timeout=86400 * 7,
                        mode="reschedule",
                        op_args=[proposal_xcom_pull, network, "{{ params.simulate }}"],
                        retries=retries,
                    )
                    adopt = python_sensor.PythonSensor(
                        task_id="wait_for_revision_adoption",
                        python_callable=hostos_sensors.have_hostos_nodes_adopted_revision,
                        poke_interval=120,
                        timeout=86400 * 7,
                        mode="reschedule",
                        op_args=[nodes_xcom_pull, tolerance_xcom_pull, network],
                        retries=retries,
                    )
                    healthy = python_sensor.PythonSensor(
                        task_id="wait_until_nodes_healthy",
                        python_callable=hostos_sensors.are_hostos_nodes_healthy,
                        poke_interval=120,
                        timeout=86400 * 7,
                        mode="reschedule",
                        op_args=[nodes_xcom_pull, tolerance_xcom_pull, network],
                        retries=retries,
                    )
                    join = EmptyOperator(
                        task_id="join",
                        trigger_rule="none_failed_min_one_success",
                    )
                    # And now the dependency of the batches.
                    plan >> wait >> nodes >> propose >> announce
                    propose >> accept >> adopt >> healthy
                    [plan, nodes, announce, healthy] >> join

                batches.append(batch(batch_name, batch_index))

        wait_for_other_rollouts = ic_os_sensor.WaitForOtherDAGs(
            task_id="wait_for_other_rollouts"
        )

        wait_for_election = python_sensor.PythonSensor(
            task_id="wait_for_revision_to_be_elected",
            python_callable=hostos_sensors.has_network_adopted_hostos_revision,
            poke_interval=300,
            timeout=86400 * 7,
            mode="reschedule",
            op_args=[network],
            retries=retries,
        )

        chain([timetable, wait_for_election, wait_for_other_rollouts], *batches)

    rollout_ic_os_to_nodes()
