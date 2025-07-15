# mypy: disable-error-code=unused-ignore
"""
Rollout IC os to subnets in batches.

Each batch runs in parallel.
"""

import datetime
import os
import sys
import typing

import pendulum
import sensors.ic_os_rollout as ic_os_sensor
from dfinity.ic_types import IC_NETWORKS
from dfinity.rollout_types import HostOSStage
from operators import hostos_rollout as hostos_operators
from operators.ic_os_rollout import RequestProposalVote
from sensors import hostos_rollout as hostos_sensors

import airflow.operators.python as python_operator
import airflow.sensors.python as python_sensor
from airflow import __version__
from airflow.decorators import dag, task, task_group
from airflow.models.baseoperator import chain
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator

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

1. `canary` stages (up to `CANARY_BATCH_COUNT` batches which is 5)
2. `main` stages (up to `MAIN_BATCH_COUNT` batches which is 50)
3. `unassigned` stages (up to `UNASSIGNED_BATCH_COUNT` batches which is 15)
4. `stragglers` stage (only one batch)

Which nodes to roll out to is decided by a list of selectors that select
which nodes are to be rolled out in each batch:

* one list of selectors per `canary` batch,
* a single list of selectors common to all `main` batches,
* a single list of selectors common to all `unassigned` batches,
* a single list of selectors for the single `stragglers` batch

A selector is a dictionary that specifies `assignment` (unassigned or
assigned), `owner` (DFINITY or others), status (Degraded, Healthy or
Down).  From those selection keys, a list of nodes is created by the batch,
and then optionally grouped by a property (either datacenter or subnet,
specified in key `group_by`).  After the (optional) grouping, a number of
nodes from each group is selected (up to an absolute number if the key
`nodes_per_group` is an integer, or a 0-100 percentage if the key is a
number postfixed by %).  Then the groups (or single group if no `group_by`
was specified) are collated into a single list, and those are the nodes that
the batch will target.  Here is an example of a selector that would select
1 unassigned node per datacenter that is owned by DFINITY:

```
- assignment: unassigned
    owner: DFINITY
    group_by: datacenter
    nodes_per_group: 1
```

The application of selectors for each batch happens as follows:

* The batch starts with all nodes not yet rolled out to as candidates.
* Each selector in the batch's list of selectors is applied iteratively,
    reducing the list of nodes that the batch will roll out to only the nodes
    matching the selector as well as all prior selectors.

A list is used rather than a single selector, because this allows for combining
multiple selectors to achieve a selection of nodes that would otherwise be
impossible to obtain with a single selector.  Note that an empty list of
selectors is equivalent to "all nodes".  The rollout has a fuse built-in that
prevents rolling out to more than 150 nodes at once, so if this error takes
place, the rollout will abort.  If an empty list of nodes is the result of
all selectors applied, the batch is simply skipped and the rollout moves to
the next batch (or the first batch of the next stage, if need be).

If a stage key is not specified, the stage is skipped altogether.

Putting it all together, here is an abridged example of a rollout that would
only roll out three `canary` batches (to a single node each), and a series
of unassigned batches, with no `main` or `stragglers` stage:

```
stages:
  canary:
    - selectors: # for batch 1
      - assignment: unassigned
        nodes_per_group: 1
    - selectors: # for batch 2
      - assignment: unassigned
        nodes_per_group: 1
    - selectors: # for batch 3
      - assignment: unassigned
        nodes_per_group: 1
  unassigned:
    selectors: # for all batches up to the 15th
    - assignment: unassigned
      nodes_per_group: 100
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
                title="Main Git revision",
                description="Git revision of the IC OS GuestOS release to roll out to"
                " subnets, unless specified otherwise directly for a specific subnet;"
                " the version must have been elected before but the rollout will check",
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
                description="If enabled (the default), the update proposal will be"
                " simulated but not created, and its acceptance will be simulated too",
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
                        op_args=[nodes_xcom_pull, network],
                        retries=retries,
                    )
                    healthy = python_sensor.PythonSensor(
                        task_id="wait_until_nodes_healthy",
                        python_callable=hostos_sensors.are_hostos_nodes_healthy,
                        poke_interval=120,
                        timeout=86400 * 7,
                        mode="reschedule",
                        op_args=[nodes_xcom_pull, network],
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
            task_id="wait_until_nodes_healthy",
            python_callable=hostos_sensors.has_network_adopted_hostos_revision,
            poke_interval=300,
            timeout=86400 * 7,
            mode="reschedule",
            op_args=[network],
            retries=retries,
        )

        chain([timetable, wait_for_election, wait_for_other_rollouts], *batches)

    rollout_ic_os_to_nodes()
