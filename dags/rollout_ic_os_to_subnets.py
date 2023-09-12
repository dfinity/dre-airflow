"""
Rollout IC os to subnets in batches.

Each batch runs in parallel.
"""

import datetime
import functools
import itertools
from typing import Any, cast

import operators.ic_os_rollout as ic_os_rollout
import pendulum
import sensors.ic_os_rollout as ic_os_sensor
import yaml
from dfinity.ic_admin import get_subnet_list
from dfinity.ic_api import IC_NETWORKS
from dfinity.ic_os_rollout import rollout_planner
from dfinity.ic_types import SubnetRolloutInstance

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.task_group import TaskGroup

BATCH_COUNT: int = 30

DEFAULT_PLANS: dict[str, str] = {
    "mainnet": """
# See documentation at the end of this configuration block.
Monday:
  9:00: [6]
  11:00: [8, 33]
Tuesday:
  7:00: [15, 18]
  9:00: [1, 5, 2]
  11:00: [4, 9, 34]
Wednesday:
  7:00: [3, 7, 11]
  9:00: [10, 13, 16]
  11:00: [20, 27, 24]
  13:00: [21, 12, 28]
Thursday:
  7:00: [26, 22, 23]
  9:00: [25, 29, 19]
  11:00: [17, 32, 35]
  13:00: [30, 31, 14]
Monday next week:
  7:00: [0]
# Remarks:
# * All times are expressed in the UTC time zone.
# * Days refer to dates relative to your current work week
#   if starting a rollout during a workday, or next week if
#   the rollout is started during a weekend.
# * A day name with " next week" added at the end means
#   "add one week to this day".
# * Subnets may be specified by integer number from 0
#   to the maximum subnet number, or may be specified
#   as full or abbreviated subnet principal ID.
"""
}

_PLAN_FORM = """
    <textarea class="form-control" name="{name}" 
           id="{name}" placeholder=""
           type="text"
           required="" rows="24">{value}</textarea>
"""


DAGS: dict[str, DAG] = {}
for network_name, network in IC_NETWORKS.items():
    with DAG(
        dag_id=f"rollout_ic_os_to_{network_name}_subnets",
        schedule=None,
        start_date=pendulum.datetime(2020, 1, 1, tz="UTC"),
        catchup=False,
        dagrun_timeout=datetime.timedelta(days=14),
        tags=["rollout", "DRE", "IC OS"],
        render_template_as_native_obj=True,
        params={
            "git_revision": Param(
                "0000000000000000000000000000000000000000",
                type="string",
                pattern="^[a-f0-9]{40}$",
                title="Git revision",
                description="Git revision of the IC-OS release to roll out to subnets",
            ),
            "plan": Param(
                default=DEFAULT_PLANS[network_name].strip(),
                type="string",
                title="Rollout plan",
                description="A YAML-formatted string describing the rollout schedule",
                custom_html_form=_PLAN_FORM,
            ),
            "simulate": Param(
                True,
                type="boolean",
                title="Simulate",
                description="If enabled (the default), the update proposal will be"
                " simulated but not created, and its acceptance will be simulated too",
            ),
        },
    ) as dag:
        DAGS[network_name] = dag
        retries = int(86400 / 60 / 5)  # one day worth of retries

        @task
        def schedule(**context):  # type: ignore
            plan_data_structure = yaml.safe_load(
                context["task"].render_template("{{ params.plan }}", context)
            )
            ic_admin_version = "{:040}".format(
                context["task"].render_template("{{ params.git_revision }}", context)
            )
            kwargs: dict[str, Any] = {"network": network}
            if ic_admin_version not in ["0000000000000000000000000000000000000000", 0]:
                kwargs["ic_admin_version"] = ic_admin_version
            subnet_list_source = functools.partial(get_subnet_list, **kwargs)
            plan = rollout_planner(
                plan_data_structure,
                subnet_list_source=subnet_list_source,
            )
            batches: dict[
                str, tuple[datetime.datetime, list[SubnetRolloutInstance]]
            ] = {}
            for n, (group_key, members) in enumerate(
                itertools.groupby(plan, key=lambda x: x.start_at)
            ):
                print(f"Batch {n+1}:")
                batches[str(n)] = (group_key, [])
                for item in members:
                    batches[str(n)][1].append(item)
                    print(
                        f"    Subnet {item.subnet_id} ({item.subnet_num}) will start"
                        f" to be rolled out at {item.start_at}."
                    )
            return batches

        sched = schedule()

        def make_me_a_batch(batch_name: str, batch: int) -> None:
            def work_to_do(
                current_batch_index: int,
                batch_name: str,
                batches: dict[
                    str, tuple[datetime.datetime, list[SubnetRolloutInstance]]
                ],
            ) -> list[str]:
                this_batch = batches.get(str(current_batch_index))
                if this_batch:
                    print("This batch will roll out the following subnets:")
                    for item in this_batch[1]:
                        print(
                            f"    Subnet {item.subnet_id} ({item.subnet_num}) will"
                            f" start to be rolled out at {item.start_at}."
                        )
                    return [f"batch_{batch_name}.wait_until_start_time"]
                print("This batch does nothing.")
                return [f"batch_{batch_name}.empty_batch"]

            @task
            def collect_batch_subnets(
                current_batch_index: int, **kwargs: Any
            ) -> list[str]:
                subnets = cast(
                    list[SubnetRolloutInstance],
                    (
                        kwargs["ti"]
                        .xcom_pull("schedule")
                        .get(str(current_batch_index))[1]
                    ),
                )
                return [s.subnet_id for s in subnets]

            proceed = collect_batch_subnets(batch)

            batch_has_work = BranchPythonOperator(
                task_id="batch_has_work",
                python_callable=work_to_do,
                op_args=[
                    batch,
                    batch_name,
                    """{{
                            task_instance.xcom_pull(task_ids='schedule')
                        }}""",
                ],
            )

            empty_batch = EmptyOperator(task_id="empty_batch")

            wait_until_start_time = ic_os_sensor.CustomDateTimeSensorAsync(
                task_id="wait_until_start_time",
                target_time="""{{
                            ti.xcom_pull(task_ids='schedule')["%d"][0] | string
                        }}"""
                % batch,
            )

            batch_has_work >> wait_until_start_time >> proceed

            join = EmptyOperator(
                task_id="join",
                trigger_rule="none_failed_min_one_success",
            )

            (
                proceed
                >> ic_os_rollout.CreateProposalIdempotently.partial(
                    task_id="create_proposal_if_none_exists",
                    git_revision="{{ params.git_revision }}",
                    simulate_proposal=cast(bool, "{{ params.simulate }}"),
                    retries=retries,
                    network=network,
                ).expand(subnet_id=proceed)
                >> (
                    ic_os_rollout.RequestProposalVote.partial(
                        task_id="request_proposal_vote",
                        source_task_id=f"batch_{batch_name}.create_proposal_if_none_exists",
                        retries=retries,
                    ).expand(_ignored=proceed),
                    (
                        ic_os_sensor.WaitForProposalAcceptance.partial(
                            task_id="wait_until_proposal_is_accepted",
                            git_revision="{{ params.git_revision }}",
                            simulate_proposal_acceptance=cast(
                                bool, """{{ params.simulate }}"""
                            ),
                            retries=retries,
                            network=network,
                        ).expand(subnet_id=proceed)
                    ),
                )
                >> ic_os_sensor.WaitForReplicaRevisionUpdated.partial(
                    task_id="wait_for_replica_revision",
                    git_revision="{{ params.git_revision }}",
                    retries=retries,
                    network=network,
                    expected_replica_count="""{{
                        ti.xcom_pull(
                            task_ids='batch_%(batch_name)s."""
                    % locals()
                    + """create_proposal_if_none_exists',
                            key='replica_count'
                        ) | int
                    }}""",
                ).expand(subnet_id=proceed)
                >> ic_os_sensor.WaitUntilNoAlertsOnSubnet.partial(
                    task_id="wait_until_no_alerts",
                    git_revision="{{ params.git_revision }}",
                    retries=retries,
                    network=network,
                ).expand(subnet_id=proceed)
                >> join
            )

            batch_has_work >> empty_batch >> join

        last_task_group = None
        for batch in range(BATCH_COUNT):
            batch_name = str(batch + 1)
            with TaskGroup(group_id=f"batch_{batch_name}") as group:
                make_me_a_batch(batch_name, batch)
                if last_task_group is not None:
                    last_task_group >> group
                else:
                    sched >> group
                last_task_group = group

        wait_for_election = ic_os_sensor.WaitForRevisionToBeElected(
            task_id="wait_for_revision_to_be_elected",
            git_revision="{{ params.git_revision }}",
            simulate_elected=cast(bool, "{{ params.simulate }}"),
            network=network,
            retries=retries,
        )

        wait_for_election >> sched


if __name__ == "__main__":
    import os
    import sys

    try:
        rev = sys.argv[1]
    except Exception:
        print(
            "Error: to run this DAG you must specify the revision"
            " as the first argument on the command line, optionally"
            " with the rollout plan in YAML form as second argument.",
            file=sys.stderr,
        )
        sys.exit(os.EX_USAGE)
    try:
        plan = sys.argv[2]
    except Exception:
        plan = DEFAULT_PLANS["mainnet"]
    dag = DAGS["mainnet"]
    dag.test(
        run_conf={
            "git_revision": rev,
            "plan": plan,
        }
    )
