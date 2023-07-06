"""
Rollout IC os to subnets.
"""

import datetime
import functools
from typing import cast

import operators.ic_os_rollout as ic_os_rollout
import pendulum
import sensors.ic_os_rollout as ic_os_sensor
import yaml
from dfinity.ic_admin import get_subnet_list
from dfinity.ic_api import IC_NETWORKS
from dfinity.ic_os_rollout import rollout_planner
from dfinity.ic_types import SubnetRolloutInstance

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models.param import Param

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
  11:00: [0]
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
            ic_admin_version = yaml.safe_load(
                context["task"].render_template("{{ params.git_revision }}", context)
            )
            kwargs = {"network": network}
            if ic_admin_version not in ["0000000000000000000000000000000000000000", 0]:
                kwargs["ic_admin_version"] = ic_admin_version
            subnet_list_source = functools.partial(get_subnet_list, **kwargs)
            plan = rollout_planner(
                plan_data_structure,
                subnet_list_source=subnet_list_source,
            )
            plan = list(sorted(plan, key=lambda x: x.start_at))
            for n, item in enumerate(plan):
                print(
                    f"{n}: Subnet {item.subnet_id} ({item.subnet_num}) will start"
                    f" to be rolled out at {item.start_at}"
                )
            return plan

        @task_group()
        def per_subnet(subnet):  # type:ignore
            @task
            def report(x: SubnetRolloutInstance) -> datetime.datetime:
                print(
                    f"Plan for subnet {x.subnet_num} with ID {x.subnet_id}"
                    f" starts at {x.start_at}"
                )
                return x  # type:ignore

            (
                report(subnet)
                >> ic_os_sensor.CustomDateTimeSensorAsync(
                    task_id="wait_until_start_time",
                    target_time="""{{
                        task_instance.xcom_pull(
                            task_ids='per_subnet.report',
                            map_indexes=task_instance.map_index,
                        ).start_at
                    }}""",
                )
                >> ic_os_sensor.WaitUntilNoAlertsOnAnySubnet(
                    task_id="wait_until_no_alerts_on_any_subnet",
                    alert_task_id="per_subnet.wait_until_no_alerts",
                )
                >> ic_os_rollout.CreateProposalIdempotently(
                    task_id="create_proposal_if_none_exists",
                    subnet_id="""{{
                    task_instance.xcom_pull(
                        task_ids='per_subnet.report',
                        map_indexes=task_instance.map_index,
                    ).subnet_id
                }}""",
                    git_revision="{{ params.git_revision }}",
                    simulate_proposal=cast(bool, "{{ params.simulate }}"),
                    retries=retries,
                    network=network,
                )
                >> (
                    ic_os_rollout.RequestProposalVote(
                        task_id="request_proposal_vote",
                        source_task_id="per_subnet.create_proposal_if_none_exists",
                    ),
                    ic_os_sensor.WaitForProposalAcceptance(
                        task_id="wait_until_proposal_is_accepted",
                        subnet_id="""{{
                                task_instance.xcom_pull(
                                    task_ids='per_subnet.report',
                                    map_indexes=task_instance.map_index,
                                ).subnet_id
                            }}""",
                        git_revision="{{ params.git_revision }}",
                        simulate_proposal_acceptance=cast(
                            bool,
                            """{{ params.simulate }}""",
                        ),
                        retries=retries,
                        network=network,
                    ),
                )
                >> ic_os_sensor.WaitForReplicaRevisionUpdated(
                    task_id="wait_for_replica_revision",
                    subnet_id="""{{
                                task_instance.xcom_pull(
                                    task_ids='per_subnet.report',
                                    map_indexes=task_instance.map_index,
                                ).subnet_id
                            }}""",
                    git_revision="{{ params.git_revision }}",
                    retries=retries,
                    network=network,
                )
                >> ic_os_sensor.WaitUntilNoAlertsOnSubnet(
                    task_id="wait_until_no_alerts",
                    subnet_id="""{{
                                task_instance.xcom_pull(
                                    task_ids='per_subnet.report',
                                    map_indexes=task_instance.map_index,
                                ).subnet_id
                            }}""",
                    git_revision="{{ params.git_revision }}",
                    retries=retries,
                    network=network,
                ),
            )

        sched = schedule()
        p = per_subnet.expand(subnet=sched)
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
