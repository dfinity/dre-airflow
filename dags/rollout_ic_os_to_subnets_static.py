"""
Rollout IC os to subnets.
"""

import functools
from typing import cast, Dict, List

import operators.ic_os_rollout as ic_os_rollout
import pendulum
import sensors.ic_os_rollout as ic_os_sensor
import yaml
from dfinity.ic_admin import get_subnet_list
from dfinity.ic_api import IC_NETWORKS
from dfinity.ic_os_rollout import rollout_planner_static
from dfinity.ic_types import SubnetRolloutInstance
from airflow.sensors.date_time import DateTimeSensorAsync
from airflow.sensors.time_sensor import TimeSensor
from airflow.utils.task_group import TaskGroup

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models.param import Param
import datetime
from datetime import datetime as dt

rollout_schedule = {
    'Monday': {
        '9:00': [6],
        '11:00': [8, 33]
    },
    'Tuesday': {
        '7:00': [15, 18],
        '9:00': [1, 5, 2],
        '11:00': [4, 9, 34]
    },
    'Wednesday': {
        '7:00': [3, 7, 11],
        '9:00': [10, 13, 16],
        '11:00': [20, 27, 24],
        '13:00': [21, 12, 28]
    },
    'Thursday': {
        '7:00': [26, 22, 23],
        '9:00': [25, 29, 19],
        '11:00': [17, 32, 35],
        '13:00': [30, 31, 14]
    }
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
        dag_id=f"rollout_ic_os_to_{network_name}_subnets_static",
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
        task_plan = list()

        kwargs = {"network": network}
        subnet_list_source = functools.partial(get_subnet_list, **kwargs)
        plan: Dict[str, List[SubnetRolloutInstance]] = rollout_planner_static(
            rollout_schedule,
            subnet_list_source=subnet_list_source,
        )

        wait_for_election = ic_os_sensor.WaitForRevisionToBeElected(
            task_id="wait_for_revision_to_be_elected",
            git_revision="{{ params.git_revision }}",
            simulate_elected=cast(bool, "{{ params.simulate }}"),
            network=network,
            retries=retries,
        )

        task_plan.append(wait_for_election)

        for dt_str, subnet_list in plan.items():
            day_sensor = DateTimeSensorAsync(
                            task_id=f'wait_for_{dt_str}',
                            target_time=subnet_list[0].start_at,
                            timeout=24*60*60,
                            poke_interval = 10,
                            mode="reschedule",
                            dag=dag
                        )
            
            task_plan.append(day_sensor)

            for subnet in subnet_list:
                with TaskGroup(f"deploy_{subnet.subnet_id}", tooltip="Tasks for section_1") as deploy:
                            (ic_os_sensor.WaitUntilNoAlertsOnAnySubnet(
                                task_id=f"wait_until_no_alerts_on_any_subnet_{subnet.subnet_id}",
                                subnet_id=subnet.subnet_id,
                                git_revision="{{ params.git_revision }}",
                                alert_task_id="per_subnet.wait_until_no_alerts",
                                retries=retries,
                                network=network,
                            )
                            >> ic_os_rollout.CreateProposalIdempotently(
                                task_id=f"create_proposal_if_none_exists_{subnet.subnet_id}",
                                subnet_id=subnet.subnet_id,
                                git_revision="{{ params.git_revision }}",
                                simulate_proposal=cast(bool, "{{ params.simulate }}"),
                                retries=retries,
                                network=network,
                            )
                            >> ic_os_sensor.WaitForProposalAcceptance(
                                    task_id=f"wait_until_proposal_is_accepted_{subnet.subnet_id}",
                                    subnet_id=subnet.subnet_id,
                                    git_revision="{{ params.git_revision }}",
                                    simulate_proposal_acceptance=cast(
                                        bool,
                                        """{{ params.simulate }}""",
                                    ),
                                    retries=retries,
                                    network=network,
                                )
                            >> ic_os_sensor.WaitForReplicaRevisionUpdated(
                                task_id=f"wait_for_replica_revision_{subnet.subnet_id}",
                                subnet_id=subnet.subnet_id,
                                git_revision="{{ params.git_revision }}",
                                retries=retries,
                                network=network,
                            )
                            >> ic_os_sensor.WaitUntilNoAlertsOnSubnet(
                                task_id=f"wait_until_no_alerts_{subnet.subnet_id}",
                                subnet_id=subnet.subnet_id,
                                git_revision="{{ params.git_revision }}",
                                retries=retries,
                                network=network,
                            ),
                        )
                task_plan.append(deploy)

    for task_id in range(len(task_plan)-1):
        task_plan[task_id] >> task_plan[task_id+1]