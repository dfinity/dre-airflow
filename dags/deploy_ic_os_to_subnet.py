"""
Example DAG demonstrating the usage of the BashOperator and custom
TimedPythonOperator.
"""

import datetime
from typing import cast

import operators.ic_os_rollout as ic_os_rollout
import pendulum
import sensors.ic_os_rollout as ic_os_sensor
from dfinity.ic_api import IC_NETWORKS

from airflow import DAG
from airflow.models.param import Param
from airflow.utils import timezone

now = timezone.utcnow().strftime("%Y-%m-%dT%H:%M:%S%z")
if not now.endswith("Z"):
    now = now[:-2] + ":" + now[-2:]

DAGS: dict[str, DAG] = {}
for network_name, network in IC_NETWORKS.items():
    with DAG(
        dag_id=f"deploy_ic_os_to_{network_name}_subnet",
        schedule=None,
        start_date=pendulum.datetime(2020, 1, 1, tz="UTC"),
        catchup=False,
        dagrun_timeout=datetime.timedelta(hours=12),
        tags=["rollout", "DRE", "IC OS"],
        render_template_as_native_obj=True,
        params={
            "subnet_id": Param(
                "qn2sv-gibnj-5jrdq-3irkq-ozzdo-ri5dn-dynlb-xgk6d-kiq7w-cvop5-uae",
                type="string",
                pattern="^([a-z0-9]{5})-([a-z0-9]{5})-([a-z0-9]{5})-([a-z0-9]{5})"
                "-([a-z0-9]{5})-([a-z0-9]{5})-([a-z0-9]{5})-([a-z0-9]{5})"
                "-([a-z0-9]{5})-([a-z0-9]{5})-([a-z0-9]{3})$",
            ),
            "git_revision": Param(
                "0000000000000000000000000000000000000000",
                type="string",
                pattern="^[a-f0-9]{40}$",
                title="Git revision",
                description="Git revision of the IC-OS release to roll out to"
                " this subnet",
            ),
            "start_time": Param(
                now,
                type="string",
                format="date-time",
                title="Start rollout at",
                description="Please select a date and time to roll out this subnet",
            ),
            "simulate": Param(
                True,
                type="boolean",
                title="Simulate proposal",
                description="If enabled (the default), the update proposal will be"
                " simulated but not created, and its acceptance will be simulated too",
            ),
        },
    ) as dag:
        DAGS[network_name] = dag
        retries = int(86400 / 60 / 5)  # one day worth of retries

        (
            ic_os_sensor.WaitForRevisionToBeElected(
                task_id="wait_for_revision_to_be_elected",
                git_revision="{{ params.git_revision }}",
                simulate_elected=cast(bool, "{{ params.simulate }}"),
                network=network,
                retries=retries,
            )
            >> ic_os_sensor.CustomDateTimeSensorAsync(
                task_id="wait_until_start_time",
                target_time="{{ params.start_time }}",
            )
            >> ic_os_rollout.CreateProposalIdempotently(
                task_id="create_proposal_if_none_exists",
                subnet_id="{{ params.subnet_id }}",
                git_revision="{{ params.git_revision }}",
                simulate_proposal=cast(bool, "{{ params.simulate }}"),
                network=network,
                retries=retries,
            )
            >> (
                ic_os_rollout.RequestProposalVote(
                    task_id="request_proposal_vote",
                    source_task_id="create_proposal_if_none_exists",
                ),
                ic_os_sensor.WaitForProposalAcceptance(
                    task_id="wait_until_proposal_is_accepted",
                    subnet_id="{{ params.subnet_id }}",
                    git_revision="{{ params.git_revision }}",
                    simulate_proposal_acceptance=cast(bool, "{{ params.simulate }}"),
                    retries=retries,
                    network=network,
                ),
            )
            >> ic_os_sensor.WaitForReplicaRevisionUpdated(
                task_id="wait_for_replica_revision",
                subnet_id="{{ params.subnet_id }}",
                git_revision="{{ params.git_revision }}",
                retries=retries,
                network=network,
                expected_replica_count=cast(
                    int,
                    """{{
                    ti.xcom_pull(
                        task_ids='create_proposal_if_none_exists',
                        key='replica_count'
                    ) | int
                }}""",
                ),
            )
            >> ic_os_sensor.WaitUntilNoAlertsOnSubnet(
                task_id="wait_until_no_alerts",
                subnet_id="{{ params.subnet_id }}",
                git_revision="{{ params.git_revision }}",
                retries=retries,
                network=network,
            )
        )


if __name__ == "__main__":
    import os
    import sys

    try:
        subnet_id = sys.argv[1]
        rev = sys.argv[2]
    except Exception:
        print(
            "Error: to run this DAG you must specify the subnet ID and the revision"
            " as arguments on the command line.",
            file=sys.stderr,
        )
        sys.exit(os.EX_USAGE)
    dag = DAGS["mainnet"]
    dag.test(
        run_conf={
            "git_revision": rev,
            "subnet_id": subnet_id,
        }
    )
