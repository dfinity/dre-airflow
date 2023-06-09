"""
Example DAG demonstrating the usage of the BashOperator and custom
TimedPythonOperator.
"""

import datetime

import pendulum

from airflow import DAG
from airflow.models.param import Param
from airflow.sensors.date_time import DateTimeSensorAsync
from airflow.utils import timezone
from operators.ic_os_rollout import CreateProposalIdempotently
from sensors.ic_os_rollout import (
    WaitForProposalAcceptance,
    WaitForReplicaRevisionUpdated,
    WaitUntilNoAlertsOnSubnet,
)

now = timezone.utcnow().strftime("%Y-%m-%dT%H:%M:%S%z")
if not now.endswith("Z"):
    now = now[:-2] + ":" + now[-2:]


with DAG(
    dag_id="rollout_git_revision_to_subnet",
    schedule=None,
    start_date=pendulum.datetime(2020, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(hours=12),
    tags=["rollout", "DRE"],
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
        ),
        "start_time": Param(
            now,
            type="string",
            format="date-time",
            title="Start rollout at",
            description="Please select a date and time to roll out this subnet",
        ),
    },
    render_template_as_native_obj=True,
) as dag:
    (
        DateTimeSensorAsync(
            task_id="wait_until_start_time",
            target_time="{{ params.start_time }}",
        )
        >> CreateProposalIdempotently(
            task_id="create_proposal_if_none_exists",
            subnet_id="{{ params.subnet_id }}",
            git_revision="{{ params.git_revision }}",
        )
        >> WaitForProposalAcceptance(
            task_id="wait_until_proposal_is_accepted",
            subnet_id="{{ params.subnet_id }}",
            git_revision="{{ params.git_revision }}",
        )
        >> WaitForReplicaRevisionUpdated(
            task_id="wait_for_replica_revision",
            subnet_id="{{ params.subnet_id }}",
            git_revision="{{ params.git_revision }}",
        )
        >> WaitUntilNoAlertsOnSubnet(
            task_id="wait_until_no_alerts_on_subnets",
            subnet_id="{{ params.subnet_id }}",
            git_revision="{{ params.git_revision }}",
        )
    )


if __name__ == "__main__":
    dag.test()
