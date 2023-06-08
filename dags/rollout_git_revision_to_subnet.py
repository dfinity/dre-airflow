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

with DAG(
    dag_id="rollout_git_revision_to_subnet",
    schedule=None,
    start_date=pendulum.datetime(2020, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(hours=12),
    tags=["rollout", "DRE"],
    params={
        "subnet_id": Param("", type="string"),
        "git_revision": Param("", type="string"),
        "start_time": Param(
            timezone.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f%z"),
            type="string",
            format="date-time",
            title="Start rollout at",
            description="Please select a date and time to roll out this subnet",
        ),
    },
) as dag:
    wait_until_start_time = DateTimeSensorAsync(
        task_id="wait_until_start_time",
        target_time="{{ params.start_time }}",
    )

    create_proposal_if_none_exists = CreateProposalIdempotently(
        task_id="create_proposal_if_none_exists",
        subnet_id="{{ params.subnet_id }}",
        git_revision="{{ params.git_revision }}",
    )

    wait_until_proposal_is_accepted = WaitForProposalAcceptance(
        task_id="wait_until_proposal_is_accepted",
        subnet_id="{{ params.subnet_id }}",
        git_revision="{{ params.git_revision }}",
    )

    wait_for_replica_revision = WaitForReplicaRevisionUpdated(
        task_id="wait_for_replica_revision",
        subnet_id="{{ params.subnet_id }}",
        git_revision="{{ params.git_revision }}",
    )

    wait_until_no_alerts_on_subnets = WaitUntilNoAlertsOnSubnet(
        task_id="wait_until_no_alerts_on_subnets",
        subnet_id="{{ params.subnet_id }}",
        git_revision="{{ params.git_revision }}",
    )

    wait_until_start_time >> create_proposal_if_none_exists
    create_proposal_if_none_exists >> wait_until_proposal_is_accepted
    wait_until_proposal_is_accepted >> wait_for_replica_revision
    wait_for_replica_revision >> wait_until_no_alerts_on_subnets


if __name__ == "__main__":
    dag.test()
