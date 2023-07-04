"""
Rollout IC os to subnets.
"""

import datetime

import operators.test_operator as test_operator
import pendulum

from airflow import DAG

with DAG(
    dag_id="test_dag",
    schedule=None,
    start_date=pendulum.datetime(2020, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(days=14),
    tags=["testing"],
) as dag:
    test_task = test_operator.TestTask(
        task_id="create_proposal_if_none_exists",
    )
