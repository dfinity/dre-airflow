"""
Rollout IC os to subnets.
"""

import datetime

import operators.test_operator as test_operator
import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.time_delta import TimeDeltaSensorAsync

with DAG(
    dag_id="test_dag",
    schedule=None,
    start_date=pendulum.datetime(2020, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(days=14),
    tags=["testing"],
) as dag:

    @task
    def finish() -> None:
        print("Finished!")

    (
        test_operator.TestTask(
            task_id="task_1",
        )
        >> test_operator.TestTask(
            task_id="task_2",
        )
        >> TimeDeltaSensorAsync(
            task_id="wait_2_minutes",
            delta=datetime.timedelta(minutes=2),
        )  # type: ignore
        >> test_operator.TestTask(
            task_id="task_3",
        )
        >> test_operator.TestTask(
            task_id="task_4",
        )
        >> finish()
    )
