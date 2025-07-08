"""
Rollout IC os to subnets.
"""

import datetime

import operators.test_operator as test_operator
import pendulum

import airflow.operators.python as python_operator
from airflow import DAG
from airflow.decorators import task
from airflow.sensors.time_delta import TimeDeltaSensorAsync

with DAG(
    dag_id="test_dag",
    schedule=None,  # "@daily",
    start_date=pendulum.datetime(2025, 7, 5, tz="UTC"),
    catchup=True,
    dagrun_timeout=datetime.timedelta(days=14),
    tags=["testing"],
) as dag:

    class TemplatedTaskArgs:
        template_fields = ["string"]

        def __init__(self, string: str) -> None:
            self.string = string

    def print_data(string: TemplatedTaskArgs) -> None:
        print(string.string)

    @task
    def finish() -> None:
        print("Finished!")

    (
        python_operator.PythonOperator(
            task_id="start",
            python_callable=print_data,
            op_args=[
                TemplatedTaskArgs(
                    "ds {{ ds }} distart {{ data_interval_start }}"
                    " diend {{ data_interval_end }} logical_date {{ logical_date }}"
                )
            ],
        )
        >> test_operator.TestTask(
            task_id="task_1",
        )
        >> test_operator.TestTask(
            task_id="task_2",
        )
        >> TimeDeltaSensorAsync(  # type: ignore
            task_id="wait_2_minutes",
            delta=datetime.timedelta(minutes=2),
        )
        >> test_operator.TestTask(
            task_id="task_3",
        )
        >> test_operator.TestTask(
            task_id="task_4",
        )
        >> finish()
    )
