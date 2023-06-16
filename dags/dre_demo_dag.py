"""
Example DAG demonstrating the usage of the BashOperator and custom
TimedPythonOperator.
"""

import datetime

import pendulum
from operators.timed_python_operator import TimedPythonOperator
from sensors.custom_poke import CustomPokeSensor

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="dre_demo_dag",
    schedule=None,
    start_date=pendulum.datetime(2020, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["testing"],
    params={"example_key": "example_value"},
) as dag:
    run_this_last = EmptyOperator(
        task_id="run_this_last",
    )

    run_this = BashOperator(
        task_id="run_after_loop",
        bash_command="echo 1",
    )

    run_this >> run_this_last

    for i in range(3):
        task = BashOperator(
            task_id="runme_" + str(i),
            bash_command='echo "{{ task_instance_key_str }}" && sleep 1',
        )
        task >> run_this

    def print_hello() -> None:
        print("Hello!")

    def sensor_done() -> bool:
        return True

    also_run_this = TimedPythonOperator(
        task_id="run_this_python",
        python_callable=print_hello,
    )
    also_run_this >> run_this_last

    wait_before = CustomPokeSensor(
        task_id="wait_till_true",
        python_callable=sensor_done,
    )

    wait_before >> also_run_this


# [START howto_operator_bash_skip]
this_will_skip = BashOperator(
    task_id="this_will_skip",
    bash_command='echo "hello world"; exit 99;',
    dag=dag,
)
# [END howto_operator_bash_skip]
this_will_skip >> run_this_last

if __name__ == "__main__":
    dag.test()
