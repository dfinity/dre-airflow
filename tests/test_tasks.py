import dfinity.ic_types as ic_types
from pendulum import datetime
from sensors.ic_os_rollout import WaitUntilNoAlertsOnSubnet

from airflow.models.dag import DAG

if 0:
    # Testing code that should not run during normal testing.
    # Will soon change to exercise the entire rollout DAG in simulate mode.
    # We will need to https://www.astronomer.io/docs/learn/testing-airflow#use-variables-and-connections-in-dagtest
    # and wrap this in a Python unit test.
    with DAG(
        dag_id="simple_classic_dag",
        start_date=datetime(2023, 1, 1),
        schedule="@daily",
        catchup=False,
    ) as dag:  # assigning the context to an object is mandatory for using dag.test()

        t1 = WaitUntilNoAlertsOnSubnet(
            task_id="x",
            subnet_id="snjp4-xlbw4-mnbog-ddwy6-6ckfd-2w5a2-eipqo-7l436-pxqkh-l6fuv-vae",
            git_revision="0" * 32,
            network=ic_types.IC_NETWORKS["mainnet"],
        )

    if __name__ == "__main__":
        dag.test()
