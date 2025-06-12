import datetime
import os
import unittest

import dfinity.ic_os_rollout as ic_os_rollout
import dfinity.ic_types as ic_types
import operators.ic_os_rollout as ic_os_rollout_operators
import pendulum

from airflow import DAG
from airflow.models import DagBag, DagRun
from airflow.models.param import Param
from airflow.settings import Session

DATA_INTERVAL_START = pendulum.datetime(2021, 9, 13, tz="UTC")
DATA_INTERVAL_END = DATA_INTERVAL_START + datetime.timedelta(days=1)


def db():
    airflow_home = os.path.dirname(__file__)
    db_path = os.path.join(airflow_home, "airflow.db")
    assert os.path.isfile(
        db_path
    ), "tests/ airflow.db does not exist, run `make test` to generate it"


class TestSchedule(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        db()
        cls.dagbag = DagBag()

    def setUp(self):
        with DAG(
            dag_id="test_schedule",
            start_date=datetime.datetime(2023, 1, 1),
            schedule=None,
            catchup=False,
            params={
                "git_revision": Param(
                    "0000000000000000000000000000000000000000",
                    type="string",
                    pattern="^[a-f0-9]{40}$",
                    title="Main Git revision",
                    description="Git revision",
                ),
                "plan": Param(
                    default=ic_os_rollout.DEFAULT_SUBNET_ROLLOUT_PLANS[
                        "mainnet"
                    ].strip(),
                    type="string",
                    title="Rollout plan",
                    description="YAML-formatted string describing the rollout schedule",
                ),
            },
        ) as dag:
            ic_os_rollout_operators.schedule(
                network=ic_types.ICNetwork(
                    "https://ic0.app/",
                    "https://dashboard.internetcomputer.org/proposal",
                    "https://dashboard.internetcomputer.org/release",
                    [
                        "https://victoria.mainnet.dfinity.network/select/0/prometheus/api/v1/query"
                    ],
                    80,
                    "dfinity.ic_admin.mainnet.proposer_key_file",
                ),
            )
        self.dag = dag
        self.dagbag.bag_dag(self.dag, None)

    def test_standard_mainnet_schedule(self):
        """Tests that the EvenNumberCheckOperator returns True for 10."""
        sess = Session()
        dag = self.dagbag.get_dag(dag_id="test_schedule", session=sess)
        dag.test()

        assert dag.get_last_dagrun().state == "success", dag.get_last_dagrun().state
        run: DagRun = dag.get_last_dagrun()
        ret = run.get_task_instance("schedule").xcom_pull()
        self.assertIsInstance(ret, dict)
        self.assertIn("0", ret)
