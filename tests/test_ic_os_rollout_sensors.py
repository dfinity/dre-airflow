import unittest
from typing import Any

import mock
from dfinity.ic_types import IC_NETWORKS
from dfinity.prom_api import PrometheusVectorResultEntry
from sensors.ic_os_rollout import (
    WaitForProposalAcceptance,
    WaitForReplicaRevisionUpdated,
    WaitUntilNoAlertsOnSubnet,
)

import airflow.exceptions


class TestOperators(unittest.TestCase):
    def test_instantiation(self) -> None:
        sid = "qn2sv-gibnj-5jrdq-3irkq-ozzdo-ri5dn-dynlb-xgk6d-kiq7w-cvop5-uae"
        gitr = "a1f503d20b7846375c74ce5f7d0f8f6620ab7511"
        for klass in [
            WaitForProposalAcceptance,
            WaitForReplicaRevisionUpdated,
            WaitUntilNoAlertsOnSubnet,
        ]:
            kwargs: dict[str, Any] = {"network": IC_NETWORKS["mainnet"]}
            if klass == WaitForProposalAcceptance:
                kwargs["simulate_proposal_acceptance"] = True
            if klass == WaitForReplicaRevisionUpdated:
                kwargs["expected_replica_count"] = 13
            klass(
                task_id=klass.__name__,
                subnet_id=sid,
                git_revision=gitr,
                **kwargs,
            )


class TestWaitForReplicaRevisionUpdated(unittest.TestCase):
    network = IC_NETWORKS["mainnet"]

    def _exercise(self) -> WaitForReplicaRevisionUpdated:
        k = WaitForReplicaRevisionUpdated(
            task_id="x",
            subnet_id="yinp6-35cfo-wgcd2",
            git_revision="d5eb7683e144acb0f8850fedb29011f34bfbe4ac",
            network=self.network,
            expected_replica_count=13,
        )
        return k

    def test_happy_path(self) -> None:
        inp = [
            PrometheusVectorResultEntry(
                metric={
                    "ic_subnet": "yinp6-35cfo-wgcd2",
                    "ic_active_version": "d5eb7683e144acb0f8850fedb29011f34bfbe4ac",
                },
                value=13.0,
                timestamp=0.0,
            )
        ]
        with mock.patch("dfinity.prom_api.query_prometheus_servers") as m:
            m.return_value = inp
            # Should not raise TaskDeferred.
            self._exercise().execute({})

    def test_halfway_done(self) -> None:
        inp = [
            PrometheusVectorResultEntry(
                metric={
                    "ic_subnet": "yinp6-35cfo-wgcd2",
                    "ic_active_version": "oldversion",
                },
                value=4.0,
                timestamp=0.0,
            ),
            PrometheusVectorResultEntry(
                metric={
                    "ic_subnet": "yinp6-35cfo-wgcd2",
                    "ic_active_version": "d5eb7683e144acb0f8850fedb29011f34bfbe4ac",
                },
                value=7.0,
                timestamp=0.0,
            ),
        ]
        with mock.patch("dfinity.prom_api.query_prometheus_servers") as m:
            m.return_value = inp
            k = self._exercise()
            self.assertRaises(airflow.exceptions.TaskDeferred, k.execute, {})

    def test_not_started(self) -> None:
        inp = [
            PrometheusVectorResultEntry(
                metric={
                    "ic_subnet": "yinp6-35cfo-wgcd2",
                    "ic_active_version": "oldversion",
                },
                value=4.0,
                timestamp=0.0,
            )
        ]
        with mock.patch("dfinity.prom_api.query_prometheus_servers") as m:
            m.return_value = inp
            k = self._exercise()
            self.assertRaises(airflow.exceptions.TaskDeferred, k.execute, {})
