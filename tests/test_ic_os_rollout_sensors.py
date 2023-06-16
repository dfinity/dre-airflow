import unittest

import mock  # type:ignore
from dfinity.prom_api import PrometheusVectorResultEntry
from sensors.ic_os_rollout import (
    WaitForProposalAcceptance,
    WaitForReplicaRevisionUpdated,
    WaitUntilNoAlertsOnSubnet,
)


class TestOperators(unittest.TestCase):
    def test_instantiation(self) -> None:
        sid = "qn2sv-gibnj-5jrdq-3irkq-ozzdo-ri5dn-dynlb-xgk6d-kiq7w-cvop5-uae"
        gitr = "a1f503d20b7846375c74ce5f7d0f8f6620ab7511"
        for klass in [
            WaitForProposalAcceptance,
            WaitForReplicaRevisionUpdated,
            WaitUntilNoAlertsOnSubnet,
        ]:
            klass(task_id=klass.__name__, subnet_id=sid, git_revision=gitr)


class TestWaitForReplicaRevisionUpdated(unittest.TestCase):
    def _exercise(self) -> bool:
        k = WaitForReplicaRevisionUpdated(
            task_id="x",
            subnet_id="yinp6-35cfo-wgcd2",
            git_revision="d5eb7683e144acb0f8850fedb29011f34bfbe4ac",
        )
        return k.poke({})  # type:ignore

    def test_happy_path(self) -> None:
        inp = [
            PrometheusVectorResultEntry(
                metric={
                    "ic_subnet": "yinp6-35cfo-wgcd2",
                    "ic_active_version": "d5eb7683e144acb0f8850fedb29011f34bfbe4ac",
                },
                value="13",
                timestamp=0.0,
            )
        ]
        exp = True
        with mock.patch("dfinity.prom_api.query_prometheus_servers") as m:
            m.return_value = inp
            res = self._exercise()
            assert res == exp, f"{res} != {exp}"

    def test_halfway_done(self) -> None:
        inp = [
            PrometheusVectorResultEntry(
                metric={
                    "ic_subnet": "yinp6-35cfo-wgcd2",
                    "ic_active_version": "oldversion",
                },
                value="4",
                timestamp=0.0,
            ),
            PrometheusVectorResultEntry(
                metric={
                    "ic_subnet": "yinp6-35cfo-wgcd2",
                    "ic_active_version": "d5eb7683e144acb0f8850fedb29011f34bfbe4ac",
                },
                value="7",
                timestamp=0.0,
            ),
        ]
        exp = False
        with mock.patch("dfinity.prom_api.query_prometheus_servers") as m:
            m.return_value = inp
            res = self._exercise()
            res = self._exercise()
            assert res == exp, f"{res} != {exp}"

    def test_not_started(self) -> None:
        inp = [
            PrometheusVectorResultEntry(
                metric={
                    "ic_subnet": "yinp6-35cfo-wgcd2",
                    "ic_active_version": "oldversion",
                },
                value="4",
                timestamp=0.0,
            )
        ]
        exp = False
        with mock.patch("dfinity.prom_api.query_prometheus_servers") as m:
            m.return_value = inp
            res = self._exercise()
            assert res == exp, f"{res} != {exp}"
