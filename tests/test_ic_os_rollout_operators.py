# mypy: disable-error-code=unused-ignore

import contextlib
import unittest

import mock
from dfinity.ic_types import (
    IC_NETWORKS,
    AbbrevProposal,
    ProposalStatus,
    ProposalTopic,
)
from operators.ic_os_rollout import (
    CreateStandardEngineProposalIdempotently,
    CreateSubnetUpdateProposalIdempotently,
)


class TestOperators(unittest.TestCase):
    def test_instantiation(self) -> None:
        sid = "qn2sv-gibnj-5jrdq-3irkq-ozzdo-ri5dn-dynlb-xgk6d-kiq7w-cvop5-uae"
        gitr = "a1f503d20b7846375c74ce5f7d0f8f6620ab7511"
        CreateSubnetUpdateProposalIdempotently(
            task_id="xxx",
            subnet_id=sid,
            git_revision=gitr,
            simulate_proposal=True,
            network=IC_NETWORKS["mainnet"],
        )


class TestCreateProposal(unittest.TestCase):
    network = IC_NETWORKS["mainnet"]

    def _exercise(self) -> CreateSubnetUpdateProposalIdempotently:
        def fake_xcom_push(key, value, context=None):  # type:ignore
            return

        k = CreateSubnetUpdateProposalIdempotently(
            task_id="x",
            subnet_id="yinp6-35cfo-wgcd2",
            git_revision="d5eb7683e144acb0f8850fedb29011f34bfbe4ac",
            simulate_proposal=True,
            network=IC_NETWORKS["mainnet"],
        )
        k.xcom_push = fake_xcom_push  # type: ignore
        return k

    def _prop(
        self,
        proposal_id: int,
        proposal_status: ProposalStatus,
        git_revision: str = "d5eb7683e144acb0f8850fedb29011f34bfbe4ac",
    ) -> AbbrevProposal:
        return {
            "proposal_id": proposal_id,
            "payload": {
                "replica_version_id": git_revision,
                "subnet_id": ("yinp6-35cfo-wgcd2"),
            },
            "proposer": 80,
            "status": proposal_status,
            "summary": "Update subnet "
            "yinp6-35cfo-wgcd2"
            "to replica version "
            f"[{git_revision}](https:##dashboard.internetcomputer.org/release/{git_revision})\n",
            "title": f"Update subnet yinp6 to replica version {git_revision}",
            "topic": ProposalTopic.TOPIC_IC_OS_VERSION_DEPLOYMENT,
        }

    @contextlib.contextmanager
    def _ctx(self):  # type:ignore
        with (
            mock.patch(
                "dfinity.dre.DRE.get_ic_os_version_deployment_proposals_for_subnet"
            ) as m,
            mock.patch("dfinity.prom_api.query_prometheus_servers") as n,
            mock.patch(
                "dfinity.dre.AuthenticatedDRE.propose_to_update_subnet_replica_version"
            ) as p,
            mock.patch("airflow.models.variable.Variable.get") as v,
        ):
            n.return_value = [{"value": 13}]
            v.return_value = "FAKE CERT"
            yield m, p

    def test_no_proposals(self) -> None:
        with self._ctx() as (m, p):
            m.return_value = []
            p.return_value = -123456
            res = self._exercise().execute({})
            assert res["needs_vote"] is True
            assert res["proposal_id"] == p.return_value

    def test_latest_proposal_matches(self) -> None:
        with self._ctx() as (m, p):
            props: list[AbbrevProposal] = [
                self._prop(123456, ProposalStatus.PROPOSAL_STATUS_OPEN),
            ]
            m.return_value = props
            p.return_value = -123456
            res = self._exercise().execute({})
            assert res["needs_vote"] is True
            assert res["proposal_id"] == props[0]["proposal_id"]

            # Now let's test with proposal status executed.
            props[0]["status"] = ProposalStatus.PROPOSAL_STATUS_EXECUTED
            res = self._exercise().execute({})
            assert res["needs_vote"] is False
            assert res["proposal_id"] == props[0]["proposal_id"]

    def test_match_exists_but_not_latest(self) -> None:
        with self._ctx() as (m, p):
            props: list[AbbrevProposal] = [
                self._prop(123456, ProposalStatus.PROPOSAL_STATUS_EXECUTED),
                self._prop(
                    123457,
                    ProposalStatus.PROPOSAL_STATUS_OPEN,
                    git_revision="some other shit",
                ),
            ]
            m.return_value = props
            p.return_value = -123456
            res = self._exercise().execute({})
            assert res["needs_vote"] is True
            assert res["proposal_id"] == p.return_value

    def test_match_exists_is_latest(self) -> None:
        with self._ctx() as (m, p):
            props: list[AbbrevProposal] = [
                self._prop(123458, ProposalStatus.PROPOSAL_STATUS_EXECUTED),
                self._prop(
                    123457,
                    ProposalStatus.PROPOSAL_STATUS_OPEN,
                    git_revision="some other shit",
                ),
            ]
            m.return_value = props
            p.return_value = -123456
            res = self._exercise().execute({})
            assert res["needs_vote"] is False
            assert res["proposal_id"] == props[0]["proposal_id"]


class TestCreateStandardEngineProposal(unittest.TestCase):
    network = IC_NETWORKS["mainnet"]
    git_revision = "d5eb7683e144acb0f8850fedb29011f34bfbe4ac"
    previous_revision = "a1f503d20b7846375c74ce5f7d0f8f6620ab7511"

    def _exercise(
        self, deployment_progress: float
    ) -> CreateStandardEngineProposalIdempotently:
        return CreateStandardEngineProposalIdempotently(
            task_id="x",
            git_revision=self.git_revision,
            deployment_progress=deployment_progress,
            simulate_proposal=True,
            network=self.network,
        )

    @contextlib.contextmanager
    def _ctx(self):  # type:ignore
        with (
            mock.patch(
                "dfinity.dre.DRE.get_standard_engine_replica_version"
            ) as get_record,
            mock.patch(
                "dfinity.dre.AuthenticatedDRE"
                ".propose_to_update_standard_engine_replica_version"
            ) as propose,
            mock.patch("airflow.models.variable.Variable.get") as v,
        ):
            v.return_value = "FAKE CERT"
            yield get_record, propose

    def test_raises_when_no_record_exists(self) -> None:
        from airflow.exceptions import AirflowException

        with self._ctx() as (get_record, propose):
            get_record.return_value = None
            with self.assertRaises(AirflowException):
                self._exercise(0.2).execute({})
            propose.assert_not_called()

    def test_starts_new_deployment_when_versions_differ(self) -> None:
        # The registry currently points its new version at what a previous
        # rollout deployed; we start converging towards this rollout's revision.
        with self._ctx() as (get_record, propose):
            get_record.return_value = {
                "new_replica_version_id": self.previous_revision,
                "old_replica_version_id": "0" * 40,
                "deployment_progress": 1.0,
            }
            propose.return_value = -123456
            res = self._exercise(0.2).execute({})
            assert res["needs_vote"] is True
            assert res["new_replica_version_id"] == self.git_revision
            # We converge away from the previous rollout's version.
            assert res["old_replica_version_id"] == self.previous_revision
            assert res["deployment_progress"] == 0.2
            # The proposal was submitted with those versions.
            _, kwargs = propose.call_args
            assert kwargs["new_replica_version_id"] == self.git_revision
            assert kwargs["old_replica_version_id"] == self.previous_revision
            assert kwargs["deployment_progress"] == 0.2

    def test_mid_deployment_keeps_record_old_version(self) -> None:
        # We are mid-deployment: the record's new version is already this
        # rollout's revision.  We must keep the record's existing old version
        # (not set old = new), and only raise progress.
        with self._ctx() as (get_record, propose):
            get_record.return_value = {
                "new_replica_version_id": self.git_revision,
                "old_replica_version_id": self.previous_revision,
                "deployment_progress": 0.2,
            }
            propose.return_value = -123456
            res = self._exercise(0.4).execute({})
            assert res["needs_vote"] is True
            assert res["new_replica_version_id"] == self.git_revision
            # Crucially, old is the record's old version, not git_revision.
            assert res["old_replica_version_id"] == self.previous_revision
            assert res["deployment_progress"] == 0.4
            _, kwargs = propose.call_args
            assert kwargs["old_replica_version_id"] == self.previous_revision
            assert kwargs["new_replica_version_id"] == self.git_revision
            assert kwargs["deployment_progress"] == 0.4

    def test_idempotent_noop_returns_actual_progress(self) -> None:
        # The record is already at (or beyond) the target progress for this
        # revision, so no proposal is submitted and the actual (current)
        # progress is reported, not the requested target.
        with self._ctx() as (get_record, propose):
            get_record.return_value = {
                "new_replica_version_id": self.git_revision,
                "old_replica_version_id": self.previous_revision,
                "deployment_progress": 0.6,
            }
            res = self._exercise(0.4).execute({})
            assert res["needs_vote"] is False
            assert res["old_replica_version_id"] == self.previous_revision
            # Actual progress is returned, not the requested target of 0.4.
            assert res["deployment_progress"] == 0.6
            propose.assert_not_called()
