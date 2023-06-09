"""
IC-OS rollout sensors.
"""

from typing import Any

import sensors.prom_api as prom
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from operators.ic_api import ProposalStatus
from operators.ic_os_rollout import RolloutParams, get_proposals_for_subnet_and_revision


class ICRolloutSensorBaseOperator(RolloutParams, BaseSensorOperator):
    def __init__(
        self,
        *,
        task_id: str,
        subnet_id: str,
        git_revision: str,
        **kwargs: Any,
    ):
        RolloutParams.__init__(self, subnet_id=subnet_id, git_revision=git_revision)
        BaseSensorOperator.__init__(self, task_id=task_id, **kwargs)


class WaitForProposalAcceptance(ICRolloutSensorBaseOperator):
    def poke(self, context: Context) -> bool:
        props = get_proposals_for_subnet_and_revision(
            subnet_id=self.subnet_id,
            git_revision=self.git_revision,
            limit=1000,
        )
        executeds = [
            p for p in props if p["status"] == ProposalStatus.PROPOSAL_STATUS_EXECUTED
        ]
        opens = [p for p in props if p["status"] == ProposalStatus.PROPOSAL_STATUS_OPEN]
        if not opens and not executeds:
            raise RuntimeError(
                "No proposal is either open or executed to update"
                f" {self.subnet_id} to revision {self.git_revision}"
            )
        if executeds:
            print(
                f"Proposal {executeds[0]['id']} titled {executeds[0]['title']}"
                f" has executed.  We can proceed."
            )
            return True
        print(
            f"Proposal {opens[0]['id']} titled {opens[0]['title']}"
            f" is open.  Waiting until it has executed."
        )
        return False


class WaitForReplicaRevisionUpdated(ICRolloutSensorBaseOperator):
    api_urls = [
        "https://ic-metrics-prometheus.ch1-obs1.dfinity.network/api/v1/query",
        "https://ic-metrics-prometheus.fr1-obs1.dfinity.network/api/v1/query",
    ]

    def poke(self, context: Context) -> bool:
        print(
            f"Waiting for all nodes on subnet id {self.subnet_id} have "
            + f"adopted revision {self.git_revision}"
        )
        query = (
            "sum(ic_replica_info{"
            + f'ic_subnet="{self.subnet_id}"'
            + "}) by (ic_active_version, ic_subnet)"
        )
        res = prom.query_prometheus_servers(self.api_urls, query)
        if len(res) == 1 and res[0]["metric"]["ic_active_version"] == self.git_revision:
            print(
                f"All {res[0]['value']} nodes in subnet {self.subnet_id} have"
                f" updated to revision {self.git_revision}"
            )
            return True
        print("Upgrade is not complete yet.  From Prometheus:")
        for r in res:
            print(r)
        return False


class WaitUntilNoAlertsOnSubnet(ICRolloutSensorBaseOperator):
    def poke(self, context: Context) -> bool:
        # FIXME implement
        print(
            f"Waiting for all alerts on subnet id {self.subnet_id} to quiesce... done!"
        )
        return True


if __name__ == "__main__":
    k = WaitForReplicaRevisionUpdated(
        task_id="x",
        subnet_id="yinp6-35cfo-wgcd2-oc4ty-2kqpf-t4dul-rfk33-fsq3r-mfmua-m2ngh-jqe",
        git_revision="d5eb7683e144acb0f8850fedb29011f34bfbe4ac",
    )
    k.poke({})
