"""
IC-OS rollout sensors.
"""

from typing import Any

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from operators.ic_os_rollout import (
    ProposalStatus,
    RolloutParams,
    get_proposals_for_subnet_and_revision,
)


class CountableSensor(RolloutParams, BaseSensorOperator):
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
        self.counter = 2


class WaitForProposalAcceptance(CountableSensor):
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


class WaitForReplicaRevisionUpdated(CountableSensor):
    def poke(self, context: Context) -> bool:
        # FIXME implement.
        if self.counter > 0:
            print(
                f"Waiting for all nodes on subnet id {self.subnet_id} have "
                + f"adopted revision {self.git_revision}"
            )
            self.counter = self.counter - 1
            return False
        else:
            return True


class WaitUntilNoAlertsOnSubnet(CountableSensor):
    def poke(self, context: Context) -> bool:
        # FIXME implement.
        if self.counter > 0:
            print(f"Waiting for all alerts on subnet id {self.subnet_id} to quiesce")
            self.counter = self.counter - 1
            return False
        else:
            return True
