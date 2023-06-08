"""
IC-OS rollout sensors.
"""

from typing import Any

from airflow.utils.context import Context
from operators.ic_os_rollout import SubnetRevisionBase


class DummyCountable(SubnetRevisionBase):
    def __init__(self, subnet_id: str, git_revision: str, **kwargs: Any):
        super().__init__(subnet_id=subnet_id, git_revision=git_revision, **kwargs)
        self.counter = 2


class WaitForProposalAcceptance(DummyCountable):
    def poke(self, context: Context) -> bool:
        # FIXME implement.
        if self.counter > 0:
            print(
                f"Checking that proposal for subnet id {self.subnet_id} to "
                + f"adopt revision {self.git_revision} has been accepted"
            )
            self.counter = self.counter - 1
            return False
        else:
            return True


class WaitForReplicaRevisionUpdated(DummyCountable):
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


class WaitUntilNoAlertsOnSubnet(DummyCountable):
    def poke(self, context: Context) -> bool:
        # FIXME implement.
        if self.counter > 0:
            print(f"Waiting for all alerts on subnet id {self.subnet_id} to quiesce")
            self.counter = self.counter - 1
            return False
        else:
            return True
