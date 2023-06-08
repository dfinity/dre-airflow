"""
IC-OS rollout operators.
"""

from typing import Any

from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context


class SubnetRevisionBase(BaseOperator):
    def __init__(self, *, subnet_id: str, git_revision: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.subnet_id = subnet_id
        self.git_revision = git_revision


class CreateProposalIdempotently(SubnetRevisionBase):
    def execute(self, context: Context) -> None:
        # FIXME implement.
        print(
            f"Creating proposal for subnet id {self.subnet_id} to "
            + f"adopt revision {self.git_revision}"
        )
