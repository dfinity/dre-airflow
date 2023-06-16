"""
IC-OS rollout operators.
"""
from typing import Any, Sequence

import dfinity.ic_api as ic_api

from airflow.models.baseoperator import BaseOperator
from airflow.template.templater import Templater
from airflow.utils.context import Context


class RolloutParams(Templater):
    template_fields: Sequence[str] = ("subnet_id", "git_revision")
    subnet_id: str
    git_revision: str

    def __init__(self, *, subnet_id: str, git_revision: str) -> None:
        self.subnet_id = subnet_id
        self.git_revision = git_revision


class ICRolloutBaseOperator(RolloutParams, BaseOperator):
    def __init__(
        self,
        *,
        task_id: str,
        subnet_id: str,
        git_revision: str,
        **kwargs: Any,
    ):
        RolloutParams.__init__(self, subnet_id=subnet_id, git_revision=git_revision)
        BaseOperator.__init__(self, task_id=task_id, **kwargs)


class CreateProposalIdempotently(ICRolloutBaseOperator):
    def execute(self, context: Context) -> None:
        props = ic_api.get_proposals_for_subnet_and_revision(
            subnet_id=self.subnet_id,
            git_revision=self.git_revision,
            limit=1000,
        )
        executeds = [
            p
            for p in props
            if p["status"] == ic_api.ProposalStatus.PROPOSAL_STATUS_EXECUTED
        ]
        opens = [
            p
            for p in props
            if p["status"] == ic_api.ProposalStatus.PROPOSAL_STATUS_OPEN
        ]
        if executeds:
            print(
                f"Proposal {executeds[0]['id']} titled {executeds[0]['title']}"
                f" has executed.  No need to do anything."
            )
        elif opens:
            print(
                f"Proposal {opens[0]['id']} titled {opens[0]['title']}"
                f" is open.  Continuing to next step until proposal has executed."
            )
        else:
            # Here we create a proposal.  FIXME.
            if not props:
                print(
                    f"No proposals for subnet id {self.subnet_id} to "
                    + f"adopt revision {self.git_revision}"
                )
            else:
                print("The following proposals neither open nor executed exist")
                for p in props:
                    print("p['id']: p['title'] in status p['status']")
            print(
                f"Creating proposal for subnet id {self.subnet_id} to "
                + f"adopt revision {self.git_revision}"
            )
