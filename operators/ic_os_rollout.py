"""
IC-OS rollout operators.
"""
from typing import Any, Sequence

import operators.ic_api as ic_api
from airflow.models.baseoperator import BaseOperator
from airflow.template.templater import Templater
from airflow.utils.context import Context


def get_proposals_for_subnet_and_revision(
    git_revision: str, subnet_id: str, limit: int, offset: int = 0
) -> list[ic_api.Proposal]:
    return [
        r
        for r in ic_api.get_proposals(
            topic=ic_api.ProposalTopic.TOPIC_SUBNET_REPLICA_VERSION_MANAGEMENT,
            limit=limit,
            offset=offset,
        )
        if r["payload"].get("subnet_id") == subnet_id
        and r["payload"].get("replica_version_id") == git_revision
    ]


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
        props = get_proposals_for_subnet_and_revision(
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


if __name__ == "__main__":
    import pprint

    for p in get_proposals_for_subnet_and_revision(
        subnet_id="yinp6-35cfo-wgcd2-oc4ty-2kqpf-t4dul-rfk33-fsq3r-mfmua-m2ngh-jqe",
        git_revision="d5eb7683e144acb0f8850fedb29011f34bfbe4ac",
        limit=1000,
    ):
        pprint.pprint(p)
