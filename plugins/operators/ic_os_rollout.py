"""
IC-OS rollout operators.
"""

import itertools
from typing import Any, Sequence

import dfinity.ic_admin as ic_admin
import dfinity.ic_api as ic_api
import dfinity.ic_types as ic_types

from airflow.models.baseoperator import BaseOperator
from airflow.template.templater import Templater
from airflow.utils.context import Context

# FIXME: these need to be parameterizable.
PROPOSER_NEURON_ID = 80
PROPOSER_NEURON_PATH = "/nonexistent.pem"


class RolloutParams(Templater):
    template_fields: Sequence[str] = ("subnet_id", "git_revision")
    subnet_id: str
    git_revision: str
    network: ic_types.ICNetwork

    def __init__(
        self, *, subnet_id: str, git_revision: str, network: ic_types.ICNetwork
    ) -> None:
        self.subnet_id = subnet_id
        self.git_revision = git_revision
        self.network = network


class ICRolloutBaseOperator(RolloutParams, BaseOperator):
    def __init__(
        self,
        *,
        task_id: str,
        subnet_id: str,
        git_revision: str,
        network: ic_types.ICNetwork,
        **kwargs: Any,
    ):
        RolloutParams.__init__(
            self,
            subnet_id=subnet_id,
            git_revision=git_revision,
            network=network,
        )
        BaseOperator.__init__(self, task_id=task_id, **kwargs)


class CreateProposalIdempotently(ICRolloutBaseOperator):
    template_fields = tuple(
        itertools.chain.from_iterable(
            (ICRolloutBaseOperator.template_fields, ("simulate_proposal",))
        )
    )
    simulate_proposal: bool

    def __init__(
        self,
        *,
        task_id: str,
        subnet_id: str,
        git_revision: str,
        simulate_proposal: bool,
        network: ic_types.ICNetwork,
        **kwargs: Any,
    ):
        ICRolloutBaseOperator.__init__(
            self,
            task_id=task_id,
            subnet_id=subnet_id,
            git_revision=git_revision,
            network=network,
            **kwargs,
        )
        self.simulate_proposal = simulate_proposal

    def execute(self, context: Context) -> None:
        props = ic_api.get_proposals_for_subnet_and_revision(
            subnet_id=self.subnet_id,
            git_revision=self.git_revision,
            limit=1000,
            network=self.network,
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
                f"Proposal"
                f" {self.network.proposal_display_url}/{executeds[0]['proposal_id']}"
                f" titled {executeds[0]['title']}"
                f" has executed.  No need to do anything."
            )
        elif opens:
            print(
                "Proposal"
                " {self.network.proposal_display_url}/{opens[0]['proposal_id']}"
                " titled {opens[0]['title']}"
                " is open.  Continuing to next step until proposal has executed."
            )
        else:
            if not props:
                print(
                    f"No proposals for subnet ID {self.subnet_id} to "
                    + f"adopt revision {self.git_revision}."
                )
            else:
                print("The following proposals neither open nor executed exist:")
                for p in props:
                    print(f"* {self.network.proposal_display_url}/{p['proposal_id']}")
            if not self.simulate_proposal:
                raise NotImplementedError
            print(
                f"Creating proposal for subnet ID {self.subnet_id} to "
                + f"adopt revision {self.git_revision}."
            )
            ic_admin.propose_to_update_subnet_replica_version(
                self.subnet_id,
                self.git_revision,
                PROPOSER_NEURON_ID,
                PROPOSER_NEURON_PATH,
                self.network,
                dry_run=True,
            )
            if self.simulate_proposal:
                print("The proposal creation was successfully simulated.")
            else:
                ic_admin.propose_to_update_subnet_replica_version(
                    self.subnet_id,
                    self.git_revision,
                    PROPOSER_NEURON_ID,
                    PROPOSER_NEURON_PATH,
                    self.network,
                )
