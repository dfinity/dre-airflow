"""
IC-OS rollout operators.
"""

import itertools
import re
import subprocess
from typing import Any, Sequence

import dfinity.ic_admin as ic_admin
import dfinity.ic_api as ic_api
import dfinity.ic_types as ic_types

import airflow.models
from airflow.models.baseoperator import BaseOperator
from airflow.template.templater import Templater
from airflow.utils.context import Context


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

    def execute(self, context: Context) -> int:
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
            self.log.info(
                f"Proposal"
                f" {self.network.proposal_display_url}/{executeds[0]['proposal_id']}"
                f" titled {executeds[0]['title']}"
                f" has executed.  No need to do anything."
            )
            return int(executeds[0]["proposal_id"])

        if opens:
            self.log.info(
                "Proposal"
                " {self.network.proposal_display_url}/{opens[0]['proposal_id']}"
                " titled {opens[0]['title']}"
                " is open.  Continuing to next step until proposal has executed."
            )
            return int(opens[0]["proposal_id"])

        if not props:
            self.log.info(
                f"No proposals for subnet ID {self.subnet_id} to "
                + f"adopt revision {self.git_revision}."
            )
        else:
            self.log.info("The following proposals neither open nor executed exist:")
            for p in props:
                self.log.info(
                    f"* {self.network.proposal_display_url}/{p['proposal_id']}"
                )
        if not self.simulate_proposal:
            raise NotImplementedError
        self.log.info(
            f"Creating proposal for subnet ID {self.subnet_id} to "
            + f"adopt revision {self.git_revision}."
        )

        # Use forrealz private key only when rolling out forrealz.
        pkey = (
            """-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIEFRa42BSz1uuRxWBh60vePDrpkgtELJJMZtkJGlExuLoAoGCCqGSM49
AwEHoUQDQgAEyiUJYA7SI/u2Rf8ouND0Ip46gdjKcGB8Vx3VkajFx5+YhtaMfHb1
5YjfGWFuNLqyxLGGvDUq6HlGsBJ9QIcPtA==
-----END EC PRIVATE KEY-----"""
            if self.simulate_proposal
            else airflow.models.Variable.get(
                self.network.proposer_neuron_private_key_variable_name
            )
        )
        net = ic_types.augment_network_with_private_key(self.network, pkey)
        # Force use of Git revision for ic-admin rollout,
        # but only when rolling out forrealz.
        ic_admin_version = None if self.simulate_proposal else self.git_revision

        try:
            proc = ic_admin.propose_to_update_subnet_replica_version(
                self.subnet_id,
                self.git_revision,
                net,
                ic_admin_version=ic_admin_version,
                dry_run=self.simulate_proposal,
            )
            self.log.info("Standard output: %s", proc.stdout)
            self.log.info("Standard error: %s", proc.stderr)
        except subprocess.CalledProcessError as exc:
            self.log.error(
                "Failure to execute ic-admin (return code %d)", exc.returncode
            )
            self.log.error("Standard output: %s", exc.stdout)
            self.log.error("Standard error: %s", exc.stderr)
            raise

        if self.simulate_proposal:
            return 123456
        proposal_number = int(
            re.search("Ok.proposal ([0-9]+).", proc.stdout).groups(1)[0]
        )
        return proposal_number
