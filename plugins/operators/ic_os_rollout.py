"""
IC-OS rollout operators.
"""

import itertools
import re
import subprocess
from typing import Any, Sequence, cast

import dfinity.ic_admin as ic_admin
import dfinity.ic_api as ic_api
import dfinity.ic_types as ic_types
import jinja2
from dfinity.ic_os_rollout import SLACK_CHANNEL, SLACK_CONNECTION_ID

import airflow.models
import airflow.providers.slack.operators.slack as slack
from airflow.models.baseoperator import BaseOperator
from airflow.template.templater import Templater
from airflow.utils.context import Context

FAKE_PROPOSAL_NUMBER = -123456


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

    def render_template_fields(
        self,
        context: Context,
        jinja_env: jinja2.Environment | None = None,
    ) -> None:
        # Ensure our simulate variable is a bool, since
        # Jinja rendering makes this a string.
        super().render_template_fields(context=context, jinja_env=jinja_env)
        self.simulate_proposal = cast(str, self.simulate_proposal) == "True"

    def execute(self, context: Context) -> dict[str, int | str | bool]:
        props = ic_api.get_proposals_for_subnet_and_revision(
            subnet_id=self.subnet_id,
            git_revision=self.git_revision,
            limit=1000,
            network=self.network,
        )

        def per_status(
            props: list[ic_api.Proposal], status: ic_api.ProposalStatus
        ) -> list[ic_api.Proposal]:
            return [p for p in props if p["status"] == status]

        executeds = per_status(props, ic_api.ProposalStatus.PROPOSAL_STATUS_EXECUTED)
        opens = per_status(props, ic_api.ProposalStatus.PROPOSAL_STATUS_OPEN)

        if self.simulate_proposal:
            self.log.info(f"simulate_proposal={self.simulate_proposal}")

        if executeds:
            url = f"{self.network.proposal_display_url}/{executeds[0]['proposal_id']}"
            self.log.info(
                "Proposal " + url + f" titled {executeds[0]['title']}"
                f" has executed.  No need to do anything."
            )
            return {
                "proposal_id": int(executeds[0]["proposal_id"]),
                "proposal_url": url,
                "needs_vote": False,
            }

        if opens:
            url = f"{self.network.proposal_display_url}/{opens[0]['proposal_id']}"
            self.log.info(
                "Proposal " + url + f" titled {opens[0]['title']}"
                " is open.  Continuing to next step until proposal has executed."
            )
            return {
                "proposal_id": int(opens[0]["proposal_id"]),
                "proposal_url": url,
                "needs_vote": True,
            }

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
            proposal_number = FAKE_PROPOSAL_NUMBER
        else:
            search_result = re.search("^proposal ([0-9]+)$", proc.stdout, re.MULTILINE)
            assert (
                search_result
            ), "Proposal creation did not create a proposal number yet it did not bomb."
            proposal_number = int(search_result.groups(1)[0])
        url = f"{self.network.proposal_display_url}/{proposal_number}"
        return {
            "proposal_id": proposal_number,
            "proposal_url": url,
            "needs_vote": True,
        }


class RequestProposalVote(slack.SlackAPIPostOperator):
    def __init__(
        self,
        source_task_id: str,
        **kwargs: Any,
    ) -> None:
        self.source_task_id = source_task_id
        text = (
            """Proposal <{{
      task_instance.xcom_pull(
        task_ids='"""
            + source_task_id
            + """',
        map_indexes=task_instance.map_index,
      ).proposal_url
}}|{{
      task_instance.xcom_pull(
        task_ids='"""
            + source_task_id
            + """',
        map_indexes=task_instance.map_index,
      ).proposal_id
}}> is now ready for voting.  Please vote for this proposal."""
        )
        slack.SlackAPIPostOperator.__init__(
            self,
            channel=SLACK_CHANNEL,
            username="Airflow",
            text=text,
            slack_conn_id=SLACK_CONNECTION_ID,
            **kwargs,
        )

    def execute(self, context: Context) -> None:  # type:ignore
        proposal_creation_result = context["task_instance"].xcom_pull(
            task_ids=self.source_task_id,
            map_indexes=context["task_instance"].map_index,
        )
        if proposal_creation_result["proposal_id"] == FAKE_PROPOSAL_NUMBER:
            self.log.info("Fake proposal.  Not requesting vote.")
        elif not proposal_creation_result["needs_vote"]:
            self.log.info("Proposal does not need vote.  Not requesting vote.")
        else:
            self.log.info("Requesting vote on proposal with text: %s", self.text)
            slack.SlackAPIPostOperator.execute(self, context=context)  # type:ignore
