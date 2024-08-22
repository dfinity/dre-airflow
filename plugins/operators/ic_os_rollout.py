"""
IC-OS rollout operators.
"""

import itertools
from typing import Any, Sequence

import dfinity.dre as dre
import dfinity.ic_types as ic_types
import dfinity.prom_api as prom
import yaml
from dfinity.ic_os_rollout import (
    DR_DRE_SLACK_ID,
    SLACK_CHANNEL,
    SLACK_CONNECTION_ID,
    RolloutPlanWithRevision,
    SubnetIdWithRevision,
    assign_default_revision,
    check_plan,
    rollout_planner,
    subnet_id_and_git_revision_from_args,
)

import airflow.providers.slack.operators.slack as slack
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.hooks.subprocess import SubprocessHook
from airflow.models.baseoperator import BaseOperator
from airflow.template.templater import Templater
from airflow.utils.context import Context


class RolloutParams(Templater):
    template_fields: Sequence[str] = ("subnet_id", "git_revision")
    subnet_id: str | SubnetIdWithRevision
    git_revision: str
    network: ic_types.ICNetwork

    def __init__(
        self,
        *,
        subnet_id: str | SubnetIdWithRevision,
        git_revision: str,
        network: ic_types.ICNetwork,
    ) -> None:
        self.subnet_id = subnet_id
        self.git_revision = git_revision
        self.network = network


class ICRolloutBaseOperator(RolloutParams, BaseOperator):
    def __init__(
        self,
        *,
        task_id: str,
        subnet_id: str | SubnetIdWithRevision,
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
        subnet_id: str | SubnetIdWithRevision,
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

    def execute(self, context: Context) -> dict[str, int | str | bool]:
        subnet_id, git_revision = subnet_id_and_git_revision_from_args(
            self.subnet_id, self.git_revision
        )
        runner = dre.DRE(network=self.network, subprocess_hook=SubprocessHook())

        # Get proposals sorted by proposal number.
        props = sorted(
            runner.get_ic_os_version_deployment_proposals_for_subnet(
                subnet_id=subnet_id,
            ),
            key=lambda prop: -prop["proposal_id"],
        )
        props_for_git_revision = [
            p for p in props if p["payload"]["replica_version_id"] == git_revision
        ]

        if self.simulate_proposal:
            self.log.info(f"simulate_proposal={self.simulate_proposal}")

        try:
            res = int(
                prom.query_prometheus_servers(
                    self.network.prometheus_urls,
                    "sum(ic_replica_info{"
                    f'ic_subnet="{subnet_id}"'
                    "}) by (ic_subnet)",
                )[0]["value"]
            )
            self.log.info("Remembering current replica count (%s)...", res)
            self.xcom_push(
                context=context,
                key="replica_count",
                value=res,
            )
        except IndexError:
            raise RuntimeError(f"No replicas have been found with subnet {subnet_id}")

        if not props:
            self.log.info("No proposals for subnet.  Will create.")
        elif not props_for_git_revision:
            self.log.info(
                "No proposals with revision %s for subnet.  Will create.", git_revision
            )
        elif props_for_git_revision[0]["proposal_id"] < props[0]["proposal_id"]:
            self.log.info(
                "Proposal %s with git revision %s for subnet "
                "is not the last (%s).  Will create.",
                props_for_git_revision[0]["proposal_id"],
                git_revision,
                props[0]["proposal_id"],
            )
        elif props_for_git_revision[0]["status"] not in (
            ic_types.ProposalStatus.PROPOSAL_STATUS_OPEN,
            ic_types.ProposalStatus.PROPOSAL_STATUS_ADOPTED,
            ic_types.ProposalStatus.PROPOSAL_STATUS_EXECUTED,
        ):
            self.log.info(
                "Proposal %s with git revision %s for subnet "
                "is in state %s and must be created again.  Will create.",
                props_for_git_revision[0]["proposal_id"],
                git_revision,
                props_for_git_revision[0]["status"],
            )
        else:
            prop = props_for_git_revision[0]
            self.log.info(
                "Proposal %s with git revision %s for subnet "
                "is in state %s and does not need to be created.",
                prop["proposal_id"],
                git_revision,
                prop["status"].name,
            )
            url = f"{self.network.proposal_display_url}/{prop['proposal_id']}"
            self.log.info(
                "Proposal " + url + f" titled {prop['title']}"
                f" has executed.  No need to do anything."
            )
            return {
                "proposal_id": int(prop["proposal_id"]),
                "proposal_url": url,
                "needs_vote": prop["status"]
                == ic_types.ProposalStatus.PROPOSAL_STATUS_OPEN,
            }

        self.log.info(
            f"Creating proposal for subnet ID {subnet_id} to "
            + f"adopt revision {git_revision} (simulate {self.simulate_proposal})."
        )

        proposal_number = (
            runner.authenticated().propose_to_update_subnet_replica_version(
                subnet_id, git_revision, dry_run=self.simulate_proposal
            )
        )

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
        _ignored: Any = None,
        **kwargs: Any,
    ) -> None:
        self.source_task_id = source_task_id
        dr_dre_slack_id = DR_DRE_SLACK_ID
        text = (
            (
                """Proposal <{{
                    task_instance.xcom_pull(
                        task_ids='%(source_task_id)s',
                        map_indexes=task_instance.map_index,
                    ).proposal_url
                }}|{{
                    task_instance.xcom_pull(
                        task_ids='%(source_task_id)s',
                        map_indexes=task_instance.map_index,
                    ).proposal_id
                }}> is now up for voting. <!subteam^%(dr_dre_slack_id)s>"""
                """ please vote for the proposal using your HSM."""
            )
            % locals()
        )
        slack.SlackAPIPostOperator.__init__(
            self,
            channel=SLACK_CHANNEL,
            username="Airflow",
            text=text,
            slack_conn_id=SLACK_CONNECTION_ID,
            **kwargs,
        )

    def execute(self, context: Context) -> None:
        proposal_creation_result = context["task_instance"].xcom_pull(
            task_ids=self.source_task_id,
            map_indexes=context["task_instance"].map_index,
        )
        if proposal_creation_result["proposal_id"] == dre.FAKE_PROPOSAL_NUMBER:
            self.log.info("Fake proposal.  Not requesting vote.")
        elif not proposal_creation_result["needs_vote"]:
            self.log.info("Proposal does not need vote.  Not requesting vote.")
        else:
            self.log.info("Requesting vote on proposal with text: %s", self.text)
            slack.SlackAPIPostOperator.execute(self, context=context)


class NotifyAboutStalledSubnet(slack.SlackAPIPostOperator):
    def __init__(
        self,
        subnet_id: str,
        _ignored: Any = None,
        **kwargs: Any,
    ) -> None:
        dr_dre_slack_id = DR_DRE_SLACK_ID
        text = (
            """Subnet `%(subnet_id)s` has not finished upgrading in over an hour."""
            """  <!subteam^%(dr_dre_slack_id)s>"""
            """ please investigate *as soon as possible*."""
        ) % locals()
        slack.SlackAPIPostOperator.__init__(
            self,
            channel=SLACK_CHANNEL,
            username="Airflow",
            text=text,
            slack_conn_id=SLACK_CONNECTION_ID,
            **kwargs,
        )


class UpgradeUnassignedNodes(BaseOperator):
    template_fields = ("simulate",)
    network: ic_types.ICNetwork
    simulate: bool

    def __init__(
        self,
        *,
        task_id: str,
        network: ic_types.ICNetwork,
        simulate: bool,
        **kwargs: Any,
    ):
        self.simulate = simulate
        self.network = network
        BaseOperator.__init__(self, task_id=task_id, **kwargs)

    def execute(self, context: Context) -> None:
        if self.simulate:
            self.log.info(f"simulate={self.simulate}")
        p = (
            dre.DRE(network=self.network, subprocess_hook=SubprocessHook())
            .authenticated()
            .upgrade_unassigned_nodes(dry_run=self.simulate)
        )
        if p.exit_code != 0:
            raise AirflowException("dre exited with status code %d", p.exit_code)


@task
def schedule(
    network: ic_types.ICNetwork, **context: dict[str, Any]
) -> RolloutPlanWithRevision:
    plan_data_structure = yaml.safe_load(
        context["task"].render_template(  # type: ignore
            "{{ params.plan }}",
            context,
        )
    )
    default_git_revision = "{:040}".format(
        context["task"].render_template(  # type: ignore
            "{{ params.git_revision }}",
            context,
        )
    )
    subnet_list_source = dre.DRE(
        network=network,
        subprocess_hook=SubprocessHook(),
    ).get_subnet_list

    plan = assign_default_revision(
        rollout_planner(
            plan_data_structure,
            subnet_list_source=subnet_list_source,
        ),
        default_git_revision,
    )

    for nstr, (_, members) in plan.items():
        print(f"Batch {int(nstr)+1}:")
        for item in members:
            print(
                f"    Subnet {item.subnet_id} ({item.subnet_num}) will start"
                f" to be rolled out at {item.start_at} to git"
                f" revision {item.git_revision}."
            )

    try:
        check_plan(plan)
    except Exception as e:
        print("Cannot proceed with rollout plan as planned: %s" % e)
        raise AirflowException("Unsafe rollout plan")

    return plan


if __name__ == "__main__":
    network = ic_types.ICNetwork(
        "https://ic0.app/",
        "https://dashboard.internetcomputer.org/proposal",
        "https://dashboard.internetcomputer.org/release",
        ["https://victoria.mainnet.dfinity.network/select/0/prometheus/api/v1/query"],
        80,
        "dfinity.ic_admin.mainnet.proposer_key_file",
    )
    x = UpgradeUnassignedNodes(task_id="upgrade", simulate=True, network=network)
    x.execute({})
