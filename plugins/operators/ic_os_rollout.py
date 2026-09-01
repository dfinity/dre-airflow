"""
IC-OS rollout operators.
"""

import itertools
from typing import Any, Sequence, cast

import airflow.providers.slack.operators.slack as slack
import dfinity.dre as dre
import dfinity.ic_types as ic_types
import dfinity.prom_api as prom
import dfinity.rollout_types as rollout_types
import yaml
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.hooks.subprocess import SubprocessHook
from airflow.models.baseoperator import BaseOperator
from airflow.template.templater import Templater
from airflow.utils.context import Context
from dfinity.ic_os_rollout import (
    DR_DRE_SLACK_ID,
    SLACK_CHANNEL,
    SLACK_CONNECTION_ID,
    SubnetIdWithRevision,
    SubnetRolloutPlanWithRevision,
    assign_default_revision,
    check_plan,
    rollout_planner,
    standard_engine_planner,
    subnet_id_and_git_revision_from_args,
)


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


class CreateSubnetUpdateProposalIdempotently(ICRolloutBaseOperator):
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
                    f'sum(ic_replica_info{{ic_subnet="{subnet_id}"}}) by (ic_subnet)',
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
                props_for_git_revision[0]["status"].name,
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
        ) % locals()
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
            self.log.info(
                "Fake proposal.  Not requesting vote."
                "  Here is the text that would be sent: %s",
                self.text,
            )
        elif not proposal_creation_result["needs_vote"]:
            self.log.info(
                "Proposal does not need vote.  Not requesting vote."
                "  Here is the text that would be sent: %s",
                self.text,
            )
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
            f"""Subnet `{subnet_id}` has not finished upgrading in over an hour."""
            f"""  <!subteam^{dr_dre_slack_id}>"""
            """ please investigate *as soon as possible*."""
        )
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


class UpgradeCloudEngines(BaseOperator):
    """
    Submit a `deploy-guestos-to-all-subnet-nodes` proposal for every subnet
    registered with type `cloud_engine`, targeting the rollout's git revision.

    Idempotent: a subnet is skipped if its most recent IC OS deployment
    proposal already targets the requested revision and is open / adopted /
    executed.  Succeeds as a no-op if no CloudEngine subnets exist.  Like
    `UpgradeUnassignedNodes`, this does NOT wait for proposal voting,
    proposal acceptance, replica revision rollout, or alert quiescence.
    """

    template_fields = ("simulate", "git_revision")
    network: ic_types.ICNetwork
    simulate: bool
    git_revision: str

    def __init__(
        self,
        *,
        task_id: str,
        network: ic_types.ICNetwork,
        git_revision: str,
        simulate: bool,
        **kwargs: Any,
    ):
        self.simulate = simulate
        self.git_revision = git_revision
        self.network = network
        BaseOperator.__init__(self, task_id=task_id, **kwargs)

    def execute(self, context: Context) -> None:
        if self.simulate:
            self.log.info(f"simulate={self.simulate}")
        runner = dre.DRE(network=self.network, subprocess_hook=SubprocessHook())
        cloud_engine_subnet_ids = runner.get_cloud_engine_subnet_ids()
        if not cloud_engine_subnet_ids:
            self.log.info(
                "No subnets with type cloud_engine are registered.  Nothing to do."
            )
            return

        self.log.info(
            "Found %d CloudEngine subnet(s); upgrading to revision %s: %s",
            len(cloud_engine_subnet_ids),
            self.git_revision,
            cloud_engine_subnet_ids,
        )

        authenticated_runner = runner.authenticated()
        for subnet_id in cloud_engine_subnet_ids:
            props = sorted(
                runner.get_ic_os_version_deployment_proposals_for_subnet(
                    subnet_id=subnet_id,
                ),
                key=lambda prop: -prop["proposal_id"],
            )
            props_for_git_revision = [
                p
                for p in props
                if p["payload"]["replica_version_id"] == self.git_revision
            ]
            if (
                props_for_git_revision
                and props_for_git_revision[0]["proposal_id"] == props[0]["proposal_id"]
                and props_for_git_revision[0]["status"]
                in (
                    ic_types.ProposalStatus.PROPOSAL_STATUS_OPEN,
                    ic_types.ProposalStatus.PROPOSAL_STATUS_ADOPTED,
                    ic_types.ProposalStatus.PROPOSAL_STATUS_EXECUTED,
                )
            ):
                self.log.info(
                    "Subnet %s already has proposal %s for revision %s in state %s;"
                    " skipping.",
                    subnet_id,
                    props_for_git_revision[0]["proposal_id"],
                    self.git_revision,
                    props_for_git_revision[0]["status"].name,
                )
                continue

            self.log.info(
                "Creating proposal for CloudEngine subnet %s to adopt revision %s"
                " (simulate=%s).",
                subnet_id,
                self.git_revision,
                self.simulate,
            )
            authenticated_runner.propose_to_update_subnet_replica_version(
                subnet_id=subnet_id,
                git_revision=self.git_revision,
                dry_run=self.simulate,
            )


@task
def schedule(
    network: ic_types.ICNetwork, **context: dict[str, Any]
) -> SubnetRolloutPlanWithRevision:
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
        print(f"Batch {int(nstr) + 1}:")
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


@task
def standard_engine_schedule(
    **context: dict[str, Any],
) -> list[dict[str, Any]]:
    """
    Parse the standard_engine_plan param into an ordered list of steps.

    Each step tells the rollout to submit, at a specific time, a proposal that
    raises the StandardEngineReplicaVersionRecord.deployment_progress to a given
    value.  The steps' new_replica_version_id is the rollout's main git_revision.

    The returned steps use ISO 8601 strings for `start_at` (rather than datetime
    objects) so the value is trivially serializable to XCom and consumable both
    by the wait-until-start-time sensor and by the rollout dashboard.
    """
    plan_data_structure = yaml.safe_load(
        context["task"].render_template(  # type: ignore
            "{{ params.standard_engine_plan }}",
            context,
        )
    )

    if not plan_data_structure:
        print("No standard engine plan specified.  Nothing to do.")
        return []

    plan = standard_engine_planner(plan_data_structure)

    steps: list[dict[str, Any]] = []
    for step in plan:
        print(
            f"At {step['start_at']} the standard engine deployment_progress"
            f" will be raised to {step['deployment_progress'] * 100:.0f}%."
        )
        steps.append(
            {
                "start_at": step["start_at"].isoformat(),
                "deployment_progress": step["deployment_progress"],
            }
        )

    return steps


class CreateStandardEngineProposalIdempotently(BaseOperator):
    """
    Submit a `propose-to-update-standard-engine-replica-version` proposal that
    raises the StandardEngineReplicaVersionRecord.deployment_progress to the
    target value for this step, upgrading a fraction of the Cloud Engines
    following the standard upgrade train towards the rollout's git revision.

    The new version is the rollout's git revision.  The old version is whatever
    the current record's `new_replica_version_id` is (i.e. what the previous
    rollout left in place).  If there is no current record, the rollout cannot
    proceed and fails, because there is no known old version to converge away
    from.

    Idempotent: if a record already targets this new version with a
    deployment_progress at or above the target, the step is a no-op.
    """

    template_fields = ("git_revision", "deployment_progress", "simulate_proposal")
    network: ic_types.ICNetwork
    git_revision: str
    deployment_progress: str | float
    simulate_proposal: bool

    def __init__(
        self,
        *,
        task_id: str,
        git_revision: str,
        deployment_progress: str | float,
        simulate_proposal: bool,
        network: ic_types.ICNetwork,
        **kwargs: Any,
    ):
        self.git_revision = git_revision
        self.deployment_progress = deployment_progress
        self.simulate_proposal = simulate_proposal
        self.network = network
        BaseOperator.__init__(self, task_id=task_id, **kwargs)

    def execute(self, context: Context) -> dict[str, int | str | float | bool]:
        _, git_revision = subnet_id_and_git_revision_from_args("", self.git_revision)
        target_progress = float(self.deployment_progress)

        if self.simulate_proposal:
            self.log.info(f"simulate_proposal={self.simulate_proposal}")

        runner = dre.DRE(network=self.network, subprocess_hook=SubprocessHook())
        current = runner.get_standard_engine_replica_version()

        if current is None:
            raise AirflowException(
                "There is no StandardEngineReplicaVersionRecord in the registry, so"
                " there is no known old version to converge away from.  A first"
                " record must be established manually before the rollout can manage"
                " the standard engine version."
            )

        if current["new_replica_version_id"] == git_revision:
            # The registry's current new version is already the version this
            # rollout is deploying, i.e. we are mid-deployment.  We must keep the
            # record's existing old version (converging to git_revision would be
            # an invalid new==old transition otherwise); we only adjust progress.
            old_replica_version_id = current["old_replica_version_id"]
            self.log.info(
                "The current standard engine new version is already %s (this"
                " rollout's revision); will only adjust deployment_progress.",
                git_revision,
            )
        else:
            # We are starting a new deployment towards git_revision, converging
            # away from whatever the record currently points its new version at
            # (i.e. what the previous rollout deployed).
            old_replica_version_id = current["new_replica_version_id"]

        # Idempotency: if we're already converging to git_revision with progress
        # at or above the target, there's nothing to do.
        if (
            current["new_replica_version_id"] == git_revision
            and current["deployment_progress"] >= target_progress
        ):
            self.log.info(
                "Standard engine already at revision %s with deployment_progress"
                " %s >= target %s.  No proposal needed.",
                git_revision,
                current["deployment_progress"],
                target_progress,
            )
            return {
                "new_replica_version_id": git_revision,
                "old_replica_version_id": old_replica_version_id,
                "deployment_progress": current["deployment_progress"],
                "needs_vote": False,
                "proposal_id": dre.FAKE_PROPOSAL_NUMBER,
            }

        self.log.info(
            "Creating proposal to update the standard engine to revision %s"
            " (from %s) with deployment_progress %s (simulate %s).",
            git_revision,
            old_replica_version_id,
            target_progress,
            self.simulate_proposal,
        )

        authenticated = runner.authenticated()
        proposal_number = (
            authenticated.propose_to_update_standard_engine_replica_version(
                new_replica_version_id=git_revision,
                old_replica_version_id=old_replica_version_id,
                deployment_progress=target_progress,
                dry_run=self.simulate_proposal,
            )
        )

        url = f"{self.network.proposal_display_url}/{proposal_number}"
        return {
            "new_replica_version_id": git_revision,
            "old_replica_version_id": old_replica_version_id,
            "deployment_progress": target_progress,
            "proposal_id": proposal_number,
            "proposal_url": url,
            "needs_vote": True,
        }


class CollectStandardEngineUpgradedSubnets(BaseOperator):
    """
    Return the engine subnet IDs that get upgraded to the new standard engine
    version as `deployment_progress` is raised from the previous step's target
    to this step's target.

    These are the Cloud Engines (following the standard upgrade train) whose
    upgrade priority falls in the range `(previous_progress, deployment_progress]`.
    The returned (flat) list is intended to be used to expand alert-monitoring
    tasks so we only watch the engines that actually changed version in this step.

    The step's own target and the previous step's target are read from the
    `standard_engine_schedule` XCom using this step's index, so this task does
    not need to be dynamically mapped.  Returns an empty list when the step is
    empty (its index is beyond the end of the plan).

    Uses the dre `engine-versions` command.
    """

    template_fields = ("git_revision",)
    network: ic_types.ICNetwork
    git_revision: str
    step_index: int
    schedule_task_id: str

    def __init__(
        self,
        *,
        task_id: str,
        git_revision: str,
        step_index: int,
        network: ic_types.ICNetwork,
        schedule_task_id: str = "standard_engine_schedule",
        **kwargs: Any,
    ):
        self.git_revision = git_revision
        self.step_index = step_index
        self.schedule_task_id = schedule_task_id
        self.network = network
        BaseOperator.__init__(self, task_id=task_id, **kwargs)

    def execute(self, context: Context) -> list[str]:
        _, git_revision = subnet_id_and_git_revision_from_args("", self.git_revision)

        plan = cast(
            "list[dict[str, Any]]",
            context["ti"].xcom_pull(self.schedule_task_id),
        )
        if not plan or self.step_index >= len(plan):
            self.log.info("This standard engine step is empty; no engines to monitor.")
            return []

        to_progress = float(plan[self.step_index]["deployment_progress"])
        from_progress = (
            0.0
            if self.step_index == 0
            else float(plan[self.step_index - 1]["deployment_progress"])
        )

        runner = dre.DRE(network=self.network, subprocess_hook=SubprocessHook())
        subnets = runner.get_engines_in_priority_range(
            from_progress=from_progress,
            to_progress=to_progress,
            new_replica_version_id=git_revision,
        )
        self.log.info(
            "Engines upgraded in range (%s, %s] to revision %s: %s",
            from_progress,
            to_progress,
            git_revision,
            subnets,
        )
        return subnets


def create_api_boundary_nodes_proposal_if_none_exists(
    api_boundary_node_ids: list[str],
    git_revision: str,
    network: ic_types.ICNetwork,
    simulate: bool,
) -> rollout_types.ProposalInfo:
    """
    Creates a proposal for boundary node upgrade if it is necessary.

    Returns the proposal information.

    Intended to be wrapped by an Airflow @task decorator.
    """
    runner = dre.DRE(network=network, subprocess_hook=SubprocessHook())

    # Get proposals sorted by proposal number.
    props = sorted(
        runner.get_ic_os_version_deployment_proposals_for_api_boundary_nodes(
            api_boundary_node_ids=api_boundary_node_ids,
        ),
        key=lambda prop: -prop["proposal_id"],
    )
    props_for_git_revision = [
        p for p in props if p["payload"]["version"] == git_revision
    ]

    if simulate:
        print(f"simulate_proposal={simulate}")

    if not props:
        print("No proposals for the specified boundary nodes.  Will create.")
    elif not props_for_git_revision:
        print(
            f"No proposals with revision {git_revision} for the specified boundary"
            " nodes.  Will create.",
            git_revision,
        )
    elif props_for_git_revision[0]["proposal_id"] < props[0]["proposal_id"]:
        print(
            (
                "Proposal %s with git revision %s for the specified boundary nodes "
                "is not the last (%s).  Will create."
            )
            % (
                props_for_git_revision[0]["proposal_id"],
                git_revision,
                props[0]["proposal_id"],
            )
        )
    elif props_for_git_revision[0]["status"] not in (
        ic_types.ProposalStatus.PROPOSAL_STATUS_OPEN,
        ic_types.ProposalStatus.PROPOSAL_STATUS_ADOPTED,
        ic_types.ProposalStatus.PROPOSAL_STATUS_EXECUTED,
    ):
        print(
            (
                "Proposal %s with git revision %s for the specified boundary nodes "
                "is in state %s and must be created again.  Will create."
            )
            % (
                props_for_git_revision[0]["proposal_id"],
                git_revision,
                props_for_git_revision[0]["status"].name,
            )
        )
    else:
        prop = props_for_git_revision[0]
        print(
            (
                "Proposal %s with git revision %s for the specified boundary nodes "
                "is in state %s and does not need to be created."
            )
            % (
                prop["proposal_id"],
                git_revision,
                prop["status"].name,
            )
        )
        url = f"{network.proposal_display_url}/{prop['proposal_id']}"
        print(
            "Proposal " + url + f" titled {prop['title']}"
            f" has executed.  No need to do anything."
        )
        return {
            "proposal_id": int(prop["proposal_id"]),
            "proposal_url": url,
            "needs_vote": prop["status"]
            == ic_types.ProposalStatus.PROPOSAL_STATUS_OPEN,
        }

    print(
        f"Creating proposal for boundary nodes {api_boundary_node_ids} to "
        + f"adopt revision {git_revision} (simulate {simulate})."
    )

    proposal_number = (
        runner.authenticated().propose_to_update_api_boundary_nodes_version(
            api_boundary_node_ids, git_revision, dry_run=simulate
        )
    )

    url = f"{network.proposal_display_url}/{proposal_number}"
    return {
        "proposal_id": proposal_number,
        "proposal_url": url,
        "needs_vote": True,
    }


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
