"""
IC-OS rollout sensors.
"""

import datetime
import itertools
import time
from typing import Any, Sequence, TypedDict, cast

import dfinity.dre as dre
import dfinity.ic_types as ic_types
import dfinity.prom_api as prom
import dfinity.rollout_types as rollout_types
from dfinity.ic_os_rollout import (
    SLACK_CHANNEL,
    SLACK_CONNECTION_ID,
    subnet_id_and_git_revision_from_args,
)
from operators.ic_os_rollout import NotifyAboutStalledSubnet, RolloutParams

import airflow.models.taskinstance
import airflow.providers.slack.operators.slack as slack
from airflow.hooks.subprocess import SubprocessHook
from airflow.models.dagrun import DagRun
from airflow.sensors.base import BaseSensorOperator
from airflow.sensors.date_time import DateTimeSensorAsync
from airflow.serialization.pydantic.dag_run import DagRunPydantic
from airflow.triggers.temporal import TimeDeltaTrigger
from airflow.utils.context import Context
from airflow.utils.state import DagRunState

SUBNET_UPDATE_STALL_TIMEOUT_SECONDS = 3600  # One hour between messages.


class SubnetAlertStatus(TypedDict):
    subnet_id: str
    alerts: bool


class ICRolloutSensorBaseOperator(RolloutParams, BaseSensorOperator):
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
        BaseSensorOperator.__init__(self, task_id=task_id, **kwargs)


class CustomDateTimeSensorAsync(DateTimeSensorAsync):
    template_fields: Sequence[str] = list(DateTimeSensorAsync.template_fields) + [
        "simulate"
    ]

    def __init__(  # type:ignore
        self,
        *,
        target_time: str | datetime.datetime,
        simulate: str | bool,
        _ignored=None,
        **kwargs,
    ) -> None:
        """Exists to work around inability to pass target_time as xcom arg."""
        self.simulate = simulate
        BaseSensorOperator.__init__(self, **kwargs)

        if isinstance(target_time, datetime.datetime):
            self.target_time = target_time.isoformat()
        elif isinstance(target_time, str):
            self.target_time = target_time
        else:
            self.target_time = target_time

    def execute(self, context: Context) -> None:
        if self.simulate:
            print("Nominally we would sleep, but this is a simulation.  Returning now.")
            return
        super().execute(context=context)


class WaitForRevisionToBeElected(ICRolloutSensorBaseOperator):
    template_fields = tuple(
        itertools.chain.from_iterable(
            (
                ICRolloutSensorBaseOperator.template_fields,
                ("simulate_elected",),
            )
        )
    )
    simulate_elected: bool

    def __init__(
        self,
        *,
        task_id: str,
        git_revision: str,
        simulate_elected: bool,
        network: ic_types.ICNetwork,
        **kwargs: Any,
    ):
        ICRolloutSensorBaseOperator.__init__(
            self,
            task_id=task_id,
            subnet_id="0" * 40,  # This is unnecessary here.
            git_revision=git_revision,
            network=network,
            **kwargs,
        )
        self.simulate_elected = simulate_elected

    def execute(self, context: Context, event: Any = None) -> None:
        _, git_revision = subnet_id_and_git_revision_from_args("", self.git_revision)

        if self.simulate_elected:
            self.log.info(
                f"Pretending that {git_revision} is elected"
                f" (simulate_elected={self.simulate_elected})."
            )
            return

        self.log.info(f"Waiting for revision {git_revision} to be elected.")
        blessed = dre.DRE(self.network, SubprocessHook()).is_replica_version_blessed(
            git_revision,
        )
        if not blessed:
            self.log.info("Revision is not yet elected.  Waiting.")
            self.defer(
                trigger=TimeDeltaTrigger(datetime.timedelta(minutes=15)),
                method_name="execute",
            )
        self.log.info("Revision is elected.  We can proceed.")


class WaitForProposalAcceptance(ICRolloutSensorBaseOperator):
    template_fields = tuple(
        itertools.chain.from_iterable(
            (
                ICRolloutSensorBaseOperator.template_fields,
                ("simulate_proposal_acceptance",),
            )
        )
    )
    simulate_proposal_acceptance: bool

    def __init__(
        self,
        *,
        task_id: str,
        subnet_id: str,
        git_revision: str,
        simulate_proposal_acceptance: bool,
        network: ic_types.ICNetwork,
        **kwargs: Any,
    ):
        """
        Initializes the waiter.

        Args:
        * simulate_proposal: if enabled, elide the check of whether the proposal
          has been approved or not, and pretend it has been.
        """
        ICRolloutSensorBaseOperator.__init__(
            self,
            task_id=task_id,
            subnet_id=subnet_id,
            git_revision=git_revision,
            network=network,
            **kwargs,
        )
        self.simulate_proposal_acceptance = simulate_proposal_acceptance

    def execute(self, context: Context, event: Any = None) -> None:
        subnet_id, git_revision = subnet_id_and_git_revision_from_args(
            self.subnet_id, self.git_revision
        )

        # https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/logging-tasks.html#grouping-of-log-lines
        props = dre.DRE(
            network=self.network, subprocess_hook=SubprocessHook()
        ).get_ic_os_version_deployment_proposals_for_subnet_and_revision(
            subnet_id=subnet_id,
            git_revision=git_revision,
        )

        def per_status(
            props: list[ic_types.AbbrevSubnetUpdateProposal],
            status: ic_types.ProposalStatus,
        ) -> list[ic_types.AbbrevSubnetUpdateProposal]:
            return [p for p in props if p["status"] == status]

        executeds = per_status(props, ic_types.ProposalStatus.PROPOSAL_STATUS_EXECUTED)
        opens = per_status(props, ic_types.ProposalStatus.PROPOSAL_STATUS_OPEN)

        if not opens and not executeds:
            # No proposal exists.
            if self.simulate_proposal_acceptance:
                # Ah, so this is why the proposal does not exist.
                self.log.info(
                    f"Simulating that the nonexistent proposal to update"
                    f" {subnet_id} to {git_revision}"
                    f" has been created and accepted."
                    f" (simulate_acceptance={self.simulate_proposal_acceptance})"
                )
                return

            for p in props:
                self.log.info(
                    f"Matching proposal not open and not executed:"
                    f" {self.network.proposal_display_url}/{p}"
                )

            self.log.info(
                "No proposal is either open or executed to update"
                f" {subnet_id} to revision {git_revision}."
                "  Waiting one minute until a proposal appears executed."
            )
            self.defer(
                trigger=TimeDeltaTrigger(datetime.timedelta(minutes=1)),
                method_name="execute",
            )

        if executeds:
            for p in executeds:
                self.log.info(f"Proposal: {p}")
            self.log.info(
                f"Proposal"
                f" {self.network.proposal_display_url}/{executeds[0]['proposal_id']}"
                f" titled {executeds[0]['title']}"
                f" has executed.  We can proceed."
            )
            return

        # There is an open proposal, but not yet voted to execution.
        if self.simulate_proposal_acceptance:
            self.log.info(
                f"Simulating that the open proposal to update {subnet_id} to"
                f" {git_revision} has been created and accepted."
            )
            return

        self.log.info(
            f"Proposal"
            f" {self.network.proposal_display_url}/{opens[0]['proposal_id']}"
            f" titled {opens[0]['title']}"
            f" is open.  Waiting five more minutes until it has executed."
        )
        self.defer(
            trigger=TimeDeltaTrigger(datetime.timedelta(minutes=5)),
            method_name="execute",
        )


class WaitForReplicaRevisionUpdated(ICRolloutSensorBaseOperator):
    template_fields = tuple(
        itertools.chain.from_iterable(
            (
                ICRolloutSensorBaseOperator.template_fields,
                ("expected_replica_count",),
            )
        )
    )

    def __init__(
        self,
        *,
        task_id: str,
        subnet_id: str,
        git_revision: str,
        network: ic_types.ICNetwork,
        expected_replica_count: int,
        **kwargs: Any,
    ):
        """
        Initializes the waiter.
        """
        ICRolloutSensorBaseOperator.__init__(
            self,
            task_id=task_id,
            subnet_id=subnet_id,
            git_revision=git_revision,
            network=network,
            **kwargs,
        )
        self.expected_replica_count = expected_replica_count

    def execute(self, context: Context, event: Any = None) -> None:
        subnet_id, git_revision = subnet_id_and_git_revision_from_args(
            self.subnet_id, self.git_revision
        )

        self.log.info(
            f"Waiting for all nodes on subnet ID {subnet_id} have "
            + f"adopted revision {git_revision}."
        )

        query = (
            "sum(ic_replica_info{"
            + f'ic_subnet="{subnet_id}"'
            + "}) by (ic_active_version, ic_subnet)"
        )
        print("::group::Querying Prometheus servers")
        self.log.info(query)
        print("::endgroup::")
        res = prom.query_prometheus_servers(self.network.prometheus_urls, query)
        if len(res) == 1 and res[0]["metric"]["ic_active_version"] == git_revision:
            current_replica_count = int(res[0]["value"])
            if current_replica_count >= self.expected_replica_count:
                self.log.info(
                    "All %s nodes in subnet %s have updated to revision %s.",
                    current_replica_count,
                    subnet_id,
                    git_revision,
                )
                return
            else:
                self.log.warn(
                    "The replica count of subnet %s is %d but %d is expected; waiting.",
                    subnet_id,
                    current_replica_count,
                    self.expected_replica_count,
                )
        if res:
            self.log.info(
                f"Upgrade of {subnet_id} to {git_revision}"
                " is not complete yet.  From Prometheus:"
            )
            for r in res:
                self.log.info(r)
        else:
            self.log.info(
                f"Upgrade has not begun yet -- Prometheus show no results for git"
                f" revision {git_revision} on subnet {subnet_id}."
            )
        self.defer(
            trigger=TimeDeltaTrigger(datetime.timedelta(minutes=3)),
            method_name="execute",
        )


class WaitUntilNoAlertsOnSubnet(ICRolloutSensorBaseOperator):
    def execute(self, context: Context, event: Any = None) -> None:
        """
        Wait for 15 minutes of alerts (pending or firing) on the subnet.

        Experimentally we have discovered that when the WaitForReplicaRevisionUpdated
        step has finished, there will be pending alerts on the subnet
        (IC_Replica_Behind) which must resolve themselves.  We look back
        15 minutes to ensure they are resolved.
        """

        def send_notification_if_necessary(subnet_id: str) -> None:
            # Small bit of code to reuse an Airflow operator that sends
            # a message to Slack notifying the DRE operator that a subnet
            # has not exited the alerts condition in over an hour.
            now = time.time()
            key = "alert_check_timestamp"
            task_id = context["task_instance"].task_id
            self.log.info(
                "Pulling alert check timestamp from xcom for %s %s %s",
                key,
                task_id,
                context["task_instance"].map_index,
            )
            alert_check_timestamp = context["task_instance"].xcom_pull(
                key=key,
                task_ids=task_id,
                map_indexes=context["task_instance"].map_index,
            )
            self.log.info(
                "Here is the current alert check timestamp: %r", alert_check_timestamp
            )
            if not alert_check_timestamp:
                # Value is not yet xcommed.  Xcom it now.
                deadline = now + SUBNET_UPDATE_STALL_TIMEOUT_SECONDS
                self.log.info(
                    "Notification deadline not initialized, storing %s", deadline
                )
                context["task_instance"].xcom_push(key=key, value=deadline)
            else:
                deadline = float(alert_check_timestamp)
                if now > deadline:
                    # Value is xcommed and is old enough.
                    deadline = now + SUBNET_UPDATE_STALL_TIMEOUT_SECONDS
                    self.log.info(
                        "Notification deadline has been hit, notifying"
                        " and resetting deadline to %s",
                        deadline,
                    )
                    # Send message here.
                    NotifyAboutStalledSubnet(
                        task_id="notify_about_stalled_subnet",
                        subnet_id=subnet_id,
                    ).execute(context=context)
                    # Remember new deadline.
                    context["task_instance"].xcom_push(
                        key=key,
                        value=deadline,
                    )

        subnet_id, git_revision = subnet_id_and_git_revision_from_args(
            self.subnet_id, self.git_revision
        )

        self.log.info(f"Waiting for alerts on subnet ID {subnet_id} to subside.")
        query = """
            sum_over_time(
                ALERTS{
                    ic_subnet="%(subnet_id)s",
                    alertname!="PrometheusTargetMissing",
                    alertname!="IC_Subnet_HttpRequest_ResponseTooSlow",
                    severity="page"
                }[15m]
            )""" % (
            {
                "subnet_id": subnet_id,
            }
        )
        print("::group::Querying Prometheus servers")
        self.log.info(query)
        print("::endgroup::")
        res = prom.query_prometheus_servers(self.network.prometheus_urls, query)
        if len(res) > 0:
            self.log.info("There are still Prometheus alerts on the subnet:")
            for r in res:
                self.log.info(r)
            # This value is used in task WaitUntilNoAlertsOnAnySubnet.
            self.xcom_push(
                context=context,
                key="alerts",
                value=SubnetAlertStatus(subnet_id=subnet_id, alerts=True),
            )
            send_notification_if_necessary(subnet_id)
            self.defer(
                trigger=TimeDeltaTrigger(datetime.timedelta(minutes=1)),
                method_name="execute",
            )
        self.log.info(f"There are no more alerts on subnet ID {subnet_id}.")
        self.xcom_push(
            context=context,
            key="alerts",
            value=SubnetAlertStatus(subnet_id=subnet_id, alerts=False),
        )


class WaitUntilNoAlertsOnAnySubnet(ICRolloutSensorBaseOperator):
    alert_task_id: str

    def __init__(
        self,
        *,
        task_id: str,
        subnet_id: str,
        git_revision: str,
        network: ic_types.ICNetwork,
        alert_task_id: str,
        **kwargs: Any,
    ):
        """
        Initializes the waiter.
        """
        ICRolloutSensorBaseOperator.__init__(
            self,
            task_id=task_id,
            subnet_id=subnet_id,
            git_revision=git_revision,
            network=network,
            **kwargs,
        )
        self.alert_task_id = alert_task_id

    def execute(self, context: Context, event: Any = None) -> None:
        """
        Wait until all concurrently-running wait-for-alerts have reported there
        are no more alerts.
        """
        subnet_id, _ = subnet_id_and_git_revision_from_args(
            self.subnet_id, self.git_revision
        )

        # This value comes from task WaitUntilNoAlertsOnSubnet.
        known_alerts = cast(
            list[SubnetAlertStatus],
            self.xcom_pull(context, task_ids=[self.alert_task_id], key="alerts"),
        )
        ti = cast(airflow.models.taskinstance.TaskInstance, context["ti"])
        dag_id = ti.dag_id
        dag_run = ti.dag_run
        url = f"https://airflow.ch1-rel1.dfinity.network/dags/{dag_id}/grid?dag_run_id={dag_run}"

        def post(text: str) -> None:
            slack.SlackAPIPostOperator(  # type:ignore
                channel=SLACK_CHANNEL,
                username="Airflow",
                text=text,
                slack_conn_id=SLACK_CONNECTION_ID,
                task_id="who_cares",
            ).execute()

        def messaged() -> bool:
            return bool(self.xcom_pull(context=context, key="messaged"))

        def remember_messaging() -> None:
            self.xcom_push(context=context, key="messaged", value=True)

        subnets_with_alerts = [r["subnet_id"] for r in known_alerts if r["alerts"]]
        if subnets_with_alerts:
            subnets_text = (
                "subnet " + subnets_with_alerts[0]
                if len(subnets_with_alerts) == 1
                else "subnets " + ", ".join(subnets_with_alerts)
            )
            text = (
                f"While rolling out {subnet_id}, alerts on {subnets_text} keep"
                f" <{url}|the rollout> stuck.  Please see the"
                " <https://www.notion.so/dfinityorg/Weekly-IC-"
                "OS-release-using-Airflow-1e3c3274ba4d406ebe222aa6eb569e3a#2f"
                "7b92466c554aeea1dc0f535f665ee1|Weekly release runbook> in"
                " Notion for more information."
            )
            self.log.warning(text)
            if not messaged():
                post(text)
                remember_messaging()
            self.defer(
                trigger=TimeDeltaTrigger(datetime.timedelta(minutes=1)),
                method_name="execute",
            )
        self.log.info("There are no alerts on any subnet.  Safe to proceed.")
        if messaged():
            post(f"Alerts have subsided.  Rollout of {subnet_id} can proceed.")


class WaitForOtherDAGs(BaseSensorOperator):
    source_dag_id: str | None

    def __init__(
        self,
        *,
        task_id: str,
        source_dag_id: str | None = None,
        **kwargs: Any,
    ):
        """
        Initializes the waiter for other rollouts.

        Optional parameters:
          source_dag_id: the ID of the DAG that must be waited upon; if None,
                         the used ID will be the same DAG ID of the DAG
                         executing this task.
        """
        BaseSensorOperator.__init__(
            self,
            task_id=task_id,
            **kwargs,
        )
        self.source_dag_id = source_dag_id

    def execute(self, context: Context, event: Any = None) -> None:
        """
        Wait until all concurrently-running wait-for-alerts have reported there
        are no more alerts.
        """
        # Take all DAG runs...
        source_dag_id = self.source_dag_id or context["dag_run"].dag_id
        dag_runs = DagRun.find(dag_id=source_dag_id)
        # ...include only running / queued and DAG runs that aren't us.
        dag_runs = [
            d
            for d in dag_runs
            if d.state
            in [
                DagRunState.QUEUED,
                DagRunState.RUNNING,
            ]
            and d.run_id != context["dag_run"].run_id
        ]
        if dag_runs:
            self.log.info(
                "There are %d other DAGs named %s queued or running.",
                len(dag_runs),
                source_dag_id,
            )

        # Exclude dags started the same week as us.
        # (Using weekday that begins on a Sunday since the automatic rollout
        # (computation that dispatches the rollout happens on Sunday.)
        # We use data_interval_end for comparison, which is the date at which
        # each DAG run was started.
        def week(x: DagRun | DagRunPydantic) -> int:
            data_interval_end = x.data_interval_end
            assert isinstance(data_interval_end, datetime.datetime), (
                data_interval_end,
                type(data_interval_end),
            )
            return data_interval_end.isocalendar().week

        my_week = week(context["dag_run"])
        for other_run in dag_runs[:]:
            other_week = week(other_run)
            if my_week == other_week:
                self.log.info(
                    "Ignoring %s as it was started the same week as us (%s == %s)",
                    other_run.run_id,
                    my_week,
                    other_week,
                )
                dag_runs.remove(other_run)

        if dag_runs:
            interval = 3
            self.log.info("Waiting %s minutes for other DAGs to complete:", interval)
            for other_run in dag_runs:
                self.log.info(
                    "* %s is %s: logical date %s data interval start %s data"
                    " interval end %s",
                    other_run.run_id,
                    other_run.state,
                    other_run.logical_date,
                    other_run.data_interval_start,
                    other_run.data_interval_end,
                )
            self.log.info(
                "If you still want to proceed, mark this task as successful"
                " or fail the other DAGs."
            )
            self.defer(
                trigger=TimeDeltaTrigger(datetime.timedelta(minutes=interval)),
                method_name="execute",
            )
        self.log.info("No other DAGs are running.  Proceeding.")


class WaitForPreconditions(ICRolloutSensorBaseOperator):
    """Performs a variety of checks.

    Current checks:

    * https://dfinity.atlassian.net/browse/REL-2675 delays updates of the
      signing subnet and its backup within less than 1 day of each other.
    """

    def execute(self, context: Context, event: Any = None) -> None:
        subnet_id, _ = subnet_id_and_git_revision_from_args(
            self.subnet_id, self.git_revision
        )

        antipodes = {
            "uzr34-akd3s-xrdag-3ql62-ocgoh-ld2ao-tamcv-54e7j-krwgb-2gm4z-oqe": "pzp6e"
            "-ekpqk-3c5x7-2h6so-njoeq-mt45d-h3h6c-q3mxf-vpeq5-fk5o7-yae",
            "pzp6e-ekpqk-3c5x7-2h6so-njoeq-mt45d-h3h6c-q3mxf-vpeq5-fk5o7-yae": "uzr34"
            "-akd3s-xrdag-3ql62-ocgoh-ld2ao-tamcv-54e7j-krwgb-2gm4z-oqe",
        }

        if subnet_id in antipodes:
            other = antipodes[subnet_id]

            self.log.info(
                f"Checking that {other} has not been updated in the"
                f" last 1 day before {subnet_id}"
            )
            # Tolerate up to 1/3 of the subnet being updated,
            # e.g. if a node is joining a subnet.
            query = f"""
(
    sum(changes((ic_replica_info{{ic_subnet="{other}"}})[1d]) or vector(0))
    >=
    scalar(job_ic_icsubnet:up:count{{job="replica", ic_subnet="{other}"}} / 3)
) or 0
"""
            print("::group::Querying Prometheus servers")
            self.log.info(query)
            print("::endgroup::")
            res = prom.query_prometheus_servers(self.network.prometheus_urls, query)
            if not res:
                raise RuntimeError(("Prometheus returned no sum of updates: %r" % res,))

            update_sum = int(res[0]["value"])
            if update_sum > 0:
                self.log.info(
                    f"{other} was updated too recently.  Waiting to protect"
                    " the integrity of signing key or its backup."
                )
                self.defer(
                    trigger=TimeDeltaTrigger(datetime.timedelta(minutes=3)),
                    method_name="execute",
                )

            self.log.info(f"It is now safe to continue with the update of {subnet_id}.")


def has_proposal_executed(
    proposal_info: rollout_types.ProposalInfo,
    network: ic_types.ICNetwork,
    simulate: bool,
) -> bool:
    """
    Waits until a proposal matching the input parameters has been adopted.

    Returns PokeReturnValue(is_done=bool) indicating whether that has happened or not.

    Intended to be wrapped by an Airflow @task.sensor decorator.
    """
    props = dre.DRE(network=network, subprocess_hook=SubprocessHook()).get_proposals()

    props = [p for p in props if p["proposal_id"] == proposal_info["proposal_id"]]

    def per_status(
        props: list[ic_types.AbbrevProposal],
        status: ic_types.ProposalStatus,
    ) -> list[ic_types.AbbrevProposal]:
        return [p for p in props if p["status"] == status]

    executeds = per_status(props, ic_types.ProposalStatus.PROPOSAL_STATUS_EXECUTED)
    opens = per_status(props, ic_types.ProposalStatus.PROPOSAL_STATUS_OPEN)

    if not opens and not executeds:
        # No proposal exists.
        if simulate:
            # Ah, so this is why the proposal does not exist.
            print(
                "Simulating that the nonexistent proposal"
                " has been created and accepted."
                f" (simulate={simulate})"
            )
            return True

        for p in props:
            print(
                "Matching proposal not open and not executed:"
                f" {network.proposal_display_url}/{p}"
            )

        print(
            "No matching proposal is either open or executed."
            "  Waiting one minute until a proposal appears executed."
        )
        return False

    if executeds:
        for p in executeds:
            print(f"Proposal: {p}")
        print(
            "Proposal"
            f" {network.proposal_display_url}/{executeds[0]['proposal_id']}"
            f" titled {executeds[0]['title']}"
            " has executed.  We can proceed."
        )
        return True

    # There is an open proposal, but not yet voted to execution.
    if simulate:
        print("Simulating that the open proposal has been created and accepted.")
        return True

    print(
        "Proposal"
        f" {network.proposal_display_url}/{opens[0]['proposal_id']}"
        f" titled {opens[0]['title']}"
        " is still open.  Waiting until it has executed."
    )
    return False


def have_api_boundary_nodes_adopted_revision(
    api_boundary_node_ids: list[str],
    git_revision: str,
    network: ic_types.ICNetwork,
) -> bool:
    print(
        f"Waiting for specified boundary nodes to have adopted revision {git_revision}."
    )

    joined = "|".join(api_boundary_node_ids)
    query = (
        "sum(ic_orchestrator_info{"
        + f'ic_node=~"{joined}"'
        + "}) by (ic_active_version)"
    )
    print("::group::Querying Prometheus servers")
    print(query)
    print("::endgroup::")
    res = prom.query_prometheus_servers(network.prometheus_urls, query)
    if len(res) == 1 and res[0]["metric"]["ic_active_version"] == git_revision:
        current_replica_count = int(res[0]["value"])
        if current_replica_count >= len(api_boundary_node_ids):
            print(
                "All %s boundary nodes have updated to revision %s."
                % (
                    current_replica_count,
                    git_revision,
                )
            )
            return True
        else:
            print(
                "The updated boundary node count is %d but %d is expected; waiting."
                % (
                    current_replica_count,
                    len(api_boundary_node_ids),
                )
            )
    if res:
        print(
            f"Upgrade of boundary nodes to {git_revision}"
            " is not complete yet.  From Prometheus:"
        )
        for r in res:
            print(r)
    else:
        print(
            f"Upgrade has not begun yet -- Prometheus show no results for the"
            f" specified boundary nodes {api_boundary_node_ids}."
        )
    return False


def have_api_boundary_nodes_stopped_alerting(
    api_boundary_node_ids: list[str],
    network: ic_types.ICNetwork,
) -> bool:
    """
    Check for 15 minutes of no alerts (pending or firing) on any of the BNs.

    Return True if no alerts, False otherwise.
    """
    joineds = "|".join(api_boundary_node_ids)

    print("Waiting for alerts on API boundary nodes to subside.")
    query = """
        sum_over_time(
            ALERTS{
                ic_node=~"%(joineds)s",
                severity=~"page|notify"
            }[15m]
        )""" % (
        {
            "joineds": joineds,
        }
    )
    print("::group::Querying Prometheus servers")
    print(query)
    print("::endgroup::")
    res = prom.query_prometheus_servers(network.prometheus_urls, query)
    if len(res) > 0:
        print("There are still Prometheus alerts on the subnet:")
        for r in res:
            print(r)
        return False
    print("There are no more alerts on boundary nodes.")
    return True


if __name__ == "__main__":
    import sys

    if sys.argv[1] == "wait_for_replica_revision_updated":
        kn = WaitForReplicaRevisionUpdated(
            task_id="x",
            subnet_id=sys.argv[2],
            git_revision=sys.argv[3],
            network=ic_types.IC_NETWORKS["mainnet"],
            expected_replica_count=13,
        )
        kn.execute({})
