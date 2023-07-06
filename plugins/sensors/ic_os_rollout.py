"""
IC-OS rollout sensors.
"""

import datetime
import itertools
from typing import Any, TypedDict, cast

import dfinity.ic_admin as ic_admin
import dfinity.ic_api as ic_api
import dfinity.ic_types as ic_types
import dfinity.prom_api as prom
from dfinity.ic_os_rollout import SLACK_CHANNEL, SLACK_CONNECTION_ID
from operators.ic_os_rollout import RolloutParams

import airflow.providers.slack.operators.slack as slack
from airflow.sensors.base import BaseSensorOperator
from airflow.sensors.date_time import DateTimeSensorAsync
from airflow.triggers.temporal import TimeDeltaTrigger
from airflow.utils.context import Context


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
    def __init__(  # type:ignore
        self,
        *,
        target_time: str | datetime.datetime,
        **kwargs,
    ) -> None:
        """Exists to work around inability to pass target_time as xcom arg."""
        BaseSensorOperator.__init__(self, **kwargs)

        if isinstance(target_time, datetime.datetime):
            self.target_time = target_time.isoformat()
        elif isinstance(target_time, str):
            self.target_time = target_time
        else:
            self.target_time = target_time


class WaitForRevisionToBeElected(ICRolloutSensorBaseOperator):
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
        if self.simulate_elected:
            self.log.info(f"Pretending that {self.git_revision} is elected.")
            return

        self.log.info(f"Waiting for revision {self.git_revision} to be elected.")
        if not ic_admin.is_replica_version_blessed(self.git_revision, self.network):
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

        if not opens and not executeds:
            # No proposal exists.
            if self.simulate_proposal_acceptance:
                # Ah, so this is why the proposal does not exist.
                self.log.info(
                    f"Simulating that the nonexistent proposal to update"
                    f" {self.subnet_id} to {self.git_revision}"
                    f" has been created and accepted."
                )
                return

            for p in props:
                self.log.info(
                    f"Matching proposal not open and not executed:"
                    f" {self.network.proposal_display_url}/{p}"
                )

            raise RuntimeError(
                "No proposal is either open or executed to update"
                f" {self.subnet_id} to revision {self.git_revision}"
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
                f"Simulating that the open proposal to update {self.subnet_id} to"
                f" {self.git_revision} has been created and accepted."
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
    def execute(self, context: Context, event: Any = None) -> None:
        self.log.info(
            f"Waiting for all nodes on subnet ID {self.subnet_id} have "
            + f"adopted revision {self.git_revision}."
        )
        query = (
            "sum(ic_replica_info{"
            + f'ic_subnet="{self.subnet_id}"'
            + "}) by (ic_active_version, ic_subnet)"
        )
        self.log.info(f"Querying Prometheus servers: {query}")
        res = prom.query_prometheus_servers(self.network.prometheus_urls, query)
        if len(res) == 1 and res[0]["metric"]["ic_active_version"] == self.git_revision:
            self.log.info(
                f"All {res[0]['value']} nodes in subnet {self.subnet_id} have"
                f" updated to revision {self.git_revision}"
            )
            return
        if res:
            self.log.info(
                f"Upgrade of {self.subnet_id} to {self.git_revision}"
                " is not complete yet.  From Prometheus:"
            )
            for r in res:
                self.log.info(r)
        else:
            self.log.info(
                f"Upgrade has not begun yet -- Prometheus show no results for git"
                f" revision {self.git_revision} on subnet {self.subnet_id}."
            )
        self.defer(
            trigger=TimeDeltaTrigger(datetime.timedelta(minutes=3)),
            method_name="execute",
        )


class WaitUntilNoAlertsOnSubnet(ICRolloutSensorBaseOperator):
    def execute(self, context: Context, event: Any = None) -> None:
        """
        Wait for 30 minutes of alerts (pending or firing) on the subnet.

        Experimentally we have discovered that when the WaitForReplicaRevisionUpdated
        step has finished, there will be pending alerts on the subnet
        (IC_Replica_Behind) which must resolve themselves.  We look back
        30 minutes to ensure they are resolved.
        """
        self.log.info(f"Waiting for alerts on subnet ID {self.subnet_id} to subside.")
        query = (
            'sum_over_time(ALERTS{ic_subnet="%(subnet_id)s",'
            % ({"subnet_id": self.subnet_id})
            + ' severity="page"}[30m])'
        )
        self.log.info(f"Querying Prometheus servers: {query}")
        res = prom.query_prometheus_servers(self.network.prometheus_urls, query)
        if len(res) > 0:
            self.log.info("There are still Prometheus alerts on the subnet:")
            for r in res:
                self.log.info(r)
            # This value is used in task WaitUntilNoAlertsOnAnySubnet.
            self.xcom_push(
                context=context,
                key="alerts",
                value=SubnetAlertStatus(subnet_id=self.subnet_id, alerts=True),
            )
            self.defer(
                trigger=TimeDeltaTrigger(datetime.timedelta(minutes=1)),
                method_name="execute",
            )
        self.log.info(f"There are no more alerts on subnet ID {self.subnet_id}.")
        self.xcom_push(
            context=context,
            key="alerts",
            value=SubnetAlertStatus(subnet_id=self.subnet_id, alerts=False),
        )


class WaitUntilNoAlertsOnAnySubnet(BaseSensorOperator):
    def __init__(
        self,
        *,
        task_id: str,
        alert_task_id: str,
        **kwargs: Any,
    ):
        BaseSensorOperator.__init__(self, task_id=task_id, **kwargs)
        self.alert_task_id = alert_task_id

    def execute(self, context: Context, event: Any = None) -> None:
        """
        Wait until all concurrently-running wait-for-alerts have reported there
        are no more alerts.
        """
        # This value comes from task WaitUntilNoAlertsOnSubnet.
        known_alerts = cast(
            list[SubnetAlertStatus],
            self.xcom_pull(context, task_ids=[self.alert_task_id], key="alerts"),
        )
        subnets_with_alerts = [r["subnet_id"] for r in known_alerts if r["alerts"]]
        if subnets_with_alerts:
            subnets_text = (
                "subnet " + subnets_with_alerts[0]
                if len(subnets_with_alerts) == 1
                else "subnets " + ", ".join(subnets_with_alerts)
            )
            text = (
                f"Alerts on {subnets_text} keep the rollout stuck.  "
                "Please see the <https://www.notion.so/dfinityorg/Weekly-IC-"
                "OS-release-using-Airflow-1e3c3274ba4d406ebe222aa6eb569e3a#2f"
                "7b92466c554aeea1dc0f535f665ee1|Weekly release runbook> in"
                " Notion for more information."
            )
            self.log.warning(text)
            if not self.xcom_pull(context=context, key="messaged"):
                slack.SlackAPIPostOperator(  # type:ignore
                    channel=SLACK_CHANNEL,
                    username="Airflow",
                    text=text,
                    slack_conn_id=SLACK_CONNECTION_ID,
                    task_id="who_cares",
                ).execute()
                self.xcom_push(context=context, key="messaged", value=True)
            self.defer(
                trigger=TimeDeltaTrigger(datetime.timedelta(minutes=1)),
                method_name="execute",
            )
        self.log.info("There are no alerts on any subnet.  Safe to proceed.")


if __name__ == "__main__":
    import sys

    if sys.argv[1] == "wait_for_replica_revision_updated":
        kn = WaitForReplicaRevisionUpdated(
            task_id="x",
            subnet_id=sys.argv[2],
            git_revision=sys.argv[3],
            network=ic_api.IC_NETWORKS["mainnet"],
        )
        kn.execute({})
