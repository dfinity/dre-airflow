"""
IC-OS rollout sensors.
"""

import datetime
import itertools
from typing import Any

import dfinity.ic_api as ic_api
import dfinity.ic_types as ic_types
import dfinity.prom_api as prom
from operators.ic_os_rollout import RolloutParams

from airflow.sensors.base import BaseSensorOperator
from airflow.sensors.date_time import DateTimeSensorAsync
from airflow.triggers.temporal import TimeDeltaTrigger
from airflow.utils.context import Context


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
                print(
                    f"Simulating that the nonexistent proposal to update"
                    f" {self.subnet_id} to {self.git_revision}"
                    f" has been created and accepted."
                )
                return

            for p in props:
                print(
                    f"Matching proposal not open and not executed:"
                    f" {self.network.proposal_display_url}/{p}"
                )

            raise RuntimeError(
                "No proposal is either open or executed to update"
                f" {self.subnet_id} to revision {self.git_revision}"
            )
        if executeds:
            for p in executeds:
                print(f"Proposal: {p}")
            print(
                f"Proposal"
                f" {self.network.proposal_display_url}/{executeds[0]['proposal_id']}"
                f" titled {executeds[0]['title']}"
                f" has executed.  We can proceed."
            )
            return

        # There is an open proposal, but not yet voted to execution.
        if self.simulate_proposal_acceptance:
            print(
                f"Simulating that the open proposal to update {self.subnet_id} to"
                f" {self.git_revision} has been created and accepted."
            )
            return

        print(
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
        print(
            f"Waiting for all nodes on subnet ID {self.subnet_id} have "
            + f"adopted revision {self.git_revision}."
        )
        query = (
            "sum(ic_replica_info{"
            + f'ic_subnet="{self.subnet_id}"'
            + "}) by (ic_active_version, ic_subnet)"
        )
        print(f"Querying Prometheus servers: {query}")
        res = prom.query_prometheus_servers(self.network.prometheus_urls, query)
        print(res)
        if len(res) == 1 and res[0]["metric"]["ic_active_version"] == self.git_revision:
            print(
                f"All {res[0]['value']} nodes in subnet {self.subnet_id} have"
                f" updated to revision {self.git_revision}"
            )
            return
        if res:
            print(
                f"Upgrade of {self.subnet_id} to {self.git_revision}"
                " is not complete yet.  From Prometheus:"
            )
            for r in res:
                print(r)
        else:
            print(
                f"Upgrade has not begun yet -- Prometheus show no results for git"
                f" revision {self.git_revision} on subnet {self.subnet_id}."
            )
        self.defer(
            trigger=TimeDeltaTrigger(datetime.timedelta(minutes=3)),
            method_name="execute",
        )


class WaitUntilNoAlertsOnSubnet(ICRolloutSensorBaseOperator):
    def execute(self, context: Context, event: Any = None) -> None:
        print(f"Waiting for all alerts on subnet ID {self.subnet_id} to subside.")
        query = (
            'sum_over_time(ALERTS{ic_subnet="%(subnet_id)s", alertstate="firing",'
            % ({"subnet_id": self.subnet_id})
            + ' severity="page"}[30m])'
        )
        print(f"Querying Prometheus servers: {query}")
        res = prom.query_prometheus_servers(self.network.prometheus_urls, query)
        if len(res) > 0:
            print("There are still Prometheus alerts on the subnet:")
            for r in res:
                print(r)
            self.defer(
                trigger=TimeDeltaTrigger(datetime.timedelta(minutes=1)),
                method_name="execute",
            )
        print(f"There are no more alerts on subnet ID {self.subnet_id}.")


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
    elif sys.argv[1] == "wait_until_no_alerts_on_subnet":
        km = WaitUntilNoAlertsOnSubnet(
            task_id="x",
            subnet_id=sys.argv[2],
            git_revision=sys.argv[3],
            network=ic_api.IC_NETWORKS["mainnet"],
        )
        km.execute({})
