"""
Rollout IC os to subnets in batches.

Each batch runs in parallel.
"""

import datetime
from typing import Any, cast

import operators.ic_os_rollout as ic_os_rollout
import pendulum
import sensors.ic_os_rollout as ic_os_sensor
from dfinity.ic_os_rollout import (
    DEFAULT_GUESTOS_ROLLOUT_PLANS,
    MAX_BATCHES,
    PLAN_FORM,
    SubnetIdWithRevision,
    SubnetRolloutPlanWithRevision,
)
from dfinity.ic_types import IC_NETWORKS

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

DAGS: dict[str, DAG] = {}
for network_name, network in IC_NETWORKS.items():
    with DAG(
        dag_id=f"rollout_ic_os_to_{network_name}_subnets",
        schedule=None,
        start_date=pendulum.datetime(2020, 1, 1, tz="UTC"),
        catchup=False,
        dagrun_timeout=datetime.timedelta(days=14),
        tags=["rollout", "DRE", "IC OS", "GuestOS"],
        render_template_as_native_obj=True,
        params={
            "git_revision": Param(
                "0000000000000000000000000000000000000000",
                type="string",
                pattern="^[a-f0-9]{40}$",
                title="Main Git revision",
                description="Git revision of the IC OS GuestOS release to roll out to "
                "subnets, unless specified otherwise directly for a specific subnet;"
                " the version must have been elected before but the rollout will check",
            ),
            "plan": Param(
                default=DEFAULT_GUESTOS_ROLLOUT_PLANS[network_name].strip(),
                type="string",
                title="Rollout plan",
                description="A YAML-formatted string describing the rollout schedule",
                custom_html_form=PLAN_FORM,
            ),
            "simulate": Param(
                True,
                type="boolean",
                title="Simulate",
                description="If enabled (the default), the update proposal will be"
                " simulated but not created, and its acceptance will be simulated too",
            ),
        },
    ) as dag:
        DAGS[network_name] = dag
        retries = int(86400 / 60 / 5)  # one day worth of retries

        @task
        def revisions(schedule, **context):  # type: ignore
            revs = set()
            for batch in schedule.values():
                for instance in batch[1]:
                    revs.add(instance.git_revision)
            return list(revs)

        def make_me_a_batch(batch_name: str, batch: int) -> None:
            @task
            def collect_batch_subnets(
                current_batch_index: int, **kwargs: Any
            ) -> list[SubnetIdWithRevision]:
                batch = cast(
                    SubnetRolloutPlanWithRevision, kwargs["ti"].xcom_pull("schedule")
                ).get(str(current_batch_index))
                if not batch:
                    print("This batch is empty.")
                    return []
                subnets = batch[1]
                return [
                    {"subnet_id": s.subnet_id, "git_revision": s.git_revision}
                    for s in subnets
                ]

            proceed = collect_batch_subnets(batch)

            join = EmptyOperator(
                task_id="join",
                trigger_rule="none_failed_min_one_success",
            )

            # When proceed returns empty, all other tasks downstream
            # from it, which use the expand() function, skip.
            # But the join task must run unconditionally, else the
            # downstream task (next batch) will be skipped, so we have
            # to add an explicit linkage between proceed and join,
            # such that join will always succeed instead of being skipped
            # and therefore the next batch will run.
            proceed >> join

            (
                ic_os_sensor.CustomDateTimeSensorAsync.partial(
                    task_id="wait_until_start_time",
                    target_time="""{{
                            ti.xcom_pull(task_ids='schedule')["%d"][0] | string
                        }}"""
                    % batch,
                ).expand(_ignored=proceed)
                >> ic_os_sensor.WaitForPreconditions.partial(
                    task_id="wait_for_preconditions",
                    git_revision="{{ params.git_revision }}",
                    retries=retries,
                    network=network,
                ).expand(subnet_id=proceed)
                >> ic_os_rollout.CreateSubnetUpdateProposalIdempotently.partial(
                    task_id="create_proposal_if_none_exists",
                    git_revision="{{ params.git_revision }}",
                    simulate_proposal=cast(bool, "{{ params.simulate }}"),
                    retries=retries,
                    network=network,
                ).expand(subnet_id=proceed)
                >> (
                    ic_os_rollout.RequestProposalVote.partial(
                        task_id="request_proposal_vote",
                        source_task_id=f"batch_{batch_name}.create_proposal_if_none_exists",
                        retries=retries,
                    ).expand(_ignored=proceed),
                    (
                        ic_os_sensor.WaitForProposalAcceptance.partial(
                            task_id="wait_until_proposal_is_accepted",
                            git_revision="{{ params.git_revision }}",
                            simulate_proposal_acceptance=cast(
                                bool, """{{ params.simulate }}"""
                            ),
                            retries=retries,
                            network=network,
                        ).expand(subnet_id=proceed)
                    ),
                )
                >> ic_os_sensor.WaitForReplicaRevisionUpdated.partial(
                    task_id="wait_for_replica_revision",
                    git_revision="{{ params.git_revision }}",
                    retries=retries,
                    network=network,
                    expected_replica_count="""{{
                        ti.xcom_pull(
                            task_ids='batch_%(batch_name)s."""
                    % locals()
                    + """create_proposal_if_none_exists',
                            key='replica_count'
                        ) | int
                    }}""",
                ).expand(subnet_id=proceed)
                >> ic_os_sensor.WaitUntilNoAlertsOnSubnet.partial(
                    task_id="wait_until_no_alerts",
                    git_revision="{{ params.git_revision }}",
                    retries=retries,
                    network=network,
                ).expand(subnet_id=proceed)
                >> join
            )

        sched = ic_os_rollout.schedule(network)
        revs = revisions(sched)
        wait_for_election = ic_os_sensor.WaitForRevisionToBeElected.partial(
            task_id="wait_for_revision_to_be_elected",
            simulate_elected=cast(bool, "{{ params.simulate }}"),
            network=network,
            retries=retries,
        ).expand(git_revision=revs)

        wait_for_other_rollouts = ic_os_sensor.WaitForOtherDAGs(
            task_id="wait_for_other_rollouts"
        )

        upgrade_unassigned_nodes = ic_os_rollout.UpgradeUnassignedNodes(
            task_id="upgrade_unassigned_nodes",
            simulate=cast(bool, "{{ params.simulate }}"),
            network=network,
            retries=retries,
        )

        task_groups = []
        for batch in range(MAX_BATCHES):
            batch_name = str(batch + 1)
            with TaskGroup(group_id=f"batch_{batch_name}") as group:
                make_me_a_batch(batch_name, batch)
                task_groups.append(group)
        chain(
            (
                wait_for_election,
                wait_for_other_rollouts,
            ),
            *task_groups,
            upgrade_unassigned_nodes,
        )


if __name__ == "__main__":
    import os
    import sys

    try:
        rev = sys.argv[1]
    except Exception:
        print(
            "Error: to run this DAG you must specify the revision"
            " as the first argument on the command line, optionally"
            " with the rollout plan in YAML form as second argument.",
            file=sys.stderr,
        )
        sys.exit(os.EX_USAGE)
    try:
        plan = sys.argv[2]
    except Exception:
        plan = DEFAULT_GUESTOS_ROLLOUT_PLANS["mainnet"]
    dag = DAGS["mainnet"]
    dag.test(
        run_conf={
            "git_revision": rev,
            "plan": plan,
        }
    )
