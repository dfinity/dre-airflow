"""
Rollout IC os to subnets in batches.

Each batch runs in parallel.
"""

import datetime
import os
import sys
from typing import Any, cast

import operators.ic_os_rollout as ic_os_rollout
import pendulum
import sensors.ic_os_rollout as ic_os_sensor
from airflow import DAG, __version__
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from dfinity.ic_os_rollout import (
    MAX_BATCHES,
    SubnetIdWithRevision,
    SubnetRolloutPlanWithRevision,
)
from dfinity.ic_types import IC_NETWORKS

# Temporarily add the DAGs folder to import defaults.py.
sys.path.append(os.path.dirname(__file__))
try:
    from defaults import DEFAULT_GUESTOS_ROLLOUT_PLANS as DEFAULT_ROLLOUT_PLANS
    from defaults import (
        DEFAULT_STANDARD_ENGINE_ROLLOUT_PLANS as DEFAULT_STANDARD_ENGINE_PLANS,
    )
finally:
    sys.path.pop()

# The number of standard engine deployment_progress steps to materialize as
# Airflow tasks.  The plan may specify fewer steps; extra tasks skip.
MAX_STANDARD_ENGINE_STEPS: int = 10

# The subnet batch (1-indexed) after which the standard engine deployment_progress
# increments are allowed to begin.  We wait for the first batch of subnets to be
# rolled out (and thus voted in) before touching the standard engine version.
STANDARD_ENGINE_START_AFTER_BATCH: int = 1

if "2.9" in __version__:
    # To be deleted when we upgrade to Airflow 2.11.
    from dfinity.ic_os_rollout import PLAN_FORM

    format = dict(custom_html_form=PLAN_FORM)
else:
    format = {"format": "multiline"}

ROLLOUT_PLAN_HELP = """\
A specification of what subnets to rollout, when, and with which versions.

Remarks:
* All times are expressed in the UTC time zone.
* Days refer to dates relative to your current work week
  if starting a rollout during a workday, or next week if
  the rollout is started during a weekend.
* A day name with " next week" added at the end means
  "add one week to this day".
* Each date/time can specify a simple list of subnets,
  or can specify a dict with two keys:
  * batch: an optional integer 1-30 with the batch number
           you want to assign to this batch.
  * subnets: a list of subnets.
* A subnet may be specified:
  * as an integer number from 0 to the maximum subnet number,
  * as a full or abbreviated subnet principal ID,
  * as a dictionary of {
       subnet: ID or principal
       git_revision: revision to deploy to this subnet
    }
    with this form being able to override the Git revision
    that will be targeted to that specific subnet.
    Example of a batch specified this way:
      Monday next week:
        7:00:
          batch: 30
          subnets:
          - subnet: tdb26
            git_revision: 0123456789012345678901234567890123456789
"""


STANDARD_ENGINE_PLAN_HELP = """\
A specification of how the standard engine replica version is upgraded through
the week, alongside the subnet rollout.

Cloud Engine subnets that follow the standard upgrade train converge towards the
main Git revision of this rollout.  This plan controls how fast: it maps each
day/time to the target `deployment_progress` (a fraction between 0.0 and 1.0)
that the standard engine record should reach at that time.  Roughly that fraction
of the engines will be running the new version after the corresponding proposal.

Remarks:
* All times are expressed in the UTC time zone.
* Day names follow the same conventions as the subnet rollout plan, including
  the " next week" suffix.
* The new version is always this rollout's main Git revision.  The old version
  is whatever the current standard engine record's new version is (i.e. what the
  previous rollout deployed), so it does not need to be specified here.
* The target `deployment_progress` must strictly increase over time, and the
  last step must reach 1.0 so the deployment completes.
* The increments only begin after the first batch of subnets has been rolled
  out, so the version has been exercised on regular subnets first.
* Leave this empty to skip standard engine management entirely.

Example:
  Monday:
    15:00:     0.1
  Tuesday:
    15:00:     0.5
  Wednesday:
    15:00:     0.8
  Thursday:
    15:00:     1.0
"""


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
                "API boundary nodes;"
                " the version must have been elected before but the rollout will"
                " check for that.",
            ),
            "plan": Param(
                default=DEFAULT_ROLLOUT_PLANS[network_name].strip(),
                type="string",
                title="Rollout plan",
                description_md=ROLLOUT_PLAN_HELP,
                **format,
            ),
            "standard_engine_plan": Param(
                default=DEFAULT_STANDARD_ENGINE_PLANS.get(network_name, "").strip(),
                type="string",
                title="Standard engine rollout plan",
                description_md=STANDARD_ENGINE_PLAN_HELP,
                **format,
            ),
            "simulate": Param(
                True,
                type="boolean",
                title="Simulate",
                description="If enabled (the default), the update proposal will be"
                " simulated but not created, and its acceptance will be simulated too.",
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
                    simulate="{{ params.simulate }}",
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

        def make_standard_engine_step(step_index: int) -> None:
            # When the plan has fewer steps than MAX_STANDARD_ENGINE_STEPS, the
            # expand() over an out-of-range index yields nothing and every task
            # downstream skips.  The join task always runs so the next step (and
            # thus the whole chain) is not skipped.
            @task
            def collect_step(step_index: int, **kwargs: Any) -> list[float]:
                plan = cast(
                    "list[dict[str, Any]]",
                    kwargs["ti"].xcom_pull("standard_engine_schedule"),
                )
                if not plan or step_index >= len(plan):
                    print("This standard engine step is empty.")
                    return []
                return [plan[step_index]["deployment_progress"]]

            proceed = collect_step(step_index)

            join = EmptyOperator(
                task_id="join",
                trigger_rule="none_failed_min_one_success",
            )
            proceed >> join

            # Collect the engines upgraded in this step so we can monitor their
            # alerts.  This runs after the proposal so the version has been
            # elected/voted and the engines have begun upgrading.  It is not
            # mapped: it reads its own and the previous step's target progress
            # from the schedule XCom, and returns a flat list of subnet IDs.  It
            # runs unconditionally (like join) so an empty step still lets the
            # alert wait (and the chain) proceed.
            upgraded_engines = ic_os_rollout.CollectStandardEngineUpgradedSubnets(
                task_id="collect_upgraded_engines",
                git_revision="{{ params.git_revision }}",
                step_index=step_index,
                retries=retries,
                network=network,
                trigger_rule="none_failed_min_one_success",
            )

            (
                ic_os_sensor.CustomDateTimeSensorAsync.partial(
                    task_id="wait_until_start_time",
                    target_time="""{{
                            ti.xcom_pull(
                                task_ids='standard_engine_schedule'
                            )[%d]["start_at"] | string
                        }}"""
                    % step_index,
                    simulate="{{ params.simulate }}",
                ).expand(_ignored=proceed)
                >> ic_os_rollout.CreateStandardEngineProposalIdempotently.partial(
                    task_id="create_proposal_if_none_exists",
                    git_revision="{{ params.git_revision }}",
                    simulate_proposal=cast(bool, "{{ params.simulate }}"),
                    retries=retries,
                    network=network,
                ).expand(deployment_progress=proceed)
                >> ic_os_rollout.RequestProposalVote.partial(
                    task_id="request_proposal_vote",
                    source_task_id="standard_engine.step_%d.create_proposal_if_none_exists"
                    % step_index,
                    retries=retries,
                ).expand(_ignored=proceed)
                >> upgraded_engines
                >> ic_os_sensor.WaitUntilNoAlertsOnSubnet.partial(
                    task_id="wait_until_no_alerts",
                    git_revision="{{ params.git_revision }}",
                    retries=retries,
                    network=network,
                ).expand(subnet_id=upgraded_engines.output)
                >> join
            )

        sched = ic_os_rollout.schedule(network)
        standard_engine_sched = ic_os_rollout.standard_engine_schedule()
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

        # Standard engine deployment_progress increments run in parallel with the
        # subnet batches, but only start once the first batch of subnets has been
        # rolled out (and thus the version voted in).  The steps run sequentially
        # among themselves because deployment_progress must strictly increase and
        # the registry only allows one transition to be in flight at a time.
        with TaskGroup(group_id="standard_engine") as standard_engine_group:
            step_groups = []
            for step_index in range(MAX_STANDARD_ENGINE_STEPS):
                with TaskGroup(group_id=f"step_{step_index}") as step_group:
                    make_standard_engine_step(step_index)
                    step_groups.append(step_group)
            chain(*step_groups)

        # standard_engine_schedule must run before any step can read it.
        standard_engine_sched >> standard_engine_group
        # Do not touch the standard engine version until the first batch of
        # subnets has finished rolling out.
        task_groups[STANDARD_ENGINE_START_AFTER_BATCH - 1] >> standard_engine_group


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
        plan = DEFAULT_ROLLOUT_PLANS["mainnet"]
    try:
        standard_engine_plan = sys.argv[3]
    except Exception:
        standard_engine_plan = DEFAULT_STANDARD_ENGINE_PLANS["mainnet"]
    dag = DAGS["mainnet"]
    dag.test(
        run_conf={
            "git_revision": rev,
            "plan": plan,
            "standard_engine_plan": standard_engine_plan,
        }
    )
