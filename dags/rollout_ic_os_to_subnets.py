"""
Rollout IC os to subnets in batches.

Each batch runs in parallel.
"""

import datetime
import functools
from typing import Any, cast

import operators.ic_os_rollout as ic_os_rollout
import pendulum
import sensors.ic_os_rollout as ic_os_sensor
import yaml
from dfinity.ic_admin import get_subnet_list
from dfinity.ic_api import IC_NETWORKS
from dfinity.ic_os_rollout import SubnetIdWithRevision, rollout_planner
from dfinity.ic_types import SubnetRolloutInstance

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

# Also defined in dfinity.ic_os_rollout
MAX_BATCHES: int = 30
DEFAULT_PLANS: dict[str, str] = {
    "mainnet": """
# See documentation at the end of this configuration block.
Monday:
  9:00:      [io67a]
  11:00:     [shefu, uzr34]
Tuesday:
  7:00:      [pjljw, qdvhd, bkfrj]
  9:00:      [snjp4, w4asl, qxesv]
  11:00:     [4zbus, ejbmu, 2fq7c]
Wednesday:
  7:00:      [pae4o, 5kdm2, csyj4]
  9:00:      [eq6en, lhg73, brlsh]
  11:00:     [k44fs, cv73p, 4ecnw]
  13:00:     [opn46, lspz2, o3ow2]
Thursday:
  7:00:      [w4rem, 6pbhf, e66qm]
  9:00:      [yinp6, fuqsr, jtdsg]
  11:00:     [mpubz, x33ed, pzp6e]
  13:00:     [3hhby, nl6hn, gmq5v]
Monday next week:
  7:00:
    subnets: [tdb26]
    batch: 30
# Remarks:
# * All times are expressed in the UTC time zone.
# * Days refer to dates relative to your current work week
#   if starting a rollout during a workday, or next week if
#   the rollout is started during a weekend.
# * A day name with " next week" added at the end means
#   "add one week to this day".
# * Each date/time can specify a simple list of subnets,
#   or can specify a dict with two keys:
#   * batch: an optional integer 1-30 with the batch number
#            you want to assign to this batch.
#   * subnets: a list of subnets.
# * A subnet may be specified:
#   * as an integer number from 0 to the maximum subnet number,
#   * as a full or abbreviated subnet principal ID,
#   * as a dictionary of {
#        subnet: ID or principal
#        git_revision: revision to deploy to this subnet
#     }
#     with this form being able to override the Git revision
#     that will be targeted to that specific subnet.
#     Example of a batch specified this way:
#       Monday next week:
#         7:00:
#           batch: 30
#           subnets:
#           - subnet: tdb26
#             git_revision: 0123456789012345678901234567890123456789
"""
}

_PLAN_FORM = """
    <textarea class="form-control" name="{name}" 
           id="{name}" placeholder=""
           type="text"
           required="" rows="24">{value}</textarea>
"""


DAGS: dict[str, DAG] = {}
for network_name, network in IC_NETWORKS.items():
    with DAG(
        dag_id=f"rollout_ic_os_to_{network_name}_subnets",
        schedule=None,
        start_date=pendulum.datetime(2020, 1, 1, tz="UTC"),
        catchup=False,
        dagrun_timeout=datetime.timedelta(days=14),
        tags=["rollout", "DRE", "IC OS"],
        render_template_as_native_obj=True,
        params={
            "git_revision": Param(
                "0000000000000000000000000000000000000000",
                type="string",
                pattern="^[a-f0-9]{40}$",
                title="Main Git revision",
                description="Git revision of the IC-OS release to roll out to subnets"
                ", unless specified otherwise directly for a specific subnet; will"
                " also determine the version of ic-admin used",
            ),
            "plan": Param(
                default=DEFAULT_PLANS[network_name].strip(),
                type="string",
                title="Rollout plan",
                description="A YAML-formatted string describing the rollout schedule",
                custom_html_form=_PLAN_FORM,
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
        def schedule(**context):  # type: ignore
            plan_data_structure = yaml.safe_load(
                context["task"].render_template("{{ params.plan }}", context)
            )
            ic_admin_version = "{:040}".format(
                context["task"].render_template("{{ params.git_revision }}", context)
            )
            kwargs: dict[str, Any] = {"network": network}
            if ic_admin_version not in ["0000000000000000000000000000000000000000", 0]:
                kwargs["ic_admin_version"] = ic_admin_version
            subnet_list_source = functools.partial(get_subnet_list, **kwargs)
            plan = rollout_planner(
                plan_data_structure,
                subnet_list_source=subnet_list_source,
            )
            for nstr, (start_time, members) in plan.items():
                print(f"Batch {int(nstr)+1}:")
                for item in members:
                    if not item.git_revision:
                        item.git_revision = ic_admin_version
                    print(
                        f"    Subnet {item.subnet_id} ({item.subnet_num}) will start"
                        f" to be rolled out at {item.start_at} to git"
                        f" revision {item.git_revision}."
                    )
            return plan

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
                try:
                    subnets = cast(
                        list[SubnetRolloutInstance],
                        (
                            kwargs["ti"]
                            .xcom_pull("schedule")
                            .get(str(current_batch_index))[1]
                        ),
                    )
                    return [
                        {"subnet_id": s.subnet_id, "git_revision": s.git_revision}
                        for s in subnets
                    ]
                except (KeyError, TypeError):  # no such batch
                    print("This batch is empty.")
                    return []

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
                >> ic_os_rollout.CreateProposalIdempotently.partial(
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

        sched = schedule()
        revs = revisions(sched)
        wait_for_election = ic_os_sensor.WaitForRevisionToBeElected.partial(
            task_id="wait_for_revision_to_be_elected",
            simulate_elected=cast(bool, "{{ params.simulate }}"),
            network=network,
            retries=retries,
        ).expand(git_revision=revs)

        task_groups = []
        for batch in range(MAX_BATCHES):
            batch_name = str(batch + 1)
            with TaskGroup(group_id=f"batch_{batch_name}") as group:
                make_me_a_batch(batch_name, batch)
                task_groups.append(group)
        chain(wait_for_election, *task_groups)


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
        plan = DEFAULT_PLANS["mainnet"]
    dag = DAGS["mainnet"]
    dag.test(
        run_conf={
            "git_revision": rev,
            "plan": plan,
        }
    )
