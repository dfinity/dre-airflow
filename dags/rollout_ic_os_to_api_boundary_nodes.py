"""
Rollout IC OS to boundary nodes in batches.
"""

import datetime
import pprint
import typing

import dfinity.ic_types as ic_types
import operators.ic_os_rollout as ic_os_rollout
import pendulum
import sensors.ic_os_rollout as ic_os_sensor
from dfinity.ic_os_rollout import (
    DEFAULT_API_BOUNDARY_NODES_ROLLOUT_PLANS,
    PLAN_FORM,
    api_boundary_node_batch_create,
    api_boundary_node_batch_timetable,
)
from dfinity.rollout_types import ProposalInfo, yaml_to_ApiBoundaryNodeRolloutPlanSpec

from airflow.decorators import dag, task, task_group
from airflow.models.baseoperator import chain
from airflow.models.param import Param
from airflow.models.taskinstance import TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.sensors.base import PokeReturnValue


class DagParams(typing.TypedDict):
    git_revision: str
    plan: str
    simulate: bool


BatchSpec = tuple[datetime.datetime, list[str]]


BATCH_COUNT: int = 20


for network_name, network in ic_types.IC_NETWORKS.items():

    @dag(
        dag_id=f"rollout_ic_os_to_{network_name}_api_boundary_nodes",
        schedule=None,
        start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
        catchup=False,
        dagrun_timeout=datetime.timedelta(days=14),
        tags=["rollout", "DRE", "IC OS", "API boundary nodes"],
        render_template_as_native_obj=True,
        params={
            "git_revision": Param(
                "0000000000000000000000000000000000000000",
                type="string",
                pattern="^[a-f0-9]{40}$",
                title="Main Git revision",
                description="Git revision of the IC OS GuestOS release to roll out to"
                " subnets, unless specified otherwise directly for a specific subnet;"
                " the version must have been elected before but the rollout will check",
            ),
            "plan": Param(
                default=DEFAULT_API_BOUNDARY_NODES_ROLLOUT_PLANS[network_name].strip(),
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
    )
    def rollout_ic_os_to_api_boundary_nodes() -> None:
        retries = int(86400 / 60 / 5)  # one day worth of retries

        @task()
        def schedule(
            params: DagParams,
        ) -> list[BatchSpec]:
            spec = yaml_to_ApiBoundaryNodeRolloutPlanSpec(params["plan"])
            timetable: list[datetime.datetime] = api_boundary_node_batch_timetable(
                spec, batch_count=BATCH_COUNT
            )
            batches: list[list[str]] = api_boundary_node_batch_create(
                spec["nodes"], batch_count=BATCH_COUNT
            )
            assert len(timetable) == len(batches), (
                "The length of the timetable %s differs from the "
                "length of the batches %s\ntimetable: %s\nbatches: %s"
            ) % (len(timetable), len(batches), timetable, batches)
            t = list(zip(timetable, batches))
            print("Timetable:\n%s" % pprint.pformat(t))
            return t

        @task.branch
        def prepare(
            batch_num: int,
            task_instance: TaskInstance,
        ) -> list[str]:
            timetable = typing.cast(
                list[BatchSpec], task_instance.xcom_pull("schedule")
            )
            run = bool(timetable[batch_num][1])
            if run:
                print(
                    "There are %s nodes for this batch, we must run."
                    % len(timetable[batch_num][1])
                )
            else:
                print("No nodes for this run, we will skip.")
            return (
                [f"batch_{batch_num + 1}.wait_until_start_time"]
                if run
                else [f"batch_{batch_num + 1}.join"]
            )

        @task.sensor(poke_interval=60, timeout=86400 * 7, mode="reschedule")
        def wait_until_start_time(
            batch_num: int,
            task_instance: TaskInstance,
            params: DagParams,
        ) -> PokeReturnValue:
            timetable = typing.cast(
                list[BatchSpec], task_instance.xcom_pull("schedule")
            )
            until, nodes = timetable[batch_num]
            if params["simulate"]:
                print("Simulating waiting until %s", until)
            else:
                print("Waiting until", until)
                now = datetime.datetime.now().replace(tzinfo=None)
                if now < until:
                    return PokeReturnValue(is_done=False)

            return PokeReturnValue(is_done=True, xcom_value=nodes)

        @task(retries=retries)
        def create_proposal_if_none_exists(
            nodes: list[str], params: DagParams
        ) -> ProposalInfo:
            git_revision = params["git_revision"]
            return ic_os_rollout.create_api_boundary_nodes_proposal_if_none_exists(
                nodes, git_revision, network, simulate=params["simulate"]
            )

        @task.sensor(
            retries=retries, poke_interval=120, timeout=86400 * 7, mode="reschedule"
        )
        def wait_until_proposal_is_accepted(
            nodes: list[str],
            proposal_info: ProposalInfo,
            params: DagParams,
        ) -> PokeReturnValue:
            return PokeReturnValue(
                is_done=ic_os_sensor.has_proposal_executed(
                    proposal_info, network, params["simulate"]
                ),
                xcom_value=nodes,
            )

        @task.sensor(
            retries=retries, poke_interval=120, timeout=86400 * 7, mode="reschedule"
        )
        def wait_for_revision_adoption(
            nodes: list[str], params: DagParams
        ) -> PokeReturnValue:
            return PokeReturnValue(
                is_done=ic_os_sensor.have_api_boundary_nodes_adopted_revision(
                    nodes, params["git_revision"], network
                ),
                xcom_value=nodes,
            )

        @task.sensor(
            retries=retries, poke_interval=60, timeout=86400 * 7, mode="reschedule"
        )
        def wait_until_nodes_healthy(
            nodes: list[str], params: DagParams
        ) -> PokeReturnValue:
            return PokeReturnValue(
                is_done=ic_os_sensor.have_api_boundary_nodes_stopped_alerting(
                    nodes, network
                )
            )

        # Begin composition of the flow based on the operators above and imported.
        # t ype: ignore is frequently necessary here since calling these functions
        # is a bit magic -- for example, some of these functions have parameters
        # declared that are automatically added by Airflow during execution, so
        # they appear on the function signature, but not below.  This is because
        # the functions being called below *are not at all* the functions you see
        # above -- they have been wrapped by decorators which do this magic for us.

        timetable = schedule()  # type: ignore

        batches = []
        for batch_index in range(BATCH_COUNT):

            @task_group(group_id=f"batch_{batch_index + 1}")
            def batch(batch_index: int) -> None:
                should_run = prepare(batch_index)  # type: ignore
                nodes_to_rollout = wait_until_start_time(batch_index)  # type: ignore
                chain(should_run, nodes_to_rollout)
                proposed = create_proposal_if_none_exists(nodes_to_rollout)  # type: ignore
                announced = ic_os_rollout.RequestProposalVote(
                    task_id="request_proposal_vote",
                    source_task_id=f"batch_{batch_index + 1}"
                    ".create_proposal_if_none_exists",
                    retries=retries,
                )
                accepted = wait_until_proposal_is_accepted(  # type: ignore
                    nodes=nodes_to_rollout,  # type: ignore
                    proposal_info=proposed,  # type: ignore
                )
                chain(proposed, announced)
                adopted = wait_for_revision_adoption(nodes=nodes_to_rollout)  # type: ignore
                chain(accepted, adopted)
                healthy = wait_until_nodes_healthy(nodes_to_rollout)  # type: ignore
                chain(adopted, healthy)
                join = EmptyOperator(
                    task_id="join",
                    trigger_rule="none_failed_min_one_success",
                )
                chain([should_run, healthy, announced], join)

            batches.append(batch(batch_index))

        wait_for_election = ic_os_sensor.WaitForRevisionToBeElected(
            task_id="wait_for_revision_to_be_elected",
            simulate_elected=typing.cast(bool, "{{ params.simulate }}"),
            network=network,
            retries=retries,
            git_revision="{{ params.git_revision }}",
        )

        wait_for_other_rollouts = ic_os_sensor.WaitForOtherDAGs(
            task_id="wait_for_other_rollouts"
        )

        chain([timetable, wait_for_election, wait_for_other_rollouts], *batches)

    rollout_ic_os_to_api_boundary_nodes()
