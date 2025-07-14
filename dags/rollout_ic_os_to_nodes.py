# mypy: disable-error-code=unused-ignore

"""
Rollout IC os to subnets in batches.

Each batch runs in parallel.
"""

import datetime
import typing

import pendulum
import sensors.ic_os_rollout as ic_os_sensor
from dfinity.ic_os_rollout import PLAN_FORM
from dfinity.ic_types import IC_NETWORKS
from dfinity.rollout_types import DEFAULT_HOSTOS_ROLLOUT_PLANS as DEFAULT_ROLLOUT_PLANS
from dfinity.rollout_types import HOSTOS_ROLLOUT_PLAN_HELP as ROLLOUT_PLAN_HELP
from dfinity.rollout_types import HostOSStage
from operators import hostos_rollout as hostos_operators
from operators.ic_os_rollout import RequestProposalVote
from sensors import hostos_rollout as hostos_sensors

import airflow.operators.python as python_operator
import airflow.sensors.python as python_sensor
from airflow.decorators import dag, task, task_group
from airflow.models.baseoperator import chain
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator

for network_name, network in IC_NETWORKS.items():

    @dag(
        dag_id=f"rollout_ic_os_to_{network_name}_nodes",
        schedule=None,
        start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
        catchup=False,
        dagrun_timeout=datetime.timedelta(days=45),
        tags=["rollout", "DRE", "IC OS", "HostOS"],
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
                default=DEFAULT_ROLLOUT_PLANS[network_name].strip(),
                type="string",
                title="Rollout plan",
                description_md=ROLLOUT_PLAN_HELP,
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
    def rollout_ic_os_to_nodes() -> None:
        retries = int(86400 / 60 / 5)  # one day worth of retries

        schedule = task(hostos_operators.schedule)

        timetable = schedule(network=network)  # type: ignore
        batches = []
        batch_name: HostOSStage
        for batch__name, batch_count in [
            ("canary", hostos_operators.CANARY_BATCH_COUNT),
            ("main", hostos_operators.MAIN_BATCH_COUNT),
            ("unassigned", hostos_operators.UNASSIGNED_BATCH_COUNT),
            ("stragglers", 1),
        ]:
            batch_name = typing.cast(HostOSStage, batch__name)
            for batch_index in range(batch_count):

                @task_group(
                    group_id=hostos_operators.stage_name(batch_name, batch_index)
                )
                def batch(stage: HostOSStage, batch_index: int) -> None:
                    nodes_xcom_pull = (
                        "{{ ti.xcom_pull('%s.collect_nodes', key='nodes') }}"
                        % (hostos_operators.stage_name(stage, batch_index))
                    )
                    start_time_xcom_pull = """{{
                        ti.xcom_pull(task_ids='%s.plan', key="start_at")
                        | string
                    }}""" % (hostos_operators.stage_name(stage, batch_index))
                    proposal_xcom_pull = """{{
                        ti.xcom_pull(task_ids='%s.create_proposal_if_none_exists')
                    }}""" % (hostos_operators.stage_name(stage, batch_index))

                    plan = python_operator.BranchPythonOperator(
                        task_id="plan",
                        python_callable=hostos_operators.plan,
                        op_args=[stage, batch_index],
                    )
                    wait = ic_os_sensor.CustomDateTimeSensorAsync(
                        task_id="wait_until_start_time",
                        target_time=start_time_xcom_pull,
                        simulate="{{ params.simulate }}",
                    )
                    # FIXME up number of tasks fetched (max tasks) in rollout dashboard!
                    nodes = python_operator.BranchPythonOperator(
                        task_id="collect_nodes",
                        python_callable=hostos_operators.collect_nodes,
                        op_args=[stage, batch_index, network],
                        retries=retries,
                    )
                    propose = python_operator.PythonOperator(
                        task_id="create_proposal_if_none_exists",
                        python_callable=hostos_operators.create_proposal_if_none_exists,
                        op_args=[nodes_xcom_pull, network],
                        retries=retries,
                        do_xcom_push=True,
                    )
                    announce = RequestProposalVote(
                        task_id="request_proposal_vote",
                        source_task_id="%s.create_proposal_if_none_exists"
                        % hostos_operators.stage_name(stage, batch_index),
                        retries=retries,
                    )
                    accept = python_sensor.PythonSensor(
                        task_id="wait_until_proposal_is_accepted",
                        python_callable=ic_os_sensor.has_proposal_executed,
                        poke_interval=120,
                        timeout=86400 * 7,
                        mode="reschedule",
                        op_args=[proposal_xcom_pull, network, "{{ params.simulate }}"],
                        retries=retries,
                    )
                    adopt = python_sensor.PythonSensor(
                        task_id="wait_for_revision_adoption",
                        python_callable=hostos_sensors.have_hostos_nodes_adopted_revision,
                        poke_interval=120,
                        timeout=86400 * 7,
                        mode="reschedule",
                        op_args=[nodes_xcom_pull, network],
                        retries=retries,
                    )
                    healthy = python_sensor.PythonSensor(
                        task_id="wait_until_nodes_healthy",
                        python_callable=hostos_sensors.are_hostos_nodes_healthy,
                        poke_interval=120,
                        timeout=86400 * 7,
                        mode="reschedule",
                        op_args=[nodes_xcom_pull, network],
                        retries=retries,
                    )
                    join = EmptyOperator(
                        task_id="join",
                        trigger_rule="none_failed_min_one_success",
                    )
                    # And now the dependency of the batches.
                    plan >> wait >> nodes >> propose >> announce
                    propose >> accept >> adopt >> healthy
                    [plan, nodes, announce, healthy] >> join

                batches.append(batch(batch_name, batch_index))

        wait_for_other_rollouts = ic_os_sensor.WaitForOtherDAGs(
            task_id="wait_for_other_rollouts"
        )

        wait_for_election = python_sensor.PythonSensor(
            task_id="wait_until_nodes_healthy",
            python_callable=hostos_sensors.has_network_adopted_hostos_revision,
            poke_interval=300,
            timeout=86400 * 7,
            mode="reschedule",
            op_args=[network],
            retries=retries,
        )

        chain([timetable, wait_for_election, wait_for_other_rollouts], *batches)

    rollout_ic_os_to_nodes()
