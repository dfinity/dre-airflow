"""
Rollout IC os to subnets in batches.

Each batch runs in parallel.
"""

import datetime

import pendulum
import sensors.ic_os_rollout as ic_os_sensor
from dfinity import hostos_rollout
from dfinity.hostos_rollout import (
    CANARY_BATCH_COUNT,
    MAIN_BATCH_COUNT,
    UNASSIGNED_BATCH_COUNT,
    collect_nodes,
    stage_name,
)
from dfinity.ic_os_rollout import (
    DEFAULT_HOSTOS_ROLLOUT_PLANS,
    PLAN_FORM,
)
from dfinity.ic_types import IC_NETWORKS
from dfinity.rollout_types import HostOSStage

import airflow.operators.python as python_operator
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
                default=DEFAULT_HOSTOS_ROLLOUT_PLANS[network_name].strip(),
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
    def rollout_ic_os_to_nodes() -> None:
        retries = int(86400 / 60 / 5)  # one day worth of retries

        schedule = task(hostos_rollout.schedule)

        timetable = schedule(network=network)  # type: ignore # noqa
        batches = []
        batch_name: HostOSStage
        for batch_name, batch_count in [  # type:ignore
            ("canary", CANARY_BATCH_COUNT),
            ("main", MAIN_BATCH_COUNT),
            ("unassigned", UNASSIGNED_BATCH_COUNT),
            ("stragglers", 1),
        ]:
            for batch_index in range(batch_count):

                @task_group(group_id=stage_name(batch_name, batch_index))
                def batch(stage: HostOSStage, batch_index: int) -> None:
                    plan = python_operator.BranchPythonOperator(
                        task_id="plan",
                        python_callable=hostos_rollout.plan,
                        op_args=[stage, batch_index],
                    )
                    wait = ic_os_sensor.CustomDateTimeSensorAsync(
                        task_id="wait_until_start_time",
                        target_time="""{{
                            ti.xcom_pull(task_ids='%s.plan', key="start_at")
                            | string
                        }}"""
                        % (stage_name(stage, batch_index)),
                        simulate="{{ params.simulate }}",
                    )
                    chain(plan, wait)
                    # FIXME: if no nodes, we should skip straight to the next batch!
                    # FIXME up number of tasks fetched (max  tasks) in rollout dashboard!!!
                    nodes = python_operator.BranchPythonOperator(
                        task_id="collect_nodes",
                        python_callable=collect_nodes,
                        op_args=[stage, batch_index, network],
                        retries=retries,
                    )
                    chain(wait, nodes)
                    propose = python_operator.PythonOperator(
                        task_id="create_proposal_if_none_exists",
                        python_callable=hostos_rollout.create_proposal_if_none_exists,
                        op_args=[stage, batch_index, network],
                    )
                    chain(nodes, propose)
                    announce = python_operator.PythonOperator(
                        task_id="request_proposal_vote",
                        python_callable=hostos_rollout.request_proposal_vote,
                        op_args=[stage, batch_index, network],
                    )
                    chain(propose, announce)
                    accept = python_operator.PythonOperator(
                        task_id="wait_until_proposal_is_accepted",
                        python_callable=hostos_rollout.wait_until_proposal_is_accepted,
                        op_args=[stage, batch_index, network],
                    )
                    chain(propose, accept)
                    adopt = python_operator.PythonOperator(
                        task_id="wait_for_revision_adoption",
                        python_callable=hostos_rollout.wait_for_revision_adoption,
                        op_args=[stage, batch_index, network],
                    )
                    chain(accept, adopt)
                    healthy = python_operator.PythonOperator(
                        task_id="wait_until_nodes_healthy",
                        python_callable=hostos_rollout.wait_until_nodes_healthy,
                        op_args=[stage, batch_index, network],
                    )
                    chain(adopt, healthy)
                    join = EmptyOperator(
                        task_id="join",
                        trigger_rule="none_failed_min_one_success",
                    )
                    chain([plan, nodes, propose, healthy, announce], join)

                batches.append(batch(batch_name, batch_index))

        chain([timetable], *batches)

    rollout_ic_os_to_nodes()
