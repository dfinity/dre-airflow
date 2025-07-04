"""
Automatic computation of next rollout.
"""

import datetime
import os

import operators.auto_rollout as auto_rollout
import operators.github_rollout as github_rollout
import operators.gsheets_rollout as gsheets_rollout
import pendulum
from dfinity.ic_os_rollout import (
    DEFAULT_API_BOUNDARY_NODES_ROLLOUT_PLANS,
    DEFAULT_GUESTOS_ROLLOUT_PLANS,
    PLAN_FORM,
)
from dfinity.ic_types import IC_NETWORKS

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import BranchPythonOperator

DEFAULT_ROLLOUT_PLAN_SHEETS = {
    "mainnet": "1ZcYB0gWjbgg7tFgy2Fhd3llzYlefJIb0Mik75UUrSXM",
}

DAGS: dict[str, DAG] = {}
for network_name, network in IC_NETWORKS.items():
    with DAG(
        dag_id=f"auto_compute_rollout_to_{network_name}",
        schedule_interval="0 18 * * 0",
        start_date=pendulum.datetime(2024, 5, 5, tz="UTC"),
        catchup=False,
        dagrun_timeout=datetime.timedelta(days=2),
        tags=["rollout", "DRE", "IC OS"],
        render_template_as_native_obj=True,
        params={
            # You can force a different release index URL (which must be publicly
            # accessible) by setting variable RELEASE_INDEX_URL_FOR_TESTING to the URL
            # you want.
            "release_index_url": Param(
                os.getenv(
                    "RELEASE_INDEX_URL_FOR_TESTING",
                    "https://raw.githubusercontent.com/dfinity/dre/main/release-index.yaml",
                ),
                type="string",
                title="Release index URL",
                description="URL that holds the list of releases approved for rollout.",
            ),
            # You can force that sheet to be used by setting variable
            # GSHEET_ID_FOR_TESTING to the GSheet ID you want to use for testing.
            # The usual test sheet ID is 1ogxK-3rupI1pX0oLQgf467bGIsn8pNCgu88mPMw8nwM.
            "release_feature_spreadsheet_id": Param(
                default=os.getenv(
                    "GSHEET_FOR_TESTING",
                    default=DEFAULT_ROLLOUT_PLAN_SHEETS[network_name],
                ).strip(),
                type="string",
                title="Feature rollout plan Google sheet ID",
                description="The ID of a spreadsheet holding the weekly feature plan.",
            ),
            "max_days_lookbehind": Param(
                default=5,
                type="integer",
                title="Maximum lookbehind in days",
                description="How many days to look back for releases in release index.",
            ),
            "guestos_rollout_plan": Param(
                default=DEFAULT_GUESTOS_ROLLOUT_PLANS[network_name].strip(),
                type="string",
                title="GuestOS rollout plan",
                description="A YAML-formatted string describing the GuestOS"
                " rollout schedule.",
                custom_html_form=PLAN_FORM,
            ),
            "api_boundary_nodes_rollout_plan": Param(
                default=DEFAULT_API_BOUNDARY_NODES_ROLLOUT_PLANS[network_name].strip(),
                type="string",
                title="API boundary nodes rollout plan",
                description="A YAML-formatted string describing the API boundary nodes"
                " rollout schedule.",
                custom_html_form=PLAN_FORM,
            ),
            "start_rollout": Param(
                default=True,
                type="boolean",
                title="Auto start rollout",
                description="If enabled, the rollout plan"
                " will be used to create and auto start a rollout.",
            ),
        },
    ) as dag:
        DAGS[network_name] = dag
        retries = int(86400 / 60 / 5)  # one day worth of retries

        compute = auto_rollout.AutoComputeRolloutPlan(
            task_id="auto_compute_rollout",
            release_versions_data_task_id="get_release_versions",
            feature_rollout_plan_task_id="get_feature_rollout_plan",
            max_days_lookbehind="{{ params.max_days_lookbehind }}",  # type: ignore
            guestos_rollout_plan="{{ params.guestos_rollout_plan }}",
            api_boundary_nodes_rollout_plan="{{ params"
            ".api_boundary_nodes_rollout_plan }}",
        )

        (
            gsheets_rollout.GetFeatureRolloutPlan(
                task_id="get_feature_rollout_plan",
                spreadsheet={
                    "spreadsheetId": "{{ params.release_feature_spreadsheet_id }}"
                },
                retries=retries,
            )
            >> compute
        )
        (
            github_rollout.GetReleases(
                task_id="get_release_versions",
                release_index_url="{{ params.release_index_url }}",
                retries=retries,
            )
            >> compute
        )

        (
            compute
            >> BranchPythonOperator(
                task_id="decide_to_start_rollout",
                python_callable=lambda should_start: (
                    ["start_rollout"] if should_start else []
                ),
                op_args=[
                    "{{ params.start_rollout }}",
                ],
            )
            >> (
                auto_rollout.TriggerGuestOSRollout(
                    task_id="start_guestos_rollout",
                    trigger_dag_id=f"rollout_ic_os_to_{network_name}_subnets",
                    plan_task_id="auto_compute_rollout",
                    simulate_rollout=False,
                ),
                auto_rollout.TriggerAPIBoundaryNodesRollout(
                    task_id="start_api_boundary_nodes_rollout",
                    trigger_dag_id=f"rollout_ic_os_to_{network_name}_api_boundary_nodes",
                    plan_task_id="auto_compute_rollout",
                    simulate_rollout=False,
                ),
            )
        )


if __name__ == "__main__":
    dag = DAGS["mainnet"]
    dag.test(run_conf={})
