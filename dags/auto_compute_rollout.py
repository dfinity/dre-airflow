"""
Automatic computation of next rollout.
"""

import datetime
import os

import operators.auto_rollout as auto_rollout
import operators.github_rollout as github_rollout
import operators.gsheets_rollout as gsheets_rollout
import pendulum
from dfinity.ic_api import IC_NETWORKS
from dfinity.ic_os_rollout import DEFAULT_PLANS, PLAN_FORM

from airflow import DAG
from airflow.models.param import Param

DEFAULT_ROLLOUT_PLAN_SHEETS = {
    "mainnet": "10smPe_HeWkbIY5nljaP7ogem5AkATEFJg0ihd_jof9I",
}

DAGS: dict[str, DAG] = {}
for network_name, network in IC_NETWORKS.items():
    with DAG(
        dag_id=f"auto_compute_rollout_to_{network_name}",
        schedule=None,
        start_date=pendulum.datetime(2020, 1, 1, tz="UTC"),
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
            "plan": Param(
                default=DEFAULT_PLANS[network_name].strip(),
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

        features = gsheets_rollout.GetFeatureRolloutPlan(
            task_id="get_feature_rollout_plan",
            spreadsheet={
                "spreadsheetId": "{{ params.release_feature_spreadsheet_id }}"
            },
        )
        releases = github_rollout.GetReleases(
            task_id="get_release_versions",
            release_index_url="{{ params.release_index_url }}",
        )
        compute = auto_rollout.AutoComputeRolloutPlan(
            task_id="auto_compute_rollout",
            release_versions_data_task_id="get_release_versions",
            feature_rollout_plan_task_id="get_feature_rollout_plan",
            max_days_lookbehind="{{ params.max_days_lookbehind }}",  # type: ignore
            default_rollout_plan="{{ params.plan }}",
        )

        features >> compute
        releases >> compute


if __name__ == "__main__":

    dag = DAGS["mainnet"]
    dag.test(run_conf={})
