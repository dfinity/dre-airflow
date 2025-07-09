import datetime

import pendulum
from operators.target_topology import (
    RunTopologyToolAndUploadOutputs,
    SendReport,
)

from airflow import DAG
from airflow.models.param import Param

with DAG(
    dag_id="target_topology",
    schedule="@daily",
    start_date=pendulum.datetime(2020, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(days=14),
    tags=["topology"],
    params={
        "target_topology_git": Param(
            # TODO: this should be the public repo that contains everything.
            default="https://github.com/dfinity/node_allocation.git",
            type="string",
            title="Node allocations git repository",
            description="URL to use to clone the node allocations git.  Anything"
            " after the (optional) # sign will be understood as the branch to use from"
            " the repo (defaults to main if unspecified)",
        ),
        "scenario": Param(
            # TODO: agree on which scenario will we try to use.
            default="airflow_clusters",
            type="string",
            title="Cluster scenario",
            description="One of the cluster scenario JSON files specified in the"
            " target topology data/cluster_scenarios",
        ),
        "topology_file": Param(
            # TODO: agree on which topology to run by default.
            default="./data/topology/airflow_topology.csv",
            type="string",
            title="Topology file",
            description="One of the topology files in `./data/topology`",
        ),
        "node_pipeline": Param(
            default="./data/node_pipelines/airflow_pipeline.csv",
            type="string",
            title="Node pipeline",
            description="One of the pipeline files in `./data/node_pipelines`",
        ),
        "folder_name": Param(
            default=None,
            type=["null", "string"],
            title="Output folder pattern",
            description="This will be the folder in Google Drive where the uploads will"
            " be present.  If empty, it will be YYYY_MM_DD based on the run's date",
        ),
    },
) as dag:
    """ This dag is used to run the target topology tool.

    The steps it needs to run:
    1. Run the tool
        Consists of:
        - Clone the target topology git repo
        - Setup the repostiry - poetry install
        - Fetch the newest data
        - Configure the config.json to run the proper scenario
        - Upload everything to the google drive
        - Calculate actual proposals needed to execute and the batches
        - Post a slack message about the outcome and next steps
    2. Poll something to see if we should proceed (if the run is approved or not)
        - ATM poll a file in gdrive
        - Run the batches
    """
    # DAG definition

    (
        RunTopologyToolAndUploadOutputs(
            target_topology_git="{{ params.target_topology_git }}",
            scenario="{{ params.scenario }}",
            topology_file="{{ params.topology_file }}",
            node_pipeline="{{ params.node_pipeline }}",
            drive_subfolder="""{{
                params.folder_name or data_interval_end.strftime('%Y-%m-%d')
            }}""",
            task_id="run_topology_tool",
        )
        >> SendReport(
            drive_subfolder="""{{
                params.folder_name or data_interval_end.strftime('%Y-%m-%d')
            }}""",
            log_url="""{{
                 ti.get_dagrun().get_task_instance('run_topology_tool').log_url
            }}""",
        )
    )
