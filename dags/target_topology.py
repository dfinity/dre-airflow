import datetime
from pathlib import Path

import pendulum
from operators.target_topology import RunTopologyTool, SendReport, UploadOutputs

from airflow import DAG
from airflow.models.param import Param

REPO_DIR = Path("/tmp/target_topology")
GOOGLE_DRIVE_FOLDER = "1v3ISHRdNHm0p1J1n-ySm4GNl4sQGEf77"

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
            title="Url to use to clone the node allocations git directory.",
        ),
        "scenario": Param(
            # TODO: agree on which scenario will we try to use.
            default="airflow_clusters",
            type="string",
            title="One of the cluster scenario json files specified in the target \
            topology data/cluster_scenarios",
        ),
        "topology_file": Param(
            # TODO: agree on which topology to run by default.
            default="./data/topology/airflow_topology.csv",
            type="string",
            title="One of the topology files in `./data/topology`",
        ),
        "node_pipeline": Param(
            default="./data/node_pipelines/airflow_pipeline.csv",
            type="string",
            title="One of the pipeline files in `./data/node_pipelines`",
        ),
        "folder_name": Param(
            default="",
            type="string",
            title="This will be the folder in Google Drive where the uploads will"
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
        RunTopologyTool(
            target_topology_git="{{ params.target_topology_git }}",
            scenario="{{ params.scenario }}",
            topology_file="{{ params.topology_file }}",
            node_pipeline="{{ params.node_pipeline }}",
            repo_root=Path("/tmp/target_topology"),
        )
        >> UploadOutputs(
            folder=REPO_DIR / "output",
            folder_id=GOOGLE_DRIVE_FOLDER,
            drive_folder="{{params.folder_name or logical_date.strftime('%Y_%m_%d')}}",
        )
        >> SendReport(scenario="{{ params.scenario }}")
    )
