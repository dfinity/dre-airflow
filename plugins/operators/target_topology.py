import datetime
import json
import shutil
import subprocess
from pathlib import Path
from typing import Any

import requests
from dfinity.ic_os_rollout import SLACK_CHANNEL, SLACK_CONNECTION_ID

import airflow.providers.slack.operators.slack as slack
from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.suite.hooks.drive import GoogleDriveHook

REPO_DIR = Path("/tmp/target_topology")
GOOGLE_DRIVE_FOLDER = "1v3ISHRdNHm0p1J1n-ySm4GNl4sQGEf77"
GITHUB_CONNECTION_ID = "github.node_allocation"


def format_slack_payload(scenario: str) -> str:
    now = datetime.datetime.now()
    formatted_dt = now.strftime("%A, %d %B %Y")
    return [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"Ignore for now: Target topology run report for\\\
                 {formatted_dt}",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "text": {
                "type": "plain_text",
                "text": "The run of the target topology tool for today is complete!",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"To view the artifacts for scenario {scenario}:",
            },
            "accessory": {
                "type": "button",
                "text": {
                    "type": "plain_text",
                    "text": "Open drive :open_file_folder:",
                    "emoji": True,
                },
                "value": "click_me_123",
                "url": "https://drive.google.com/drive/u/2/folders/"
                + GOOGLE_DRIVE_FOLDER,
                "action_id": "button-action",
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "Please review the contents of the output following the\\\
                 `ReviewGuide.md`",
            },
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "cc <@U02RZR7PDK4>, <@U047B65SPB3>"},
        },
    ]


class SendReport(slack.SlackAPIPostOperator):
    def __init__(
        self,
        scenario: str,
        **kwargs,
    ) -> None:
        self.scenario = scenario
        slack.SlackAPIPostOperator.__init__(
            self,
            channel=SLACK_CHANNEL,
            username="Airflow",
            blocks=format_slack_payload(scenario),
            slack_conn_id=SLACK_CONNECTION_ID,
            task_id="send_report",
            **kwargs,
        )


class UploadOutputs(BaseOperator):
    folder_id: str
    drive_folder: str
    folder: Path
    template_fields = ["drive_folder"]

    def __init__(
        self,
        folder: Path,
        folder_id: str,
        drive_folder: str,
        **kwargs: Any,
    ) -> None:
        if not folder.exists():
            folder.mkdir(parents=True)

        self.folder_id = folder_id
        self.folder = folder
        self.drive_folder = drive_folder

        super().__init__(task_id="upload_outputs", **kwargs)

    def execute(self, context: Any):
        hook = GoogleDriveHook(gcp_conn_id="google_cloud_default")

        for file in self.folder.rglob("*"):
            if not file.is_file():
                continue
            dest_file = Path(self.drive_folder) / file.relative_to(self.folder)

            hook.upload_file(str(file), str(dest_file), folder_id=self.folder_id)


class RunTopologyTool(BaseOperator):
    target_topology_git: str
    scenario: str
    topology_file: str
    node_pipeline: str

    git: str
    topology: str
    pipeline: str
    repo_root: Path
    template_fields = [
        "target_topology_git",
        "scenario",
        "topology_file",
        "node_pipeline",
    ]

    def __init__(
        self,
        target_topology_git: str,
        scenario: str,
        topology_file: str,
        node_pipeline: str,
        repo_root: Path,
        **kwargs: Any,
    ) -> None:
        self.target_topology_git = target_topology_git
        self.scenario = scenario
        self.topology_file = topology_file
        self.node_pipeline = node_pipeline
        self.repo_root = repo_root

        super().__init__(task_id="run_topology_tool", **kwargs)

    def execute(self, context: Any):
        self.git = self.target_topology_git
        self.topology = self.topology_file
        self.pipeline = self.node_pipeline

        self.clone_repository()
        self.setup_repository()
        destination = self.sync_dashboard_data()
        self.configure_tool(destination)
        self.run_tool_inner()
        self.collect_inputs()

    def clone_repository(self) -> None:
        """Clone a git repository."""
        shutil.rmtree(self.repo_root, ignore_errors=True)

        connection = BaseHook.get_connection(GITHUB_CONNECTION_ID)
        token = connection.password

        url = self.git.split("https://")[1]
        url = "https://" + token + "@" + url

        output = subprocess.run(["git", "clone", url, self.repo_root])

        output.check_returncode()

        output = subprocess.run(
            ["git", "checkout", "nim-preparing-airflow-data"], cwd=self.repo_root
        )
        output.check_returncode()

    def setup_repository(self) -> None:
        """Sets up the repository."""
        output = subprocess.run(["poetry", "install"], cwd=self.repo_root)

        output.check_returncode()

    def sync_dashboard_data(self) -> str:
        """Sync the dashboard data."""
        response = requests.get(
            "https://raw.githubusercontent.com/dfinity/decentralization/refs/heads/main/ic_topology/main.py"
        )

        ic_topology = self.repo_root / "ic_topology"
        ic_topology.mkdir(exist_ok=True)

        with open(ic_topology / "main.py", "w+") as f:
            f.write(response.text)

        output = subprocess.run(
            ["poetry", "run", "python", "./ic_topology/main.py"],
            cwd=self.repo_root,
            stdout=subprocess.PIPE,
        )

        output.check_returncode()
        destination = output.stdout.decode().splitlines()[-1]
        destination = destination.replace("Saved current nodes to ", "")

        print("Sync completed. Destination:", destination)
        return destination

    def configure_tool(self, nodes_file: str) -> None:
        """Configure the `config.json`

        This should only change the `nodes_file` in the config since
        that changes daily. Other this are left unchanged.
        """
        config = self.repo_root / "topology_optimizer" / "config.json"

        with open(config, "r") as f:
            configuration = json.load(f)

        configuration["cluster_file"] = self.scenario + ".json"
        configuration["topology_file"] = self.topology
        configuration["node_pipeline_file"] = self.pipeline
        configuration["nodes_file"] = nodes_file

        with open(config, "w") as f:
            json.dump(configuration, f, indent=2)

        print("The configuration has been updated.")

    def run_tool_inner(self) -> None:
        """Run the actual tool.

        This function will run the tool until completion.
        """
        # TODO: enrich this with diagnostics like the overall time it took
        # to run. Also forward the stderr to airflow.
        output = subprocess.run(
            [
                "poetry",
                "run",
                "python",
                "./topology_optimizer/main.py",
                "--config-file",
                "./topology_optimizer/config.json",
            ],
            cwd=self.repo_root,
            stdout=subprocess.PIPE,
        )

        output.check_returncode()

        print("Tool output:\n", output.stdout)

    def collect_inputs(self) -> None:
        inputs = self.repo_root / "output" / "inputs"
        inputs.mkdir(exist_ok=True)

        config = self.repo_root / "topology_optimizer" / "config.json"
        with open(config, "r") as f:
            configuration = json.load(f)

        files = [
            configuration["nodes_file"],
            configuration["topology_file"],
            configuration["node_pipeline_file"],
            configuration["blacklist_file"],
            Path(configuration["scenario_folder"]) / configuration["cluster_file"],
            config,
        ]

        for file in files:
            path = self.repo_root / file
            shutil.copyfile(path, inputs / path.name)
