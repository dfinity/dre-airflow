import datetime
import json
import pprint
import shlex
import shutil
import tempfile
from functools import partial
from pathlib import Path
from subprocess import CalledProcessError
from typing import Any, cast

import requests
from dfinity.ic_os_rollout import SLACK_CHANNEL, SLACK_CONNECTION_ID

import airflow.providers.slack.operators.slack as slack
from airflow.hooks.base import BaseHook
from airflow.hooks.subprocess import SubprocessHook, SubprocessResult
from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.suite.hooks.drive import GoogleDriveHook

REPO_DIR = Path("/tmp/target_topology")
GOOGLE_DRIVE_FOLDER = "1v3ISHRdNHm0p1J1n-ySm4GNl4sQGEf77"
GITHUB_CONNECTION_ID = "github.node_allocation"
GOOGLE_CONNECTION_ID = "google_cloud_default"


def format_slack_payload(scenario: str) -> list[object]:
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
        **kwargs: Any,
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


class RunTopologyToolAndUploadOutputs(BaseOperator):
    template_fields = [
        "target_topology_git",
        "scenario",
        "topology_file",
        "node_pipeline",
        "drive_subfolder",
    ]

    def __init__(
        self,
        target_topology_git: str,
        scenario: str,
        topology_file: str,
        node_pipeline: str,
        drive_subfolder: str,
        **kwargs: Any,
    ) -> None:
        self.target_topology_git = target_topology_git
        self.scenario = scenario
        self.topology_file = topology_file
        self.node_pipeline = node_pipeline
        self.folder_id = GOOGLE_DRIVE_FOLDER
        self.drive_subfolder = drive_subfolder

        super().__init__(**kwargs)

    def execute(self, context: Any) -> None:
        self.log.info(
            "User requested results to be uploaded to subfolder %s",
            self.drive_subfolder,
        )
        with tempfile.TemporaryDirectory(prefix="target_topology") as folder:
            scratch_folder = Path(folder)
            self.clone_repository(scratch_folder)
            self.setup_repository(scratch_folder)
            destination = self.sync_dashboard_data(scratch_folder)
            self.configure_tool(scratch_folder, destination)
            self.run_tool_inner(scratch_folder)
            self.collect_inputs(scratch_folder)
            self.upload_outputs(scratch_folder)

    def run_cmd(
        self, cmd: list[str | Path], cwd: str | Path | None = None, check: bool = True
    ) -> SubprocessResult:
        shlexed = shlex.join([str(s) for s in cmd])
        print(f"::group::Output of {shlexed}")
        with tempfile.NamedTemporaryFile(mode="r") as f:
            wrapped_cmd = ["bash", "-c", shlexed + " > " + f.name]
            r = SubprocessHook().run_command(wrapped_cmd, cwd=str(cwd) if cwd else None)
            f.seek(0)
            data = f.read()
            if check and r.exit_code != 0:
                raise CalledProcessError(r.exit_code, cmd, output=data, stderr=None)
            if data.rstrip():
                print(data.rstrip())
        print("::endgroup::")
        return SubprocessResult(r.exit_code, data)

    def clone_repository(self, scratch_folder: Path) -> None:
        """Clone a git repository."""
        self.log.info("Cloning %s to %s .", self.target_topology_git, scratch_folder)
        connection = BaseHook.get_connection(GITHUB_CONNECTION_ID)
        token = connection.password

        url = self.target_topology_git.split("https://")[1]
        url = "https://" + token + "@" + url
        if "#" in url:
            url, _, branch = url.partition("#")
        else:
            url, branch = url, "main"

        shutil.rmtree(scratch_folder)
        self.run_cmd(["git", "clone", url, scratch_folder])
        self.run_cmd(["git", "checkout", branch], cwd=scratch_folder)

    def setup_repository(self, scratch_folder: Path) -> None:
        """Sets up the repository."""
        self.log.info("Setting up repository environment.")
        self.run_cmd(["poetry", "install"], cwd=scratch_folder)

    def sync_dashboard_data(self, scratch_folder: Path) -> str:
        """Sync the dashboard data."""
        self.log.info("Synchronizing dashboard data.")
        response = requests.get(
            "https://raw.githubusercontent.com/dfinity/decentralization/refs/heads/main/ic_topology/main.py"
        )
        response.raise_for_status()

        ic_topology = scratch_folder / "ic_topology"
        ic_topology.mkdir(exist_ok=True)

        with open(ic_topology / "main.py", "w+") as f:
            f.write(response.text)

        output = self.run_cmd(
            ["poetry", "run", "python", "./ic_topology/main.py"], cwd=scratch_folder
        )

        destination = cast(str, output.output.splitlines()[-1])
        destination = destination.replace("Saved current nodes to ", "")

        self.log.info("Sync completed. Destination: %s", destination)
        return destination

    def configure_tool(self, scratch_folder: Path, nodes_file: str) -> None:
        """Configure the `config.json`

        This should only change the `nodes_file` in the config since
        that changes daily. Other this are left unchanged.
        """
        self.log.info("Configuring topology optimizer.")
        config = scratch_folder / "topology_optimizer" / "config.json"

        with open(config, "r") as f:
            configuration = json.load(f)

        configuration["cluster_file"] = self.scenario + ".json"
        configuration["topology_file"] = self.topology_file
        configuration["node_pipeline_file"] = self.node_pipeline
        configuration["nodes_file"] = nodes_file

        with open(config, "w") as f:
            json.dump(configuration, f, indent=2)

        print(
            "The configuration has been updated.  It looks like this:\n%s"
            % pprint.pformat(configuration),
        )

    def run_tool_inner(self, scratch_folder: Path) -> None:
        """Run the actual tool.

        This function will run the tool until completion.
        """
        self.log.info("Running topology optimizer.")
        # TODO: enrich this with diagnostics like the overall time it took
        # to run. Also forward the stderr to airflow.
        self.run_cmd(
            [
                "poetry",
                "run",
                "python",
                "./topology_optimizer/main.py",
                "--config-file",
                "./topology_optimizer/config.json",
            ],
            cwd=scratch_folder,
        )

    def collect_inputs(self, scratch_folder: Path) -> None:
        self.log.info("Collecting inputs.")
        inputs = scratch_folder / "output" / "inputs"
        inputs.mkdir(exist_ok=True)

        config = scratch_folder / "topology_optimizer" / "config.json"
        with open(config, "r") as f:
            configuration = json.load(f)

        files: list[str | Path] = [
            configuration["nodes_file"],
            configuration["topology_file"],
            configuration["node_pipeline_file"],
            configuration["blacklist_file"],
            Path(configuration["scenario_folder"]) / configuration["cluster_file"],
            config,
        ]

        for file in files:
            path = scratch_folder / file
            shutil.copyfile(path, inputs / path.name)

    def upload_outputs(self, scratch_folder: Path) -> None:
        self.log.info("Uploading inputs and outputs.")
        hook = GoogleDriveHook(gcp_conn_id=GOOGLE_CONNECTION_ID)
        upload = partial(hook.upload_file, folder_id=self.folder_id)

        output = scratch_folder / "output"
        for file in output.rglob("*"):
            if not file.is_file():
                continue
            dest_file = Path(self.drive_subfolder) / file.relative_to(output)
            upload(str(file), str(dest_file))
