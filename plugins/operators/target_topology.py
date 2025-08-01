import datetime
import json
import pprint
import shlex
import shutil
import tempfile
import zipfile
from pathlib import Path
from subprocess import CalledProcessError
from typing import Any, Sequence, cast

import requests
from dfinity.ic_os_rollout import SLACK_CHANNEL, SLACK_CONNECTION_ID

import airflow.providers.slack.operators.slack as slack
from airflow.hooks.base import BaseHook
from airflow.hooks.subprocess import SubprocessHook, SubprocessResult
from airflow.models.baseoperator import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

REPO_DIR = Path("/tmp/target_topology")
GITHUB_CONNECTION_ID = "github.node_allocation"
S3_CONNECTION_ID = "wasabi.target_topology"
S3_BUCKET = "dre-target-topology"


def format_slack_payload(subfolder: str, log_link: str, success: bool) -> list[object]:
    now = datetime.datetime.now()
    formatted_dt = now.strftime("%A, %d %B %Y")
    status_log = ":white_check_mark: success" if success else ":x: failure"
    return [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "Ignore for now: Target topology run report for"
                f" {formatted_dt}",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "text": {
                "type": "plain_text",
                "text": "The run of the target topology tool for today is complete!\n"
                + "Execution status: "
                + status_log,
                "emoji": True,
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "Download artifacts of the run:",
            },
            "accessory": {
                "type": "button",
                "text": {
                    "type": "plain_text",
                    "text": "Download :open_file_folder:",
                    "emoji": True,
                },
                "value": "click_me_123",
                "url": f"https://s3.eu-central-2.wasabisys.com/{S3_BUCKET}/{subfolder}.zip",
                "action_id": "button-action",
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "To view the execution logs follow:",
            },
            "accessory": {
                "type": "button",
                "text": {
                    "type": "plain_text",
                    "text": "Open logs :scroll:",
                    "emoji": True,
                },
                "value": "click_me_124",
                "url": log_link,
                "action_id": "button-action",
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "Please review the contents of the output following the"
                " `ReviewGuide.md` document.",
            },
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "cc <@U02RZR7PDK4>, <@U047B65SPB3>"},
        },
    ]


class SendReport(slack.SlackAPIPostOperator):
    template_fields: Sequence[str] = list(
        slack.SlackAPIPostOperator.template_fields
    ) + ["drive_subfolder", "log_url", "task_state"]

    def __init__(
        self,
        drive_subfolder: str,
        log_url: str,
        task_state: str,
        **kwargs: Any,
    ) -> None:
        self.drive_subfolder = drive_subfolder
        self.log_url = log_url
        self.task_state = task_state
        slack.SlackAPIPostOperator.__init__(
            self,
            channel=SLACK_CHANNEL,
            username="Airflow",
            slack_conn_id=SLACK_CONNECTION_ID,
            task_id="send_report",
            **kwargs,
        )

    def construct_api_call_params(self) -> Any:
        # We must override caller instead of setting the blocks in the constructor
        # because the Airflow machinery needs to render the templates passed as
        # parameters set in the class instance durign construction.
        super().construct_api_call_params()
        assert isinstance(self.api_params, dict)  # appease the type checker gods
        self.api_params["blocks"] = json.dumps(
            format_slack_payload(
                self.drive_subfolder, self.log_url, self.task_state == "success"
            )
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

        configuration["scenario"] = f"./data/cluster_scenarios/{self.scenario}.json"
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

        scenario = Path(configuration["scenario"])
        if scenario.is_dir():
            scenario_files = [scenario]
        else:
            scenario_files = sorted(scenario.glob("*.json"))

        files: list[str | Path] = [
            configuration["nodes_file"],
            configuration["topology_file"],
            configuration["node_pipeline_file"],
            configuration["blacklist_file"],
            config,
        ]

        files.extend(scenario_files)

        for file in files:
            path = scratch_folder / file
            shutil.copyfile(path, inputs / path.name)

    def upload_outputs(self, scratch_folder: Path) -> None:
        self.log.info("Uploading inputs and outputs.")

        output = scratch_folder / "output"
        with tempfile.TemporaryDirectory() as tmpdir:
            zip_path = Path(tmpdir) / "output.zip"
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                for file in output.rglob("*"):
                    if file.is_file():
                        # Write file to zip with relative path inside "output" folder
                        zipf.write(file, arcname=file.relative_to(output))

            # Upload the zip to Wasabi
            hook = S3Hook(aws_conn_id=S3_CONNECTION_ID)
            zip_key = f"{self.drive_subfolder}.zip"
            hook.load_file(
                filename=str(zip_path),
                key=zip_key,
                bucket_name=S3_BUCKET,
                replace=False,
                acl_policy="public-read",
            )
