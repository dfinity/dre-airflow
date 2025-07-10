import datetime
import json
import pprint
import random
import shlex
import shutil
import tempfile
import time
from logging import Logger
from pathlib import Path
from subprocess import CalledProcessError
from typing import Any, Callable, Sequence, cast

import requests
from dfinity.ic_os_rollout import SLACK_CHANNEL, SLACK_CONNECTION_ID

import airflow.providers.slack.operators.slack as slack
from airflow.hooks.base import BaseHook
from airflow.hooks.subprocess import SubprocessHook, SubprocessResult
from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.suite.hooks.drive import GoogleDriveHook

REPO_DIR = Path("/tmp/target_topology")
GOOGLE_DRIVE_FOLDER = "1FuEIL4qKMxPqpNqxEwR9zAKxPOF5waqF"
GITHUB_CONNECTION_ID = "github.node_allocation"
GOOGLE_CONNECTION_ID = "google_cloud_default"


def format_slack_payload(drive_subfolder: str, log_link: str) -> list[object]:
    now = datetime.datetime.now()
    formatted_dt = now.strftime("%A, %d %B %Y")
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
                "text": "The run of the target topology tool for today is complete!",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"To view the artifacts for the run"
                f" open folder {drive_subfolder} in",
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
    ) + ["drive_subfolder", "log_url"]

    def __init__(
        self,
        drive_subfolder: str,
        log_url: str,
        **kwargs: Any,
    ) -> None:
        self.drive_subfolder = drive_subfolder
        self.log_url = log_url
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
            format_slack_payload(self.drive_subfolder, self.log_url)
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
        hook = GoogleDriveHook(gcp_conn_id=GOOGLE_CONNECTION_ID)

        output = scratch_folder / "output"
        for file in output.rglob("*"):
            if not file.is_file():
                continue
            dest_file = Path(self.drive_subfolder) / file.relative_to(output)
            run_with_backoff(
                hook.upload_file,
                args=(str(file), str(dest_file)),
                kwargs={"folder_id": self.folder_id},
            )


def run_with_backoff(
    func: Callable,
    args: tuple = (),
    kwargs: dict = {},
    *,
    max_retries: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    logger: Logger = None,
) -> Any:
    attempt = 0

    while True:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            attempt += 1
            if attempt > max_retries:
                if logger:
                    logger.error(f"Exceeded {max_retries} retries. Raising exception.")
                raise
            delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
            jitter = random.uniform(0, delay / 2)
            total_delay = delay + jitter
            if logger:
                logger.warning(
                    f"Retry {attempt}/{max_retries} in {total_delay:.2f}s due to: {e}"
                )
            time.sleep(total_delay)
