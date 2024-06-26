"""
DRE utility proxy and downloader.
"""

import fcntl
import json
import os
import pprint
import re
import shlex
import tempfile
import time
from contextlib import contextmanager
from typing import IO, Generator, cast

import requests

import airflow.models
import dfinity.ic_types as ic_types
from airflow.exceptions import AirflowException
from airflow.hooks.subprocess import SubprocessHook, SubprocessResult

DRE_URL = "https://github.com/dfinity/dre/releases/latest/download/dre"
FAKE_PROPOSAL_NUMBER = -123456


@contextmanager
def locked_open(filename: str, mode: str = "w") -> Generator[IO[str], None, None]:
    """
    Context manager that on entry opens the path `filename`, using `mode`
    (default: `r`), and applies an advisory write lock on the file which
    is released when leaving the context. Yields the open file object for
    use within the context.

    Note: advisory locking implies that all calls to open the file using
    this same api will block for both read and write until the lock is
    acquired. Locking this way will not prevent the file from access using
    any other api/method.
    """
    if "b" in mode:
        raise ValueError("binary not supported by this decorator")
    with open(filename, mode) as fd:
        fcntl.flock(fd, fcntl.LOCK_EX)
        try:
            yield fd
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)


class DRE:

    def _prep(self) -> None:
        d = self.base_dir
        os.makedirs(d, exist_ok=True)

        dre_path = self.dre_path
        if not os.path.exists(dre_path):
            mod_date = 0.0
        else:
            mod_date = os.stat(dre_path).st_mtime
        now = time.time()
        oneweek = 7 * 86400
        if mod_date < now - oneweek:
            r = requests.get(DRE_URL)
            r.raise_for_status()
            dre_data = r.content
            tmp_dre_path = f"{dre_path}.tmp.{now}"
            with open(tmp_dre_path, "wb") as dre_tmp:
                dre_tmp.write(dre_data)
            os.chmod(tmp_dre_path, 0o755)
            os.rename(tmp_dre_path, dre_path)

    def __init__(
        self,
        network: ic_types.ICNetwork,
        subprocess_hook: SubprocessHook,
    ):
        rundir = f"/run/user/{os.getuid()}"
        if os.path.isdir(rundir):
            d = os.path.join(rundir, "dre")
        elif os.getenv("TMPDIR") and os.path.isdir(os.getenv("TMPDIR")):  # type:ignore
            d = f"{os.getenv('TMPDIR')}/.dre.{os.getuid()}"
        elif os.getenv("HOME") and os.path.isdir(os.getenv("HOME")):  # type:ignore
            d = f"{os.getenv('HOME')}/.cache/dre"
        else:
            assert 0, "No suitable location for downloading the DRE tool"
        self.base_dir = d
        self.dre_path = os.path.join(self.base_dir, "dre")
        self.subprocess_hook = subprocess_hook
        self.network = network

    def authenticated(self) -> "AuthenticatedDRE":
        """
        Transforms a DRE into an authenticated DRE.

        May only be called in operator / sensor context."""
        return AuthenticatedDRE(
            ic_types.augment_network_with_private_key(
                self.network,
                airflow.models.Variable.get(
                    self.network.proposer_neuron_private_key_variable_name
                ),
            ),
            self.subprocess_hook,
        )

    def run(
        self,
        *args: str,
        dry_run: bool = False,
        yes: bool = False,
        full_stdout: bool = False,
    ) -> SubprocessResult:
        """
        Run dre, potentially downloading it if not present.

        Args:
        * dry_run: if true, the command will get a --dry-run
          appended at the end.
        * yes: if true, --yes appended at the end, but only if
          dry_run is not true.
        """
        self._prep()
        # Locking to prevent clashes in
        with locked_open(os.path.join(self.base_dir, ".runlock")):
            with tempfile.NamedTemporaryFile(
                "w",
                suffix=".proposal-cert.pem" if not dry_run else ".fake-cert.pem",
            ) as w:
                if isinstance(self, AuthenticatedDRE):
                    w.write(self.network.proposer_neuron_private_key)
                    w.flush()
                nnsurl = ["--nns-urls", self.network.nns_url]
                if isinstance(self, AuthenticatedDRE):
                    pem = ["--private-key-pem", w.name]
                    nid = ["--neuron-id", str(self.network.proposer_neuron_id)]
                else:
                    pem, nid = [], []
                cmd = [self.dre_path] + nnsurl + nid + pem + list(args)
                if dry_run:
                    cmd.append("--dry-run")
                if yes and not dry_run:
                    pos = cmd.index("propose")
                    if pos == -1:
                        cmd.append("--yes")
                    else:
                        cmd.insert(pos + 1, "--yes")
                if full_stdout:
                    with tempfile.NamedTemporaryFile(mode="r") as f:
                        cmd = ["bash", "-c", shlex.join(cmd) + " > " + f.name]
                        r = self.subprocess_hook.run_command(cmd)
                        f.seek(0)
                        data = f.read()
                        return SubprocessResult(r.exit_code, data)
                else:
                    return self.subprocess_hook.run_command(cmd)

    def get_proposals(
        self,
        topic: ic_types.ProposalTopic | None = None,
        limit: int = 1000,
    ) -> list[ic_types.AbbrevProposal]:
        """
        List proposals.

        Returns: a list of AbbrevProposals.
        On failure, raises AirflowException.
        """

        def topic_to_str(t: ic_types.ProposalTopic) -> str:
            return t.name.lower()[6:].replace("_", "-")

        def str_to_topic(t: str) -> ic_types.ProposalTopic:
            return cast(
                ic_types.ProposalTopic,
                getattr(ic_types.ProposalTopic, t),
            )

        def str_to_status(t: str) -> ic_types.ProposalStatus:
            return cast(
                ic_types.ProposalStatus,
                getattr(ic_types.ProposalStatus, t),
            )

        topicdata = ["--topic", topic_to_str(topic)] if topic is not None else []
        limitdata = ["--limit", str(limit)]
        cmd = ["proposals", "filter"] + topicdata + limitdata
        r = self.run(*cmd, full_stdout=True)
        if r.exit_code != 0:
            raise AirflowException("dre exited with status code %d", r.exit_code)
        data = json.loads(r.output)
        results: list[ic_types.AbbrevProposal] = []
        for d in data:
            d["proposal_id"] = d["id"]
            del d["id"]
            d["status"] = str_to_status("PROPOSAL_STATUS_" + d["status"].upper())
            d["topic"] = re.sub("([A-Z])", "_\\1", d["topic"])
            d["topic"] = "TOPIC" + d["topic"].upper()
            d["topic"] = str_to_topic(d["topic"])
            if isinstance(d["payload"], str):
                if not d["payload"]:
                    d["payload"] = {}
                else:
                    try:
                        d["payload"] = json.loads(d["payload"])
                    except json.decoder.JSONDecodeError:
                        assert 0, "Invalid proposal payload: " + pprint.pformat(d)
            results.append(cast(ic_types.AbbrevProposal, d))
        return results

    def get_ic_os_version_deployment_proposals_for_subnet_and_revision(
        self,
        git_revision: str,
        subnet_id: str,
        limit: int = 1000,
    ) -> list[ic_types.AbbrevProposal]:
        return [
            r
            for r in self.get_proposals(
                topic=ic_types.ProposalTopic.TOPIC_IC_OS_VERSION_DEPLOYMENT,
                limit=limit,
            )
            if r["payload"].get("subnet_id") == subnet_id
            and r["payload"].get("replica_version_id") == git_revision
        ]

    def get_subnet_list(self) -> list[str]:
        r = self.run("get", "subnet-list", "--json", full_stdout=True)
        if r.exit_code != 0:
            raise AirflowException("dre exited with status code %d", r.exit_code)
        return cast(list[str], json.loads(r.output))

    def get_blessed_replica_versions(self) -> list[str]:
        r = self.run("get", "blessed-replica-versions", "--json", full_stdout=True)
        if r.exit_code != 0:
            raise AirflowException("dre exited with status code %d", r.exit_code)
        return cast(list[str], json.loads(r.output)["value"]["blessed_version_ids"])

    def is_replica_version_blessed(self, git_revision: str) -> bool:
        return git_revision.lower() in [
            x.lower() for x in self.get_blessed_replica_versions()
        ]


class AuthenticatedDRE(DRE):

    network: ic_types.ICNetworkWithPrivateKey

    def upgrade_unassigned_nodes(
        self,
        dry_run: bool = False,
    ) -> SubprocessResult:
        """
        Create proposal to upgrade unassigned nodes.

        Args:
        * dry_run: if true, tell ic-admin to only simulate the proposal.

        Returns:
        A SubprocessResult with an output attribute containing the last line
        of the combined standard output / standard error of the command, and
        an exit_code denoting the subprocess return code.
        No exception is raised -- caller must check the exit_code attribute
        of the returned object to be non-zero (or whatever expected value it is).
        """
        return self.run(
            "update-unassigned-nodes",
            dry_run=dry_run,
            yes=True,
        )

    def propose_to_update_subnet_replica_version(
        self,
        subnet_id: str,
        git_revision: str,
        dry_run: bool = False,
    ) -> int:
        """
        Create proposal to update subnet to a blessed version.

        Args:
        * dry_run: if true, tell ic-admin to only simulate the proposal.

        Returns:
        The proposal number as integer.
        In dry-run mode, the returned proposal number will be FAKE_PROPOSAL_NUMBER.

        On failure, raises AirflowException.
        """
        subnet_id_short = subnet_id.split("-")[0]
        git_revision_short = git_revision[:7]
        proposal_title = (
            f"Update subnet {subnet_id_short} to replica version {git_revision_short}"
        )
        proposal_summary = (
            f"""Update subnet {subnet_id} to replica version """
            f"""[{git_revision}]({self.network.release_display_url}/{git_revision})
""".strip()
        )
        r = self.run(
            "propose",
            "update-subnet-replica-version",
            "--proposal-title",
            proposal_title,
            "--summary",
            proposal_summary,
            subnet_id,
            git_revision,
            dry_run=dry_run,
            yes=not dry_run,
        )
        if r.exit_code != 0:
            raise AirflowException("dre exited with status code %d", r.exit_code)
        if dry_run:
            return FAKE_PROPOSAL_NUMBER
        return int(r.output.rstrip().splitlines()[-1].split()[1])


if __name__ == "__main__":
    network = ic_types.ICNetworkWithPrivateKey(
        "https://ic0.app/",
        "https://dashboard.internetcomputer.org/proposal",
        "https://dashboard.internetcomputer.org/release",
        ["https://victoria.mainnet.dfinity.network/select/0/prometheus/api/v1/query"],
        80,
        "unused",
        """-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIEFRa42BSz1uuRxWBh60vePDrpkgtELJJMZtkJGlExuLoAoGCCqGSM49
AwEHoUQDQgAEyiUJYA7SI/u2Rf8ouND0Ip46gdjKcGB8Vx3VkajFx5+YhtaMfHb1
5YjfGWFuNLqyxLGGvDUq6HlGsBJ9QIcPtA==
-----END EC PRIVATE KEY-----""",
    )
    d = DRE(network, SubprocessHook())
    p = d.get_ic_os_version_deployment_proposals_for_subnet_and_revision(
        subnet_id="pae4o-o6dxf-xki7q-ezclx-znyd6-fnk6w-vkv5z-5lfwh-xym2i-otrrw-fqe",
        git_revision="ec35ebd252d4ffb151d2cfceba3a86c4fb87c6d6",
    )
    p = d.get_proposals(
        limit=1, topic=ic_types.ProposalTopic.TOPIC_IC_OS_VERSION_ELECTION
    )

    pprint.pprint(p)
    print(len(p))
    # p = DRE(network, SubprocessHook()).upgrade_unassigned_nodes(dry_run=True)
    # print("Stdout", p.output)
    # print("Return code:", p.exit_code)
