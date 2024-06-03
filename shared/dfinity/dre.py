"""
ic-admin proxy and downloader.
"""

import fcntl
import os
import tempfile
import time
from contextlib import contextmanager
from typing import IO, Any, Generator

import requests

import dfinity.ic_types
from airflow.hooks.subprocess import SubprocessHook, SubprocessResult

DRE_URL = "https://github.com/dfinity/dre/releases/latest/download/dre"


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

    def __init__(
        self,
        network: dfinity.ic_types.ICNetworkWithPrivateKey,
        subprocess_hook: SubprocessHook,
    ):
        rundir = f"/run/user/{os.getuid()}"
        if os.path.isdir(rundir):
            d = os.path.join(rundir, "dre")
            self.log.info("Selecting run directory %s for DRE", d)
        elif os.getenv("TMPDIR") and os.path.isdir(os.getenv("TMPDIR")):  # type:ignore
            d = f"{os.getenv('TMPDIR')}/.dre.{os.getuid()}"
            self.log.info("Selecting temporary directory %s for DRE", d)
        elif os.getenv("HOME") and os.path.isdir(os.getenv("HOME")):  # type:ignore
            d = f"{os.getenv('HOME')}/.cache/dre"
            self.log.info("Selecting home cache directory %s for DRE", d)
        else:
            assert 0, "No suitable location for downloading the DRE tool"
        self.base_dir = d
        self.dre_path = os.path.join(self.base_dir, "dre")
        self.subprocess_hook = subprocess_hook
        self.network = network

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

    def run(
        self,
        *args: str,
        dry_run: bool = False,
        yes: bool = False,
        **kwargs: Any,
    ) -> SubprocessResult:
        """
        Run dre, potentially downloading it if not present.

        Args:
        * dry_run: if true, the command will get a --dry-run
          appended at the end.
        * yes: if true, --yes appended at the end, but only if
          dry_run is not true.
        * capture_output: if true, the returned CompletedProcess
          will have the stdout and stderr of the process as
          attributes.
        """
        self._prep()
        # Locking to prevent clashes in
        with locked_open(os.path.join(self.base_dir, ".runlock")):
            with tempfile.NamedTemporaryFile(
                "w",
                suffix=".proposal-cert.pem" if not dry_run else ".fake-cert.pem",
            ) as w:
                w.write(self.network.proposer_neuron_private_key)
                w.flush()
                nnsurl = ["--nns-urls", self.network.nns_url]
                pem = ["--private-key-pem", w.name]
                nid = ["--neuron-id", str(self.network.proposer_neuron_id)]
                cmd = [self.dre_path] + nnsurl + nid + pem + list(args)
                if dry_run:
                    cmd.append("--dry-run")
                if yes and not dry_run:
                    cmd.append("--yes")
                return self.subprocess_hook.run_command(cmd, **kwargs)

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


if __name__ == "__main__":
    network = dfinity.ic_types.ICNetworkWithPrivateKey(
        "https://ic0.app/",
        "https://ic-api.internetcomputer.org/api/v3/proposals",
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
    p = DRE(network, SubprocessHook()).upgrade_unassigned_nodes(dry_run=True)
    print("Stdout", p.output)
    print("Return code:", p.exit_code)
