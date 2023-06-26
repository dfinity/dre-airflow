"""
ic-admin proxy and downloader.
"""

import fcntl
import glob
import os
import shlex
import subprocess
import sys
import zlib
from contextlib import contextmanager
from typing import IO, Any, Generator, cast

import dfinity.ic_types as ic_types
import requests  # type:ignore
import yaml  # type: ignore

GOVERNANCE_CANISTER_VERSION_URL = "https://dashboard.internal.dfinity.network/api/proxy/registry/mainnet/canisters/governance/version"
IC_ADMIN_GZ_URL = (
    "https://download.dfinity.systems/ic/%(version)s/binaries/%(platform)s/ic-admin.gz"
)


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


def ic_admin(
    *args: str,
    network: ic_types.ICNetwork,
    ic_admin_version: str | None = None,
    dry_run: bool = False,
    **kwargs: Any,
) -> subprocess.CompletedProcess[str]:
    """
    Run ic-admin, potentially downloading it if not present.

    Args:
    * dry_run: if true, the command will be echoed on standard error,
      but it won't be executed.
    """
    rundir = f"/run/user/{os.getuid()}"
    if os.path.isdir(rundir):
        d = os.path.join(rundir, "ic_admin")
    elif os.getenv("TMPDIR") and os.path.isdir(os.getenv("TMPDIR")):  # type:ignore
        d = f"{os.getenv('TMPDIR')}/.ic_admin.{os.getuid()}"
    elif os.getenv("HOME") and os.path.isdir(os.getenv("HOME")):  # type:ignore
        d = f"{os.getenv('TMPDIR')}/.cache/ic_admin"

    os.makedirs(d, exist_ok=True)
    with locked_open(os.path.join(d, ".oplock")):
        if ic_admin_version is None:
            ic_admins = glob.glob(os.path.join(d, "ic-admin.*"))
            if ic_admins:
                icapath = ic_admins[0]
            else:
                vr = requests.get(GOVERNANCE_CANISTER_VERSION_URL)
                vr.raise_for_status()
                ic_admin_version = vr.json()["stringified_hash"].strip()
                icapath = os.path.join(d, f"ic-admin.{ic_admin_version}")
        else:
            ic_admin_version = ic_admin_version.strip()
            icapath = os.path.join(d, f"ic-admin.{ic_admin_version}")
        if not os.path.exists(icapath):
            platform = "x86_64-linux"
            ic_admin_gz_url = IC_ADMIN_GZ_URL % {
                "platform": platform,
                "version": ic_admin_version,
            }
            r = requests.get(ic_admin_gz_url)
            r.raise_for_status()
            ic_admin_gz = r.content
            ungzipped_data = zlib.decompress(ic_admin_gz, 15 + 32)
            with open(icapath, "wb") as ic_admin_file:
                ic_admin_file.write(ungzipped_data)
            os.chmod(icapath, 0o755)
        kwargs["text"] = True
        nnsurl = ["--nns-url", network.nns_url]
        cmd = [icapath] + nnsurl + list(args)
        if dry_run:
            print(" ".join(shlex.quote(x) for x in cmd), file=sys.stderr)
            return subprocess.CompletedProcess(cmd, 0, None, None)
        return subprocess.run([icapath] + nnsurl + list(args), **kwargs)


def get_subnet_list(
    network: ic_types.ICNetwork,
    ic_admin_version: str | None = None,
) -> list[str]:
    listp = ic_admin(
        "get-subnet-list",
        network=network,
        capture_output=True,
        check=True,
        ic_admin_version=ic_admin_version,
    )
    return cast(list[str], yaml.safe_load(listp.stdout))


def propose_to_update_subnet_replica_version(
    subnet_id: str,
    git_revision: str,
    proposer_neuron_id: int,
    proposer_neuron_pem: str,
    network: ic_types.ICNetwork,
    ic_admin_version: str | None = None,
    dry_run: bool = False,
) -> None:
    """
    Create proposal to update a subnet to a specific git revision.

    Args:
    * subnet_id: which subnet to upgrade.
    * git_revision: which git revision to upgrade to.
    * proposer_neuron_id: the number of the neuron to use for proposal.
    * proposer_neuron_pem: the path to the PEM file containing the private
      key material for the neuron used to propose.
    * dry_run: if true, print the command that would be run to standard
      error, but do not run it.
    """
    subnet_id_short = subnet_id.split("-")[0]
    git_revision_short = git_revision[:7]
    proposal_title = (
        f"Update subnet {subnet_id_short} to replica version {git_revision_short}"
    )
    proposal_summary = (
        f"""Update subnet {subnet_id} to replica version """
        """[{git_revision}]("{network.release_display_url}/{git_revision})
""".strip()
    )
    ic_admin(
        "-s",
        proposer_neuron_pem,
        "propose-to-update-subnet-replica-version",
        "--proposal-title",
        proposal_title,
        "--summary",
        proposal_summary,
        "--proposer",
        str(proposer_neuron_id),
        subnet_id,
        git_revision,
        network=network,
        ic_admin_version=ic_admin_version,
        dry_run=dry_run,
        check=True,
        capture_output=True,
        input="y\n",
    )
