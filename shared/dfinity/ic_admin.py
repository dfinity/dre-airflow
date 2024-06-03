"""
ic-admin proxy and downloader.
"""

import fcntl
import glob
import os
import subprocess
import tempfile
import zlib
from contextlib import contextmanager
from typing import IO, Any, Generator, cast

import requests
import yaml

import dfinity.ic_types

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
    network: dfinity.ic_types.ICNetwork,
    ic_admin_version: str | None = None,
    dry_run: bool = False,
    **kwargs: Any,
) -> subprocess.CompletedProcess[str]:
    """
    Run ic-admin, potentially downloading it if not present.

    Args:
    * dry_run: if true, the command will get a --dry-run
      appended at the end.
    * capture_output: if true, the returned CompletedProcess
      will have the stdout and stderr of the process as
      attributes.
    """
    rundir = f"/run/user/{os.getuid()}"
    if os.path.isdir(rundir):
        d = os.path.join(rundir, "ic_admin")
    elif os.getenv("TMPDIR") and os.path.isdir(os.getenv("TMPDIR")):  # type:ignore
        d = f"{os.getenv('TMPDIR')}/.ic_admin.{os.getuid()}"
    elif os.getenv("HOME") and os.path.isdir(os.getenv("HOME")):  # type:ignore
        d = f"{os.getenv('HOME')}/.cache/ic_admin"
    else:
        assert 0, "No prudent location for downloaded DRE tool"

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
            cmd.append("--dry-run")
        return subprocess.run(cmd, **kwargs)


def get_subnet_list(
    network: dfinity.ic_types.ICNetwork,
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
    network: dfinity.ic_types.ICNetworkWithPrivateKey,
    ic_admin_version: str | None,
    dry_run: bool = False,
) -> subprocess.CompletedProcess[str]:
    """
    Create proposal to update a subnet to a specific git revision.

    Args:
    * subnet_id: which subnet to upgrade.
    * git_revision: which git revision to upgrade to.
    * proposer_neuron_id: the number of the neuron to use for proposal.
    * proposer_neuron_pem: the path to the PEM file containing the private
      key material for the neuron used to propose.
    * dry_run: if true, tell ic-admin to only simulate the proposal.

    Returns:
      A CompletedProcess with stdout and stderr attributes to read from.
    """
    subnet_id_short = subnet_id.split("-")[0]
    git_revision_short = git_revision[:7]
    proposal_title = (
        f"Update subnet {subnet_id_short} to replica version {git_revision_short}"
    )
    proposal_summary = (
        f"""Update subnet {subnet_id} to replica version """
        f"""[{git_revision}]({network.release_display_url}/{git_revision})
""".strip()
    )
    with tempfile.NamedTemporaryFile(
        "w",
        suffix=".proposal-cert.pem" if not dry_run else ".fake-proposal-cert.pem",
    ) as w:
        w.write(network.proposer_neuron_private_key)
        w.flush()
        return ic_admin(
            "-s",
            w.name,
            "propose-to-update-subnet-replica-version",
            "--proposal-title",
            proposal_title,
            "--summary",
            proposal_summary,
            "--proposer",
            str(network.proposer_neuron_id),
            subnet_id,
            git_revision,
            network=network,
            ic_admin_version=ic_admin_version,
            dry_run=dry_run,
            check=True,
            capture_output=True,
            input="y\n",
        )


def get_blessed_replica_versions(
    network: dfinity.ic_types.ICNetwork,
    ic_admin_version: str | None = None,
) -> list[str]:
    listp = ic_admin(
        "--json",
        "get-blessed-replica-versions",
        network=network,
        capture_output=True,
        check=True,
        ic_admin_version=ic_admin_version,
    )
    return cast(list[str], yaml.safe_load(listp.stdout)["value"]["blessed_version_ids"])


def is_replica_version_blessed(
    git_revision: str,
    network: dfinity.ic_types.ICNetwork,
    ic_admin_version: str | None = None,
) -> bool:
    return git_revision.lower() in [
        x.lower()
        for x in get_blessed_replica_versions(
            network=network, ic_admin_version=ic_admin_version
        )
    ]


if __name__ == "__main__":
    print(
        is_replica_version_blessed(
            "b314222935b7d06c70036b0b54aa80a33252d79c",
            dfinity.ic_types.ICNetwork(
                "https://ic0.app/",
                "https://ic-api.internetcomputer.org/api/v3/proposals",
                "https://dashboard.internetcomputer.org/proposal",
                "https://dashboard.internetcomputer.org/release",
                [
                    "https://victoria.mainnet.dfinity.network/select/0/prometheus/api/v1/query",
                ],
                80,
                "unused",
            ),
        )
    )
    import sys

    sys.exit(0)
    try:
        p = propose_to_update_subnet_replica_version(
            "qn2sv-gibnj-5jrdq-3irkq-ozzdo-ri5dn-dynlb-xgk6d-kiq7w-cvop5-uae",
            "0000000000000000000000000000000000000000",
            dfinity.ic_types.ICNetworkWithPrivateKey(
                "https://ic0.app/",
                "https://ic-api.internetcomputer.org/api/v3/proposals",
                "https://dashboard.internetcomputer.org/proposal",
                "https://dashboard.internetcomputer.org/release",
                [
                    "https://victoria.mainnet.dfinity.network/select/0/prometheus/api/v1/query"
                ],
                80,
                "unused",
                """-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIEFRa42BSz1uuRxWBh60vePDrpkgtELJJMZtkJGlExuLoAoGCCqGSM49
AwEHoUQDQgAEyiUJYA7SI/u2Rf8ouND0Ip46gdjKcGB8Vx3VkajFx5+YhtaMfHb1
5YjfGWFuNLqyxLGGvDUq6HlGsBJ9QIcPtA==
-----END EC PRIVATE KEY-----""",
            ),
            None,
            True,
        )
        print("Stdout", p.stdout)
        print("Stderr", p.stderr)
    except subprocess.CalledProcessError as exc:
        print("Failure return code:", exc.returncode)
        print("Stdout", exc.stdout)
        print("Stderr", exc.stderr)
