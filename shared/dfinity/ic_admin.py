"""
ic-admin proxy and downloader.
"""

import fcntl
import glob
import os
import subprocess
import zlib
from contextlib import contextmanager
from typing import IO, Any, Generator, cast

import requests  # type:ignore
import yaml  # type: ignore

GOVERNANCE_CANISTER_VERSION_URL = "https://dashboard.internal.dfinity.network/api/proxy/registry/mainnet/canisters/governance/version"
IC_ADMIN_GZ_URL = (
    "https://download.dfinity.systems/ic/%(version)s/binaries/%(platform)s/ic-admin.gz"
)
NNS_URL = "https://ic0.app"


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
    *args: str, ic_admin_version: str | None = None, **kwargs: Any
) -> subprocess.CompletedProcess[str]:
    """Run ic-admin, potentially downloading it if not present."""
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
            if ic_admins := glob.glob(os.path.join(d, "ic-admin.*")):
                icapath = ic_admins[0]
            else:
                vr = requests.get(GOVERNANCE_CANISTER_VERSION_URL)
                vr.raise_for_status()
                ic_admin_version = vr.json()["stringified_hash"]
                icapath = os.path.join(d, f"ic-admin.{ic_admin_version}")
        else:
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
        nnsurl = ["--nns-url", NNS_URL]
        return subprocess.run([icapath] + nnsurl + list(args), **kwargs)


def get_subnet_list(ic_admin_version: str | None = None) -> list[str]:
    listp = ic_admin("get-subnet-list", capture_output=True, check=True)
    return cast(list[str], yaml.safe_load(listp.stdout))
