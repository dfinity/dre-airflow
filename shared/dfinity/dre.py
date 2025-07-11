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
from typing import IO, Generator, TypedDict, cast

import requests

import airflow.models
import dfinity.ic_types as ic_types
import dfinity.rollout_types as rollout_types
from airflow.exceptions import AirflowException
from airflow.hooks.subprocess import SubprocessHook, SubprocessResult

DRE_URL = (
    "https://github.com/dfinity/dre/releases/latest/download/dre-x86_64-unknown-linux"
)
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


type PossibleSubnetId = rollout_types.SubnetId | None


class RegistryNode(TypedDict):
    node_id: rollout_types.NodeId
    # xnet
    # http
    node_operator_id: rollout_types.NodeOperatorId
    #   "chip_id": null,
    hostos_version_id: rollout_types.HostOsVersion
    #   "public_ipv4_config": null,
    subnet_id: PossibleSubnetId
    dc_id: rollout_types.DCId
    node_provider_id: rollout_types.NodeProviderId
    status: rollout_types.NodeStatus
    #  "node_type": "type1.1"


class RegistrySubnet(TypedDict):
    subnet_id: PossibleSubnetId
    membership: list[rollout_types.NodeId]
    nodes: dict[rollout_types.NodeId, RegistryNode]
    #  "max_ingress_bytes_per_message": 2097152,
    #  "max_ingress_messages_per_block": 1000,
    #  "max_block_payload_size": 4194304,
    #  "unit_delay_millis": 1000,
    #  "initial_notary_delay_millis": 300,
    #  "replica_version_id": "e915efecc8af90993ccfc499721ebe826aadba60",
    #  "dkg_interval_length": 499,
    #  "dkg_dealings_per_block": 1,
    #  "start_as_nns": false,
    #  "subnet_type": "application",
    #  "features": {
    #    "canister_sandboxing": false,
    #    "http_requests": true,
    #    "sev_enabled": null
    #  },
    #  "max_number_of_canisters": 120000,
    #  "ssh_readonly_access": [],
    #  "ssh_backup_access": [],
    #  "is_halted": false,
    #  "halt_at_cup_height": false,
    #  "chain_key_config": null


class RegistryDC(TypedDict):
    id: rollout_types.DCId
    region: str
    owner: str
    # gps: {
    #   "latitude": 51.219398498535156,
    #   "longitude": 4.402500152587891
    # }


class RegistryNodeOperatorComputedValues(TypedDict):
    node_provider_name: str
    #    "node_provider_name": "Illusions In Art (Pty) Ltd",
    #    "node_allowance_remaining": 0,
    #    "node_allowance_total": 6,
    #    "total_up_nodes": 6,
    nodes_health: dict[rollout_types.NodeStatus, list[rollout_types.NodeId]]
    #    "max_rewardable_count": {
    #      "type3.1": 6
    #    },
    #    "nodes_in_subnets": 4,
    #    "nodes_in_registry": 6


class RegistryNodeOperator(TypedDict):
    node_operator_principal_id: rollout_types.NodeOperatorId
    node_provider_principal_id: rollout_types.NodeProviderId
    dc_id: rollout_types.DCId
    #  "rewardable_nodes": {
    #    "type3.1": 6
    #  },
    #  "node_allowance": 0,
    #  "ipv6": "",
    computed: RegistryNodeOperatorComputedValues


class RegistryNodeProvider(TypedDict):
    name: str
    principal: rollout_types.NodeProviderId
    #   "reward_account": "",
    #  "total_nodes": 2,
    #  "nodes_in_subnet": 0,
    #  "nodes_per_dc": {
    #    "zh5": 2
    #  }


class RegistrySnapshot(TypedDict):
    subnets: list[RegistrySubnet]
    nodes: list[RegistryNode]
    # unassigned_nodes_config
    dcs: list[RegistryDC]
    node_operators: list[RegistryNodeOperator]
    # node_rewards_table
    # api_bns
    # elected_guest_os_versions
    # elected_host_os_versions
    node_providers: list[RegistryNodeProvider]


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
                    try:
                        pos = cmd.index("propose")
                        cmd.insert(pos + 1, "--dry-run")
                    except ValueError:
                        cmd.append("--dry-run")
                if yes and not dry_run:
                    # In dry-run mode, this kicks in, but cmd.index raises
                    # ValueError when it cannot find the value.
                    try:
                        pos = cmd.index("propose")
                        cmd.insert(pos + 1, "--yes")
                    except ValueError:
                        cmd.append("--yes")

                print("::group::DRE output")
                env = os.environ.copy()
                env["RUST_BACKTRACE"] = "1"
                if full_stdout:
                    with tempfile.NamedTemporaryFile(mode="r") as f:
                        cmd = ["bash", "-c", shlex.join(cmd) + " > " + f.name]
                        r = self.subprocess_hook.run_command(cmd, env=env)
                        f.seek(0)
                        data = f.read()
                        result = SubprocessResult(r.exit_code, data)
                else:
                    result = self.subprocess_hook.run_command(cmd, env=env)
                print("::endgroup::")

                return result

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
            d["proposal_id"] = int(d["id"])
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
    ) -> list[ic_types.AbbrevSubnetUpdateProposal]:
        return [
            r
            for r in self.get_ic_os_version_deployment_proposals_for_subnet(
                subnet_id,
                limit=limit,
            )
            if r["payload"]["replica_version_id"] == git_revision
        ]

    def get_ic_os_version_deployment_proposals_for_subnet(
        self,
        subnet_id: str,
        limit: int = 1000,
    ) -> list[ic_types.AbbrevSubnetUpdateProposal]:
        return [
            cast(ic_types.AbbrevSubnetUpdateProposal, r)
            for r in self.get_proposals(
                topic=ic_types.ProposalTopic.TOPIC_IC_OS_VERSION_DEPLOYMENT,
                limit=limit,
            )
            if r["payload"].get("subnet_id") == subnet_id
            and r["payload"].get("replica_version_id") is not None
        ]

    def get_registry(
        self,
    ) -> RegistrySnapshot:
        r = self.run("registry", full_stdout=True)
        if r.exit_code != 0:
            raise AirflowException("dre exited with status code %d", r.exit_code)
        data = json.loads(r.output)
        return cast(RegistrySnapshot, data)

    def get_ic_os_version_deployment_proposals_for_boundary_nodes_and_revision(
        self,
        git_revision: str,
        api_boundary_node_ids: list[str],
        limit: int = 1000,
    ) -> list[ic_types.AbbrevApiBoundaryNodesUpdateProposal]:
        return [
            r
            for r in self.get_ic_os_version_deployment_proposals_for_api_boundary_nodes(
                api_boundary_node_ids,
                limit=limit,
            )
            if r["payload"]["version"] == git_revision
        ]

    def get_ic_os_version_deployment_proposals_for_api_boundary_nodes(
        self,
        api_boundary_node_ids: list[str],
        limit: int = 1000,
    ) -> list[ic_types.AbbrevApiBoundaryNodesUpdateProposal]:
        return [
            cast(ic_types.AbbrevApiBoundaryNodesUpdateProposal, r)
            for r in self.get_proposals(
                topic=ic_types.ProposalTopic.TOPIC_IC_OS_VERSION_DEPLOYMENT,
                limit=limit,
            )
            if all(x in r["payload"].get("node_ids", []) for x in api_boundary_node_ids)
            and r["payload"].get("version") is not None
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
            "--forum-post-link=omit",
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
            "--forum-post-link=omit",
            "deploy-guestos-to-all-subnet-nodes",
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
        try:
            return int(r.output.rstrip().splitlines()[-1].split()[1])
        except ValueError:
            raise AirflowException(
                f"dre failed to print the proposal number in "
                f"its standard output: {r.output.rstrip()}"
            )

    def propose_to_update_api_boundary_nodes_version(
        self,
        api_boundary_node_ids: list[str],
        git_revision: str,
        dry_run: bool = False,
    ) -> int:
        """
        Create proposal to update some API boundary nodes.

        Args:
        * dry_run: if true, tell ic-admin to only simulate the proposal.

        Returns:
        The proposal number as integer.
        In dry-run mode, the returned proposal number will be FAKE_PROPOSAL_NUMBER.

        On failure, raises AirflowException.
        """
        git_revision_short = git_revision[:7]
        proposal_title = (
            f"Update {len(api_boundary_node_ids)} API boundary node(s)"
            f" to replica version {git_revision_short}"
        )
        proposal_summary = (
            f"""Update API boundary nodes to GuestOS version """
            f"""[{git_revision}]({self.network.release_display_url}/{git_revision})"""
            f"""\n\nMotivation: update the API boundary nodes"""
            f""" {", ".join(api_boundary_node_ids)}."""
        )
        nodesparms: list[str] = []
        for n in api_boundary_node_ids:
            nodesparms.append("--nodes")
            nodesparms.append(n)

        r = self.run(
            "propose",
            "--forum-post-link=omit",
            "deploy-guestos-to-some-api-boundary-nodes",
            "--proposal-title",
            proposal_title,
            "--summary",
            proposal_summary,
            "--version",
            git_revision,
            *nodesparms,
            dry_run=dry_run,
            yes=not dry_run,
        )
        if r.exit_code != 0:
            raise AirflowException("dre exited with status code %d", r.exit_code)
        if dry_run:
            return FAKE_PROPOSAL_NUMBER
        try:
            return int(r.output.rstrip().splitlines()[-1].split()[1])
        except ValueError:
            raise AirflowException(
                f"dre failed to print the proposal number in "
                f"its standard output: {r.output.rstrip()}"
            )


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
    p2 = d.get_proposals(
        limit=1, topic=ic_types.ProposalTopic.TOPIC_IC_OS_VERSION_ELECTION
    )

    pprint.pprint(p)
    print(len(p))
    pprint.pprint(p2)
    print(len(p2))
    # p = DRE(network, SubprocessHook()).upgrade_unassigned_nodes(dry_run=True)
    # print("Stdout", p.output)
    # print("Return code:", p.exit_code)
