import datetime
from dataclasses import dataclass
from enum import Enum
from typing import Optional, TypedDict


@dataclass
class SubnetRolloutInstance:
    start_at: datetime.datetime
    subnet_num: int
    subnet_id: str
    git_revision: Optional[str] = None


@dataclass
class SubnetRolloutInstanceWithRevision:
    start_at: datetime.datetime
    subnet_num: int
    subnet_id: str
    git_revision: str


@dataclass
class ICNetwork:
    nns_url: str
    proposal_display_url: str
    release_display_url: str
    prometheus_urls: list[str]
    # The neuron ID to use for proposals.
    proposer_neuron_id: int
    # The name of the Airflow variable containing the key material
    # for the proposer neuron.
    proposer_neuron_private_key_variable_name: str


@dataclass
class ICNetworkWithPrivateKey(ICNetwork):
    # The contents of the private key for the proposer neuron.
    proposer_neuron_private_key: str


def augment_network_with_private_key(
    network: ICNetwork, private_key: str
) -> ICNetworkWithPrivateKey:
    return ICNetworkWithPrivateKey(
        proposer_neuron_private_key=private_key,
        nns_url=network.nns_url,
        proposal_display_url=network.proposal_display_url,
        release_display_url=network.release_display_url,
        prometheus_urls=network.prometheus_urls,
        proposer_neuron_id=network.proposer_neuron_id,
        proposer_neuron_private_key_variable_name=network.proposer_neuron_private_key_variable_name,
    )


IC_NETWORKS: dict[str, ICNetwork] = {
    "mainnet": ICNetwork(
        "https://ic0.app/",
        "https://dashboard.internetcomputer.org/proposal",
        "https://dashboard.internetcomputer.org/release",
        [
            "https://victoria.mainnet.dfinity.network/select/0/prometheus/api/v1/query",
        ],
        80,
        "dfinity.ic_admin.mainnet.proposer_key_file",
    ),
    # "staging": ic_types.ICNetwork("http://[2600:3004:1200:1200:5000:11ff:fe37:c55d]:8080/"),
    # FIXME we do not have a proposals URL for staging
}


class ProposalStatus(Enum):
    ## Weird proposal status not mentioned in the proto.
    PROPOSAL_STATUS_UNKNOWN = -1

    ## Unspecified status.
    PROPOSAL_STATUS_UNSPECIFIED = 0

    ## A decision (adopt/reject) has yet to be made.
    PROPOSAL_STATUS_OPEN = 1

    ## The proposal has been rejected.
    PROPOSAL_STATUS_REJECTED = 2

    ## The proposal has been adopted (sometimes also called
    ## "accepted"). At this time, either execution as not yet started,
    ## or it has but the outcome is not yet known.
    PROPOSAL_STATUS_ADOPTED = 3

    ## The proposal was adopted and successfully executed.
    PROPOSAL_STATUS_EXECUTED = 4

    ## The proposal was adopted, but execution failed.
    PROPOSAL_STATUS_FAILED = 5


class ProposalTopic(Enum):
    ## The `Unspecified` topic is used as a fallback when
    ## following. That is, if no followees are specified for a given
    ## topic (other than the "Governance" and "SNS & Neurons' Fund" topics),
    ## the followees for this topic are used instead.
    TOPIC_UNSPECIFIED = 0
    ## A special topic by means of which a neuron can be managed by the
    ## followees for this topic (in this case, there is no fallback to
    ## 'unspecified'). Votes on this topic are not included in the
    ## voting history of the neuron (cf., `recent_ballots` in `Neuron`).
    ##
    ## For proposals on this topic, only followees on the 'neuron
    ## management' topic of the neuron that the proposals pertains to
    ## are allowed to vote.
    ##
    ## As the set of eligible voters on this topic is restricted,
    ## proposals on this topic have a *short voting period*.
    TOPIC_NEURON_MANAGEMENT = 1
    ## All proposals that provide “real time” information about the
    ## value of ICP, as measured by an IMF SDR, which allows the NNS to
    ## convert ICP to cycles (which power computation) at a rate which
    ## keeps their real world cost constant. Votes on this topic are not
    ## included in the voting history of the neuron (cf.,
    ## `recent_ballots` in `Neuron`).
    ##
    ## Proposals on this topic have a *short voting period* due to their
    ## frequency.
    TOPIC_EXCHANGE_RATE = 2
    ## All proposals that administer network economics, for example,
    ## determining what rewards should be paid to node operators.
    TOPIC_NETWORK_ECONOMICS = 3
    ## All proposals that administer governance, for example to freeze
    ## malicious canisters that are harming the network.
    TOPIC_GOVERNANCE = 4
    ## All proposals that administer node machines, including, but not
    ## limited to, upgrading or configuring the OS, upgrading or
    ## configuring the virtual machine framework and upgrading or
    ## configuring the node replica software.
    TOPIC_NODE_ADMIN = 5
    ## All proposals that administer network participants, for example,
    ## granting and revoking DCIDs (data center identities) or NOIDs
    ## (node operator identities).
    TOPIC_PARTICIPANT_MANAGEMENT = 6
    ## All proposals that administer network subnets, for example
    ## creating new subnets, adding and removing subnet nodes, and
    ## splitting subnets.
    TOPIC_SUBNET_MANAGEMENT = 7
    ## All proposals to manage NNS-controlled canisters not covered by other topics (Protocol Canister
    ## Management or Service Nervous System Management).
    TOPIC_APPLICATION_CANISTER_MANAGEMENT = 8
    ## Proposals that update KYC information for regulatory purposes,
    ## for example during the initial Genesis distribution of ICP in the
    ## form of neurons.
    TOPIC_KYC = 9
    ## Topic for proposals to reward node providers.
    TOPIC_NODE_PROVIDER_REWARDS = 10

    ## IC OS upgrade proposals
    ## -----------------------
    ## ICP runs on a distributed network of nodes grouped into subnets. Each node runs a stack of
    ## operating systems, including HostOS (runs on bare metal) and GuestOS (runs inside HostOS;
    ## contains, e.g., the ICP replica process). HostOS and GuestOS are distributed via separate disk
    ## images. The umbrella term IC OS refers to the whole stack.
    ##
    ## The IC OS upgrade process involves two phases, where the first phase is the election of a new
    ## IC OS version and the second phase is the deployment of a previously elected IC OS version on
    ## all nodes of a subnet or on some number of nodes (including nodes comprising subnets and
    ## unassigned nodes).
    ##
    ## A special case is for API boundary nodes, special nodes that route API requests to a replica
    ## of the right subnet. API boundary nodes run a different process than the replica, but their
    ## executable is distributed via the same disk image as GuestOS. Therefore, electing a new GuestOS
    ## version also results in a new version of boundary node software being elected.
    ##
    ## Proposals handling the deployment of IC OS to some nodes. It is possible to deploy only
    ## the versions of IC OS that are in the set of elected IC OS versions.
    TOPIC_IC_OS_VERSION_DEPLOYMENT = 12
    ## Proposals for changing the set of elected IC OS versions.
    TOPIC_IC_OS_VERSION_ELECTION = 13

    ## Proposals related to SNS and Community Fund.
    TOPIC_SNS_AND_COMMUNITY_FUND = 14
    ## Proposals related to the management of API Boundary Nodes
    TOPIC_API_BOUNDARY_NODE_MANAGEMENT = 15

    ## Proposals related to subnet rental.
    TOPIC_SUBNET_RENTAL = 16

    ## All proposals to manage protocol canisters, which are considered part of the ICP protocol and
    ## are essential for its proper functioning.
    TOPIC_PROTOCOL_CANISTER_MANAGEMENT = 17

    ## All proposals to manage the canisters of service nervous systems (SNS), including upgrading
    ## relevant canisters and managing SNS framework canister WASMs through SNS-W.
    TOPIC_SERVICE_NERVOUS_SYSTEM_MANAGEMENT = 18


class AbbrevProposal(TypedDict):
    """
    {'proposal_id': 102768,
    'payload': {'replica_version_id': 'b3b00ba59c366384e3e0cd53a69457e9053ec987',
                'subnet_id': ('4zbus-z2bmt-ilreg-'
                    'xakz4-6tyre-hsqj4-slb4g-zjwqo-snjcc-iqphi-3qe')},
    'proposer': '80',
    'status': 'EXECUTED',
    'summary': 'Update subnet '
                '4zbus-z2bmt-ilreg-xakz4-6tyre-hsqj4-slb4g-zjwqo-snjcc-iqphi-3qe '
                'to replica version '
                '[b3b00ba59c366384e3e0cd53a69457e9053ec987](https:##dashboard.internetcomputer.org/release/b3b00ba59c366384e3e0cd53a69457e9053ec987)\n',
    'title': 'Update subnet 4zbus to replica version b3b00ba5',
    'topic': 'TOPIC_IC_OS_VERSION_DEPLOYMENT'}

    could also be

    {'proposal_id': 102768,
    'payload': {'version': 'b3b00ba59c366384e3e0cd53a69457e9053ec987',
                'node_ids': [
                '4fssn-4vi43-2qufr-hlrfz-hfohd-jgrwc-7l7ok-uatwb-ukau7-lwmoz-tae'
                ]},
    'proposer': '80',
    'status': 'EXECUTED',
    'summary': 'Update 1 API boundary node(s) to '
                'f8131bfbc2d339716a9cff06e04de49a68e5a80b '
                '\n'nMotivation...',
    'title': 'Update 1 API boundary node(s) to f8131bf',
    'topic': 'TOPIC_IC_OS_VERSION_DEPLOYMENT'}
    """

    payload: dict[str, str]
    proposal_id: int
    proposer: int
    status: ProposalStatus
    summary: str
    title: str
    topic: ProposalTopic


class AbbrevSubnetUpdateProposalPayload(TypedDict):
    replica_version_id: str
    subnet_id: str


class AbbrevSubnetUpdateProposal(TypedDict):
    """
    {'proposal_id': 102768,
    'payload': {'replica_version_id': 'b3b00ba59c366384e3e0cd53a69457e9053ec987',
                'subnet_id': ('4zbus-z2bmt-ilreg-'
                    'xakz4-6tyre-hsqj4-slb4g-zjwqo-snjcc-iqphi-3qe')},
    'proposer': '80',
    'status': 'EXECUTED',
    'summary': 'Update subnet '
                '4zbus-z2bmt-ilreg-xakz4-6tyre-hsqj4-slb4g-zjwqo-snjcc-iqphi-3qe '
                'to replica version '
                '[b3b00ba59c366384e3e0cd53a69457e9053ec987](https:##dashboard.internetcomputer.org/release/b3b00ba59c366384e3e0cd53a69457e9053ec987)\n',
    'title': 'Update subnet 4zbus to replica version b3b00ba5',
    'topic': 'TOPIC_IC_OS_VERSION_DEPLOYMENT'}
    """

    payload: AbbrevSubnetUpdateProposalPayload
    proposal_id: int
    proposer: int
    status: ProposalStatus
    summary: str
    title: str
    topic: ProposalTopic


class AbbrevApiBoundaryNodesUpdateProposalPayload(TypedDict):
    version: str
    node_ids: list[str]


class AbbrevApiBoundaryNodesUpdateProposal(TypedDict):
    """
    {'proposal_id': 102768,
    'payload': {'version': 'b3b00ba59c366384e3e0cd53a69457e9053ec987',
                'node_ids': [
                '4fssn-4vi43-2qufr-hlrfz-hfohd-jgrwc-7l7ok-uatwb-ukau7-lwmoz-tae'
                ]},
    'proposer': '80',
    'status': 'EXECUTED',
    'summary': 'Update 1 API boundary node(s) to '
                'f8131bfbc2d339716a9cff06e04de49a68e5a80b '
                '\n'nMotivation...',
    'title': 'Update 1 API boundary node(s) to f8131bf',
    'topic': 'TOPIC_IC_OS_VERSION_DEPLOYMENT'}
    """

    payload: AbbrevApiBoundaryNodesUpdateProposalPayload
    proposal_id: int
    proposer: int
    status: ProposalStatus
    summary: str
    title: str
    topic: ProposalTopic


class AbbrevHostOsVersionUpdateProposalPayload(TypedDict):
    hostos_version_id: str | None
    node_ids: list[str]


class AbbrevHostOsVersionUpdateProposal(TypedDict):
    """
    {'proposal_id': 102768,
    'payload': {'hostos_version_id': 'b3b00ba59c366384e3e0cd53a69457e9053ec987',
                'node_ids': [
                '4fssn-4vi43-2qufr-hlrfz-hfohd-jgrwc-7l7ok-uatwb-ukau7-lwmoz-tae'
                ]},
    'proposer': '80',
    'status': 'EXECUTED',
    'summary': '...',
    'title': 'Set HostOS version: f195ba756bc3bf170a2888699e5e74101fdac6ba'
             ' on 27 nodes',
    'topic': 'TOPIC_IC_OS_VERSION_DEPLOYMENT'}
    """

    payload: AbbrevHostOsVersionUpdateProposalPayload
    proposal_id: int
    proposer: int
    status: ProposalStatus
    summary: str
    title: str
    topic: ProposalTopic
