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
    ## topic, the followees for this topic are used instead.
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
    ## Installing and upgrading “system” canisters that belong to the network.
    ## For example, upgrading the NNS.
    TOPIC_NETWORK_CANISTER_MANAGEMENT = 8
    ## Proposals that update KYC information for regulatory purposes,
    ## for example during the initial Genesis distribution of ICP in the
    ## form of neurons.
    TOPIC_KYC = 9
    ## Topic for proposals to reward node providers.
    TOPIC_NODE_PROVIDER_REWARDS = 10
    ## Superseded by SNS_COMMUNITY_FUND.
    ##
    ## TODO(NNS1-1787): Delete this. In addition to clients wiping this from their
    ## memory, I think we'll need Candid support in order to safely delete
    ## this. There is no rush to delete this though.
    TOPIC_SNS_DECENTRALIZATION_SALE = 11
    ## Proposals handling updates of a subnet's replica version.
    ## The only proposal in this topic is UpdateSubnetReplicaVersion.
    TOPIC_IC_OS_VERSION_DEPLOYMENT = 12
    ## All proposals dealing with blessing and retirement of replica versions.
    TOPIC_IC_OS_VERSION_ELECTION = 13
    ## Proposals related to SNS and Community Fund.
    TOPIC_SNS_AND_COMMUNITY_FUND = 14
    ## Proposals related to the management of API Boundary nodes.
    TOPIC_API_BOUNDARY_NODE_MANAGEMENT = 15
    ## Proposals related to rental of subnets (Utopia).
    ## TODO: verify the int corresponds to the proto.
    TOPIC_SUBNET_RENTAL = 16


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
    """

    payload: dict[str, str]
    proposal_id: int
    proposer: int
    status: ProposalStatus
    summary: str
    title: str
    topic: ProposalTopic
