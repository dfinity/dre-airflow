"""
IC API code.

Minimal API to deal with proposals, sufficient in scope to
help with IC OS rollouts.
"""

import urllib.parse
from enum import Enum
from typing import Any, TypedDict, cast

import requests  # type:ignore


class ProposalStatus(Enum):
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
    TOPIC_SUBNET_REPLICA_VERSION_MANAGEMENT = 12
    ## All proposals dealing with blessing and retirement of replica versions.
    TOPIC_REPLICA_VERSION_MANAGEMENT = 13
    ## Proposals related to SNS and Community Fund.
    TOPIC_SNS_AND_COMMUNITY_FUND = 14


class Proposal(TypedDict):
    """
    {'action': 'ExecuteNnsFunction',
    'action_nns_function': 'UpdateSubnetReplicaVersion',
    'deadline_timestamp_seconds': 1685189780,
    'decided_timestamp_seconds': 1684844290,
    'executed_timestamp_seconds': 1684844290,
    'failed_timestamp_seconds': 0,
    'id': 102768,
    'latest_tally': {'no': 0,
                    'timestamp_seconds': 1684844290,
                    'total': 44511285100798545,
                    'yes': 44247798987033488},
    'payload': {'replica_version_id': 'b3b00ba59c366384e3e0cd53a69457e9053ec987',
                'subnet_id': ('4zbus-z2bmt-ilreg-'
                    'xakz4-6tyre-hsqj4-slb4g-zjwqo-snjcc-iqphi-3qe')},
    'proposal_id': 122572,
    'proposal_timestamp_seconds': 1684844180,
    'proposer': '80',
    'reject_cost_e8s': 1000000000,
    'reward_status': 'SETTLED',
    'settled_at': 1685203200,
    'status': 'EXECUTED',
    'summary': 'Update subnet '
                '4zbus-z2bmt-ilreg-xakz4-6tyre-hsqj4-slb4g-zjwqo-snjcc-iqphi-3qe '
                'to replica version '
                '[b3b00ba59c366384e3e0cd53a69457e9053ec987](https:##dashboard.internetcomputer.org/release/b3b00ba59c366384e3e0cd53a69457e9053ec987)\n',
    'title': 'Update subnet 4zbus to replica version b3b00ba5',
    'topic': 'TOPIC_SUBNET_REPLICA_VERSION_MANAGEMENT',
    'updated_at': '2023-05-27T16:01:35.534945',
    'url': ''}
    """

    action: str
    action_nns_function: str
    deadline_timestamp_seconds: int
    decided_timestamp_seconds: int
    executed_timestamp_seconds: int
    failed_timestamp_seconds: int
    id: int
    payload: dict[str, str]
    proposal_id: int
    proposal_timestamp_seconds: int
    proposer: int
    reject_cost_e8s: int
    reward_status: str
    settled_at: int
    status: ProposalStatus
    summary: str
    title: str
    topic: ProposalTopic
    updated_at: str
    url: str


def unroll(limit: int, offset: int) -> list[tuple[int, int]]:
    r = []
    while limit > 100:
        r.append((100, offset))
        limit = limit - 100
        offset = offset + 100
    r.append((limit, offset))
    return r


def get_proposals(
    topic: ProposalTopic | None, limit: int = 100, offset: int = 0
) -> list[Proposal]:
    url = "https://ic-api.internetcomputer.org/api/v3/proposals"
    res: list[Proposal] = []

    for limit, offset in unroll(limit, offset):
        params: dict[str, Any] = {
            "limit": limit,
            "offset": offset,
        }
        if topic is not None:
            params["topic"] = topic.name

        p = urllib.parse.urlencode(params)
        u = url + ("" if not p else ("?" + p))
        r = requests.get(u)
        r.raise_for_status()
        for p in r.json()["data"]:
            x = cast(Proposal, p)
            x["status"] = getattr(
                ProposalStatus, "PROPOSAL_STATUS_" + cast(str, x["status"])
            )
            x["topic"] = getattr(ProposalTopic, cast(str, x["topic"]))
            res.append(x)

    return res


def get_proposals_for_subnet_and_revision(
    git_revision: str, subnet_id: str, limit: int, offset: int = 0
) -> list[Proposal]:
    return [
        r
        for r in get_proposals(
            topic=ProposalTopic.TOPIC_SUBNET_REPLICA_VERSION_MANAGEMENT,
            limit=limit,
            offset=offset,
        )
        if r["payload"].get("subnet_id") == subnet_id
        and r["payload"].get("replica_version_id") == git_revision
    ]


if __name__ == "__main__":
    import pprint

    for p in get_proposals_for_subnet_and_revision(
        subnet_id="yinp6-35cfo-wgcd2-oc4ty-2kqpf-t4dul-rfk33-fsq3r-mfmua-m2ngh-jqe",
        git_revision="d5eb7683e144acb0f8850fedb29011f34bfbe4ac",
        limit=1000,
    ):
        pprint.pprint(p)
