import datetime
from dataclasses import dataclass


@dataclass
class SubnetRolloutInstance:
    start_at: datetime.datetime
    subnet_num: int
    subnet_id: str


@dataclass
class ICNetwork:
    nns_url: str
    proposal_url: str
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
        proposal_url=network.proposal_url,
        proposal_display_url=network.proposal_display_url,
        release_display_url=network.release_display_url,
        prometheus_urls=network.prometheus_urls,
        proposer_neuron_id=network.proposer_neuron_id,
        proposer_neuron_private_key_variable_name=network.proposer_neuron_private_key_variable_name,
    )
