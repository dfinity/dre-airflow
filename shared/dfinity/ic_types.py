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
