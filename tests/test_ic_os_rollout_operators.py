import unittest

from dfinity.ic_types import IC_NETWORKS
from operators.ic_os_rollout import CreateProposalIdempotently


class TestOperators(unittest.TestCase):
    def test_instantiation(self) -> None:
        sid = "qn2sv-gibnj-5jrdq-3irkq-ozzdo-ri5dn-dynlb-xgk6d-kiq7w-cvop5-uae"
        gitr = "a1f503d20b7846375c74ce5f7d0f8f6620ab7511"
        CreateProposalIdempotently(
            task_id="xxx",
            subnet_id=sid,
            git_revision=gitr,
            simulate_proposal=True,
            network=IC_NETWORKS["mainnet"],
        )
