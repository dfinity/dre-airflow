import unittest

from operators.ic_api import unroll
from operators.ic_os_rollout import CreateProposalIdempotently


class TestOperators(unittest.TestCase):
    def test_instantiation(self) -> None:
        sid = "qn2sv-gibnj-5jrdq-3irkq-ozzdo-ri5dn-dynlb-xgk6d-kiq7w-cvop5-uae"
        gitr = "a1f503d20b7846375c74ce5f7d0f8f6620ab7511"
        CreateProposalIdempotently(task_id="xxx", subnet_id=sid, git_revision=gitr)


class TestUnroll(unittest.TestCase):
    def test_simple_case(self) -> None:
        self.assertListEqual(unroll(100, 0), [(100, 0)])

    def test_thrice_unrolled(self) -> None:
        self.assertListEqual(unroll(300, 0), [(100, 0), (100, 100), (100, 200)])

    def test_thrice_unrolled_uneven(self) -> None:
        self.assertListEqual(
            unroll(305, 0), [(100, 0), (100, 100), (100, 200), (5, 300)]
        )
