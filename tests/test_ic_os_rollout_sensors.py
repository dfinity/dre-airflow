import unittest

from sensors.ic_os_rollout import (
    WaitForProposalAcceptance,
    WaitForReplicaRevisionUpdated,
    WaitUntilNoAlertsOnSubnet,
)


class TestOperators(unittest.TestCase):
    def test_instantiation(self) -> None:
        sid = "qn2sv-gibnj-5jrdq-3irkq-ozzdo-ri5dn-dynlb-xgk6d-kiq7w-cvop5-uae"
        gitr = "a1f503d20b7846375c74ce5f7d0f8f6620ab7511"
        for klass in [
            WaitForProposalAcceptance,
            WaitForReplicaRevisionUpdated,
            WaitUntilNoAlertsOnSubnet,
        ]:
            m = klass(task_id=klass.__name__, subnet_id=sid, git_revision=gitr)
            assert m.counter == 2
