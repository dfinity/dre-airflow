import textwrap
import unittest

import dfinity.rollout_types as rollout_types


class TestBoundaryNodeRolloutPlanSpec(unittest.TestCase):
    def test_valid(self) -> None:
        """Tests that a valid rollout plan spec works."""
        inp = textwrap.dedent("""\
        nodes:
        - abc
        start_day: Wednesday
        resume_at: 9:00
        suspend_at: 18:59
        minimum_minutes_per_batch: 60
        """)
        rollout_types.yaml_to_BoundaryNodeRolloutPlanSpec(inp)

    def test_bad_weekday(self) -> None:
        inp = textwrap.dedent("""\
        nodes:
        - abc
        start_day: Wednsesday
        resume_at: 9:00
        suspend_at: 18:59
        minimum_minutes_per_batch: 60
        """)
        self.assertRaises(
            ValueError, lambda: rollout_types.yaml_to_BoundaryNodeRolloutPlanSpec(inp)
        )

    def test_no_nodes(self) -> None:
        inp = textwrap.dedent("""\
        nodes: []
        start_day: Wednsesday
        resume_at: 9:00
        suspend_at: 18:59
        minimum_minutes_per_batch: 60
        """)
        self.assertRaises(
            ValueError, lambda: rollout_types.yaml_to_BoundaryNodeRolloutPlanSpec(inp)
        )

    def test_zero_minimum_minutes(self) -> None:
        inp = textwrap.dedent("""\
        nodes: [abc]
        start_day: Wednsesday
        resume_at: 9:00
        suspend_at: 18:59
        minimum_minutes_per_batch: 0
        """)
        self.assertRaises(
            ValueError, lambda: rollout_types.yaml_to_BoundaryNodeRolloutPlanSpec(inp)
        )

    def test_wrong_resume_at_time(self) -> None:
        inp = textwrap.dedent("""\
        nodes: [abc]
        start_day: Wednsesday
        resume_at: 5.5
        suspend_at: 18:59
        minimum_minutes_per_batch: 60
        """)
        self.assertRaises(
            ValueError, lambda: rollout_types.yaml_to_BoundaryNodeRolloutPlanSpec(inp)
        )
