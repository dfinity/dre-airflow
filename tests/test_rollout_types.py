import textwrap
import unittest

import dfinity.rollout_types as rollout_types


class TestApiBoundaryNodeRolloutPlanSpec(unittest.TestCase):
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
        rollout_types.yaml_to_ApiBoundaryNodeRolloutPlanSpec(inp)

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
            ValueError,
            lambda: rollout_types.yaml_to_ApiBoundaryNodeRolloutPlanSpec(inp),
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
            ValueError,
            lambda: rollout_types.yaml_to_ApiBoundaryNodeRolloutPlanSpec(inp),
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
            ValueError,
            lambda: rollout_types.yaml_to_ApiBoundaryNodeRolloutPlanSpec(inp),
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
            ValueError,
            lambda: rollout_types.yaml_to_ApiBoundaryNodeRolloutPlanSpec(inp),
        )


class TestHostOSRolloutPlanSpec(unittest.TestCase):
    def test_valid(self) -> None:
        inp = textwrap.dedent("""\
        stages:
            canary:
            - selectors:
                assignment: unassigned
                owner: DFINITY
                nodes_per_group: 1
                status: Healthy
            - selectors:
                - assignment: unassigned
                  owner: DFINITY
                  nodes_per_group: 5
                  status: Healthy
            - selectors:
                - assignment: assigned
                  owner: DFINITY
                  nodes_per_group: 40%
                  status: Healthy
            - selectors:
                - assignment: assigned
                  owner: others
                  group_by: subnet
                  nodes_per_group: 1
                  status: Healthy
            main:
                selectors:
                - assignment: assigned
                  group_by: subnet
                  nodes_per_group: 1
                  status: Healthy
            unassigned:
                selectors:
                - assignment: unassigned
                  status: Healthy
            stragglers:
                selectors: []
        allowed_days: [Wednesday]
        resume_at: 9:00
        suspend_at: 18:59
        minimum_minutes_per_batch: 60
        """)
        rollout_types.yaml_to_HostOSRolloutPlanSpec(inp)

    def test_only_canary(self) -> None:
        inp = textwrap.dedent("""\
        stages:
            canary:
            - selectors:
                - assignment: unassigned
                  owner: DFINITY
                  nodes_per_group: 20%
                  status: Healthy
        allowed_days: [Wednesday]
        resume_at: 9:00
        suspend_at: 18:59
        minimum_minutes_per_batch: 60
        """)

        p = rollout_types.yaml_to_HostOSRolloutPlanSpec(inp)
        stages = p["stages"]
        assert (
            stages["canary"][0]["selectors"]["intersect"][0]["nodes_per_group"] == 0.2
        )
        assert len(stages["canary"]) == 1
        assert "main" not in stages
        assert "unassigned" not in stages
        assert "stragglers" not in stages

    def test_tolerance(self) -> None:
        inp = textwrap.dedent("""\
        stages:
            canary:
            - selectors:
                - assignment: unassigned
                  owner: DFINITY
                  nodes_per_group: 20%
                  status: Healthy
              tolerance: 0
            - selectors:
                - assignment: unassigned
                  owner: DFINITY
                  nodes_per_group: 20%
                  status: Healthy
              tolerance: 25%
        allowed_days: [Wednesday]
        resume_at: 9:00
        suspend_at: 18:59
        minimum_minutes_per_batch: 60
        """)

        p = rollout_types.yaml_to_HostOSRolloutPlanSpec(inp)
        stages = p["stages"]
        assert stages["canary"][0]["tolerance"] == 0
        assert stages["canary"][1]["tolerance"] == 0.25

    def test_join(self) -> None:
        inp = textwrap.dedent("""\
        stages:
            main:
                selectors:
                   join:
                    - assignment: assigned
                      group_by: subnet
                      status: Healthy
                      nodes_per_group: 1
                    - assignment: API boundary
                      status: Healthy
                      nodes_per_group: 1
        allowed_days: [Wednesday]
        resume_at: 9:00
        suspend_at: 18:59
        minimum_minutes_per_batch: 60
        """)
        p = rollout_types.yaml_to_HostOSRolloutPlanSpec(inp)
        stages = p["stages"]
        assert stages["main"]["selectors"]["join"][0]["nodes_per_group"] == 1
        assert stages["main"]["selectors"]["join"][1]["assignment"] == "API boundary"

    def test_illegal_specifier(self) -> None:
        inp = textwrap.dedent("""\
        stages:
            main:
                selectors: [{}]
        allowed_days: [Wednesday]
        resume_at: 9:00
        suspend_at: 18:59
        minimum_minutes_per_batch: 60
        """)
        self.assertRaises(
            ValueError,
            lambda: rollout_types.yaml_to_HostOSRolloutPlanSpec(inp),
        )

    def test_complement(self) -> None:
        inp = textwrap.dedent("""\
        stages:
            main:
                selectors:
                    intersect:
                    - join:
                            - assignment: assigned
                              group_by: subnet
                              status: Healthy
                              nodes_per_group: 1
                            - assignment: API boundary
                              status: Healthy
                              nodes_per_group: 1
                    - not:
                        datacenter: hk4
        allowed_days: [Wednesday]
        resume_at: 9:00
        suspend_at: 18:59
        minimum_minutes_per_batch: 60
        """)
        p = rollout_types.yaml_to_HostOSRolloutPlanSpec(inp)
        stages = p["stages"]
        assert (
            stages["main"]["selectors"]["intersect"][0]["join"][0]["nodes_per_group"]
            == 1
        )
        assert (
            stages["main"]["selectors"]["intersect"][0]["join"][1]["assignment"]
            == "API boundary"
        )
        assert stages["main"]["selectors"]["intersect"][1]["not"]["datacenter"] == "hk4"


class TestIntOrFloatBounded(unittest.TestCase):
    def test_int(self) -> None:
        assert 1 == rollout_types.intorfloatbounded("1")
        assert 0 == rollout_types.intorfloatbounded("0")
        assert 0.5 == rollout_types.intorfloatbounded("50%")
        self.assertRaises(ValueError, lambda: rollout_types.intorfloatbounded("101%"))
        self.assertRaises(ValueError, lambda: rollout_types.intorfloatbounded(-1))
        self.assertRaises(ValueError, lambda: rollout_types.intorfloatbounded({}))
