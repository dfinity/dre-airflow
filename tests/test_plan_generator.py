import datetime
import unittest
from datetime import timezone as tz

import yaml
from dfinity.ic_os_rollout import RolloutPlan, rollout_planner, week_planner

_MONDAY = datetime.datetime(2023, 6, 12, 0, 0, 0)


class TestWeekPlanner(unittest.TestCase):
    def test_week_planner(self) -> None:
        """Test for import errors on a file"""
        plan = week_planner(_MONDAY)
        assert plan["Monday"].day == _MONDAY.day
        assert plan["Friday"].day == _MONDAY.day + 4, plan["Friday"]
        assert plan["Monday next week"].day == _MONDAY.day + 7

        on_friday = datetime.datetime(2023, 6, 16, 0, 0, 0)
        plan = week_planner(_MONDAY)
        assert plan["Monday"].day == on_friday.day - 4, plan["Monday"]
        assert plan["Friday"].day == on_friday.day, plan["Friday"]
        assert plan["Monday next week"].day == on_friday.day + 3

        on_saturday = datetime.datetime(2023, 6, 17, 0, 0, 0)
        plan = week_planner(on_saturday)
        assert plan["Monday"].day == on_saturday.day + 2
        assert plan["Monday next week"].day == on_saturday.day + 9


class TestRolloutPlanner(unittest.TestCase):
    def transform_actual(
        self,
        inp: RolloutPlan,
    ) -> list[tuple[int, datetime.datetime, int]]:
        """
        Convert RolloutPlan into simple list of batch index, date/time, subnet num.

        This is done for aesthetic purposes in diffs when the tests do not return
        the expected results.
        """
        ret: list[tuple[int, datetime.datetime, int]] = []
        for nstr, (_, subnets) in inp.items():
            for i in subnets:
                ret.append((int(nstr) + 1, i.start_at, i.subnet_num))
        return ret

    def transform_actual_with_git_revision(
        self,
        inp: RolloutPlan,
    ) -> list[tuple[int, datetime.datetime, int, str]]:
        """
        Convert RolloutPlan into simple list of batch index, date/time, subnet num.

        This is done for aesthetic purposes in diffs when the tests do not return
        the expected results.
        """
        ret: list[tuple[int, datetime.datetime, int, str]] = []
        for nstr, (_, subnets) in inp.items():
            for i in subnets:
                ret.append((int(nstr) + 1, i.start_at, i.subnet_num, i.git_revision))
        return ret

    def fake_get_subnet_list(self) -> list[str]:
        return ["0000-00%02d-0000-0000" % n for n in range(38)]

    def test_rollout_planner_default_rollout(self) -> None:
        plan = """
Monday:
  9:00: [6]
  11:00: [8, 33]
Tuesday:
  7:00: [15, 18]
  9:00: [1, 5, 2]
  11:00: [4, 9, 34]
Wednesday:
  7:00: [3, 7, 11]
  9:00: [10, 13, 16]
  11:00: [20, 27]
  13:00: [21, 12, 28]
Thursday:
  7:00: [26, 22, 23]
  9:00: [25, 29, 19]
  11:00: [17, 32, 35]
  13:00: [30, 31, 14]
Monday next week:
  11:00: [0]
"""
        expected = [
            (1, datetime.datetime(2023, 6, 12, 9, 0, tzinfo=tz.utc), 6),
            (2, datetime.datetime(2023, 6, 12, 11, 0, tzinfo=tz.utc), 8),
            (2, datetime.datetime(2023, 6, 12, 11, 0, tzinfo=tz.utc), 33),
            (3, datetime.datetime(2023, 6, 13, 7, 0, tzinfo=tz.utc), 15),
            (3, datetime.datetime(2023, 6, 13, 7, 0, tzinfo=tz.utc), 18),
            (4, datetime.datetime(2023, 6, 13, 9, 0, tzinfo=tz.utc), 1),
            (4, datetime.datetime(2023, 6, 13, 9, 0, tzinfo=tz.utc), 5),
            (4, datetime.datetime(2023, 6, 13, 9, 0, tzinfo=tz.utc), 2),
            (5, datetime.datetime(2023, 6, 13, 11, 0, tzinfo=tz.utc), 4),
            (5, datetime.datetime(2023, 6, 13, 11, 0, tzinfo=tz.utc), 9),
            (5, datetime.datetime(2023, 6, 13, 11, 0, tzinfo=tz.utc), 34),
            (6, datetime.datetime(2023, 6, 14, 7, 0, tzinfo=tz.utc), 3),
            (6, datetime.datetime(2023, 6, 14, 7, 0, tzinfo=tz.utc), 7),
            (6, datetime.datetime(2023, 6, 14, 7, 0, tzinfo=tz.utc), 11),
            (7, datetime.datetime(2023, 6, 14, 9, 0, tzinfo=tz.utc), 10),
            (7, datetime.datetime(2023, 6, 14, 9, 0, tzinfo=tz.utc), 13),
            (7, datetime.datetime(2023, 6, 14, 9, 0, tzinfo=tz.utc), 16),
            (8, datetime.datetime(2023, 6, 14, 11, 0, tzinfo=tz.utc), 20),
            (8, datetime.datetime(2023, 6, 14, 11, 0, tzinfo=tz.utc), 27),
            (9, datetime.datetime(2023, 6, 14, 13, 0, tzinfo=tz.utc), 21),
            (9, datetime.datetime(2023, 6, 14, 13, 0, tzinfo=tz.utc), 12),
            (9, datetime.datetime(2023, 6, 14, 13, 0, tzinfo=tz.utc), 28),
            (10, datetime.datetime(2023, 6, 15, 7, 0, tzinfo=tz.utc), 26),
            (10, datetime.datetime(2023, 6, 15, 7, 0, tzinfo=tz.utc), 22),
            (10, datetime.datetime(2023, 6, 15, 7, 0, tzinfo=tz.utc), 23),
            (11, datetime.datetime(2023, 6, 15, 9, 0, tzinfo=tz.utc), 25),
            (11, datetime.datetime(2023, 6, 15, 9, 0, tzinfo=tz.utc), 29),
            (11, datetime.datetime(2023, 6, 15, 9, 0, tzinfo=tz.utc), 19),
            (12, datetime.datetime(2023, 6, 15, 11, 0, tzinfo=tz.utc), 17),
            (12, datetime.datetime(2023, 6, 15, 11, 0, tzinfo=tz.utc), 32),
            (12, datetime.datetime(2023, 6, 15, 11, 0, tzinfo=tz.utc), 35),
            (13, datetime.datetime(2023, 6, 15, 13, 0, tzinfo=tz.utc), 30),
            (13, datetime.datetime(2023, 6, 15, 13, 0, tzinfo=tz.utc), 31),
            (13, datetime.datetime(2023, 6, 15, 13, 0, tzinfo=tz.utc), 14),
            (14, datetime.datetime(2023, 6, 19, 11, 0, tzinfo=tz.utc), 0),
        ]

        plan_structure = yaml.safe_load(plan)
        rollout_plan = rollout_planner(
            plan_structure,
            self.fake_get_subnet_list,
            _MONDAY,
        )
        assert self.transform_actual(rollout_plan) == expected

        on_prior_saturday = datetime.datetime(2023, 6, 10, 0, 0, 0)
        plan_structure = yaml.safe_load(plan)
        rollout_plan = rollout_planner(
            plan_structure,
            self.fake_get_subnet_list,
            on_prior_saturday,
        )
        assert self.transform_actual(rollout_plan) == expected

    def test_rollout_planner_with_batch_number(self) -> None:
        plan = """
Monday:
  9:00: [6]
Monday next week:
  11:00:
    subnets: [0]
    batch: 30
"""
        expected = [
            (1, datetime.datetime(2023, 6, 12, 9, 0, tzinfo=tz.utc), 6),
            (30, datetime.datetime(2023, 6, 19, 11, 0, tzinfo=tz.utc), 0),
        ]

        plan_structure = yaml.safe_load(plan)
        rollout_plan = rollout_planner(
            plan_structure,
            self.fake_get_subnet_list,
            _MONDAY,
        )
        assert self.transform_actual(rollout_plan) == expected

    def test_date_time_ordered_rollout(self) -> None:
        plan = """
Monday next week:
  11:00:
    subnets: [0]
Monday:
  9:00: [6]
  7:00: [22]
"""
        expected = [
            (1, datetime.datetime(2023, 6, 12, 7, 0, tzinfo=tz.utc), 22),
            (2, datetime.datetime(2023, 6, 12, 9, 0, tzinfo=tz.utc), 6),
            (3, datetime.datetime(2023, 6, 19, 11, 0, tzinfo=tz.utc), 0),
        ]

        plan_structure = yaml.safe_load(plan)
        rollout_plan = rollout_planner(
            plan_structure,
            self.fake_get_subnet_list,
            _MONDAY,
        )
        assert self.transform_actual(rollout_plan) == expected

    def test_max_batches_exceeded(self) -> None:
        plan = """
Monday:
  9:00: [6]
Monday next week:
  11:00:
    subnets: [0]
    batch: 30
  14:00:
    subnets: [21]
"""
        self.assertRaises(
            ValueError,
            lambda: rollout_planner(
                yaml.safe_load(plan),
                self.fake_get_subnet_list,
                _MONDAY,
            ),
        )

    def test_cannot_rollout_to_same_subnet_twice(self) -> None:
        plan = """
Monday:
  9:00: [6]
Monday next week:
  11:00:
    subnets: [6]
"""
        self.assertRaises(
            ValueError,
            lambda: rollout_planner(
                yaml.safe_load(plan),
                self.fake_get_subnet_list,
                _MONDAY,
            ),
        )

    def test_batch_too_low(self) -> None:
        plan = """
Monday:
  9:00: [6]
Monday next week:
  11:00:
    subnets: [0]
    batch: 25
  14:00:
    subnets: [21]
    batch: 24
"""
        self.assertRaises(
            ValueError,
            lambda: rollout_planner(
                yaml.safe_load(plan),
                self.fake_get_subnet_list,
                _MONDAY,
            ),
        )

    def test_batch_reused(self) -> None:
        plan = """
Monday:
  9:00: [6]
Monday next week:
  11:00:
    subnets: [0]
    batch: 25
  14:00:
    subnets: [21]
    batch: 25
"""
        self.assertRaises(
            ValueError,
            lambda: rollout_planner(
                yaml.safe_load(plan),
                self.fake_get_subnet_list,
                _MONDAY,
            ),
        )

    def test_rollout_planner_ambiguous_rollout(self) -> None:
        plan = """
Monday:
  9:00: ["0000"]
"""
        rollout_plan = yaml.safe_load(plan)
        self.assertRaises(
            ValueError,
            lambda: rollout_planner(
                rollout_plan,
                self.fake_get_subnet_list,
                _MONDAY,
            ),
        )

    def test_rollout_planner_unambiguous_rollout(self) -> None:
        plan = """
Monday:
  9:00: ["0000-0002"]
"""
        rollout_plan = yaml.safe_load(plan)
        expected = [
            (1, datetime.datetime(2023, 6, 12, 9, 0, tzinfo=tz.utc), 2),
        ]
        res = rollout_planner(
            rollout_plan,
            self.fake_get_subnet_list,
            _MONDAY,
        )
        assert self.transform_actual(res) == expected

    def test_rollout_planner_with_git_revision(self) -> None:
        plan = """
Monday:
  9:00:
  - subnet: 0000-0002
    git_revision: 0123456789012345678901234567890123456789
  - subnet: 0000-0003
  10:00:
    subnets:
    - subnet: 0000-0004
      git_revision: 0123456789012345678901234567890123456789
    - subnet: 0000-0005
    batch: 4
"""
        rollout_plan = yaml.safe_load(plan)
        expected = [
            (
                1,
                datetime.datetime(2023, 6, 12, 9, 0, tzinfo=tz.utc),
                2,
                "0123456789012345678901234567890123456789",
            ),
            (
                1,
                datetime.datetime(2023, 6, 12, 9, 0, tzinfo=tz.utc),
                3,
                None,
            ),
            (
                4,
                datetime.datetime(2023, 6, 12, 10, 0, tzinfo=tz.utc),
                4,
                "0123456789012345678901234567890123456789",
            ),
            (
                4,
                datetime.datetime(2023, 6, 12, 10, 0, tzinfo=tz.utc),
                5,
                None,
            ),
        ]
        res = rollout_planner(
            rollout_plan,
            self.fake_get_subnet_list,
            _MONDAY,
        )
        assert self.transform_actual_with_git_revision(res) == expected

    def test_rollout_planner_with_bad_git_revision(self) -> None:
        plan = """
Monday:
  9:00:
  - subnet: 0000-0002
    git_revision: 012345h7890123456789
"""
        rollout_plan = yaml.safe_load(plan)
        self.assertRaises(
            ValueError,
            lambda: rollout_planner(rollout_plan, self.fake_get_subnet_list, _MONDAY),
        )

    def test_rollout_planner_with_odd_parameters(self) -> None:
        plan = """
Monday:
  9:00:
  - subnet: 0000-0002
    revision: 0123456789012345678901234567890123456789
"""
        rollout_plan = yaml.safe_load(plan)
        self.assertRaises(
            ValueError,
            lambda: rollout_planner(rollout_plan, self.fake_get_subnet_list, _MONDAY),
        )
