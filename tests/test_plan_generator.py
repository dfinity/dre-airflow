import datetime
import unittest
from datetime import timezone as tz

import yaml
from dfinity.ic_os_rollout import rollout_planner, week_planner
from dfinity.ic_types import SubnetRolloutInstance


class TestWeekPlanner(unittest.TestCase):
    def test_week_planner(self) -> None:
        """Test for import errors on a file"""
        on_monday = datetime.datetime(2023, 6, 12, 0, 0, 0)
        plan = week_planner(on_monday)
        assert plan["Monday"].day == on_monday.day
        assert plan["Friday"].day == on_monday.day + 4, plan["Friday"]
        assert plan["Monday next week"].day == on_monday.day + 7

        on_friday = datetime.datetime(2023, 6, 16, 0, 0, 0)
        plan = week_planner(on_monday)
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
        inp: list[SubnetRolloutInstance],
    ) -> list[tuple[datetime.datetime, int]]:
        return list((i.start_at, i.subnet_num) for i in inp)

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
  11:00: [20, 27, 34]
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
            (datetime.datetime(2023, 6, 12, 9, 0, tzinfo=tz.utc), 6),
            (datetime.datetime(2023, 6, 12, 11, 0, tzinfo=tz.utc), 8),
            (datetime.datetime(2023, 6, 12, 11, 0, tzinfo=tz.utc), 33),
            (datetime.datetime(2023, 6, 13, 7, 0, tzinfo=tz.utc), 15),
            (datetime.datetime(2023, 6, 13, 7, 0, tzinfo=tz.utc), 18),
            (datetime.datetime(2023, 6, 13, 9, 0, tzinfo=tz.utc), 1),
            (datetime.datetime(2023, 6, 13, 9, 0, tzinfo=tz.utc), 5),
            (datetime.datetime(2023, 6, 13, 9, 0, tzinfo=tz.utc), 2),
            (datetime.datetime(2023, 6, 13, 11, 0, tzinfo=tz.utc), 4),
            (datetime.datetime(2023, 6, 13, 11, 0, tzinfo=tz.utc), 9),
            (datetime.datetime(2023, 6, 13, 11, 0, tzinfo=tz.utc), 34),
            (datetime.datetime(2023, 6, 14, 7, 0, tzinfo=tz.utc), 3),
            (datetime.datetime(2023, 6, 14, 7, 0, tzinfo=tz.utc), 7),
            (datetime.datetime(2023, 6, 14, 7, 0, tzinfo=tz.utc), 11),
            (datetime.datetime(2023, 6, 14, 9, 0, tzinfo=tz.utc), 10),
            (datetime.datetime(2023, 6, 14, 9, 0, tzinfo=tz.utc), 13),
            (datetime.datetime(2023, 6, 14, 9, 0, tzinfo=tz.utc), 16),
            (datetime.datetime(2023, 6, 14, 11, 0, tzinfo=tz.utc), 20),
            (datetime.datetime(2023, 6, 14, 11, 0, tzinfo=tz.utc), 27),
            (datetime.datetime(2023, 6, 14, 11, 0, tzinfo=tz.utc), 34),
            (datetime.datetime(2023, 6, 14, 13, 0, tzinfo=tz.utc), 21),
            (datetime.datetime(2023, 6, 14, 13, 0, tzinfo=tz.utc), 12),
            (datetime.datetime(2023, 6, 14, 13, 0, tzinfo=tz.utc), 28),
            (datetime.datetime(2023, 6, 15, 7, 0, tzinfo=tz.utc), 26),
            (datetime.datetime(2023, 6, 15, 7, 0, tzinfo=tz.utc), 22),
            (datetime.datetime(2023, 6, 15, 7, 0, tzinfo=tz.utc), 23),
            (datetime.datetime(2023, 6, 15, 9, 0, tzinfo=tz.utc), 25),
            (datetime.datetime(2023, 6, 15, 9, 0, tzinfo=tz.utc), 29),
            (datetime.datetime(2023, 6, 15, 9, 0, tzinfo=tz.utc), 19),
            (datetime.datetime(2023, 6, 15, 11, 0, tzinfo=tz.utc), 17),
            (datetime.datetime(2023, 6, 15, 11, 0, tzinfo=tz.utc), 32),
            (datetime.datetime(2023, 6, 15, 11, 0, tzinfo=tz.utc), 35),
            (datetime.datetime(2023, 6, 15, 13, 0, tzinfo=tz.utc), 30),
            (datetime.datetime(2023, 6, 15, 13, 0, tzinfo=tz.utc), 31),
            (datetime.datetime(2023, 6, 15, 13, 0, tzinfo=tz.utc), 14),
            (datetime.datetime(2023, 6, 19, 11, 0, tzinfo=tz.utc), 0),
        ]

        on_monday = datetime.datetime(2023, 6, 12, 0, 0, 0)
        plan_structure = yaml.safe_load(plan)
        rollout_plan = rollout_planner(
            plan_structure,
            self.fake_get_subnet_list,
            on_monday,
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

    def test_rollout_planner_ambiguous_rollout(self) -> None:
        plan = """
Monday:
  9:00: ["0000"]
"""
        rollout_plan = yaml.safe_load(plan)
        monday = datetime.datetime(2023, 6, 12, 0, 0, 0)
        self.assertRaises(
            ValueError,
            lambda: rollout_planner(
                rollout_plan,
                self.fake_get_subnet_list,
                monday,
            ),
        )

    def test_rollout_planner_unambiguous_rollout(self) -> None:
        plan = """
Monday:
  9:00: ["0000-0002"]
"""
        rollout_plan = yaml.safe_load(plan)
        monday = datetime.datetime(2023, 6, 12, 0, 0, 0)
        expected = [
            (datetime.datetime(2023, 6, 12, 9, 0, tzinfo=tz.utc), 2),
        ]
        res = rollout_planner(
            rollout_plan,
            self.fake_get_subnet_list,
            monday,
        )
        assert self.transform_actual(res) == expected
