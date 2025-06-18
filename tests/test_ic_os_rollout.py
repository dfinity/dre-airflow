import datetime
import typing
import unittest

from dfinity.ic_os_rollout import (
    boundary_node_batch_create,
    boundary_node_batch_timetable,
    exponential_increase,
    next_day_of_the_week,
)


class TestBoundaryNodeRolloutPlanSpec(unittest.TestCase):
    def test_various_days(self) -> None:
        """Tests that a valid rollout plan spec works."""
        now = datetime.datetime(2025, 6, 12, 11, 59, 10)  # This is a Thursday
        for inp, exp in [
            ("Thursday", 12),
            ("Friday", 13),
            ("Saturday", 14),
            ("Sunday", 15),
            ("Monday", 16),
            ("Tuesday", 17),
            ("Wednesday", 18),
            ("Thursday next week", 19),
        ]:
            res = next_day_of_the_week(inp, now=now).day
            assert res == exp, f"inp: res != exp ({inp}: {res} != {exp})"

    def test_invalid_day(self) -> None:
        self.assertRaises(ValueError, lambda: next_day_of_the_week("Everyday"))


def fmtdate(d: datetime.datetime, day: bool = False) -> str:
    if day:
        return d.strftime("%A %d %H:%M")
    return d.strftime("%A %H:%M")


class TestBoundaryNodeBatchTimetable(unittest.TestCase):
    maxDiff = None

    def test_simple_case(self) -> None:
        now = datetime.datetime(2025, 6, 12, 14, 33, 16)
        plan = {
            "nodes": ["abc"],
            "start_day": "Thursday",
            "resume_at": "9:00",
            "suspend_at": "18:00",
            "minimum_minutes_per_batch": 60,
        }
        exp = [
            "Thursday 09:00",
            "Thursday 10:00",
            "Thursday 11:00",
            "Thursday 12:00",
            "Thursday 13:00",
            "Thursday 14:00",
            "Thursday 15:00",
            "Thursday 16:00",
            "Thursday 17:00",
            "Friday 09:00",
            "Friday 10:00",
            "Friday 11:00",
            "Friday 12:00",
            "Friday 13:00",
            "Friday 14:00",
            "Friday 15:00",
            "Friday 16:00",
            "Friday 17:00",
            "Saturday 09:00",
            "Saturday 10:00",
        ]
        res = [fmtdate(d) for d in boundary_node_batch_timetable(plan, 20, now=now)]
        self.assertListEqual(exp, res)

    def test_tight_night_case(self) -> None:
        now = datetime.datetime(2025, 6, 12, 14, 33, 16)
        plan = {
            "nodes": ["abc"],
            "start_day": "Thursday",
            "resume_at": "21:00",
            "suspend_at": "6:59",
            "minimum_minutes_per_batch": 60,
        }
        exp = [
            "Thursday 21:00",
            "Thursday 22:00",
            "Thursday 23:00",
            "Friday 00:00",
            "Friday 01:00",
            "Friday 02:00",
            "Friday 03:00",
            "Friday 04:00",
            "Friday 05:00",
            "Friday 21:00",
            "Friday 22:00",
            "Friday 23:00",
            "Saturday 00:00",
            "Saturday 01:00",
            "Saturday 02:00",
            "Saturday 03:00",
            "Saturday 04:00",
            "Saturday 05:00",
            "Saturday 21:00",
            "Saturday 22:00",
        ]
        res = [fmtdate(d) for d in boundary_node_batch_timetable(plan, 20, now=now)]
        self.assertListEqual(exp, res)

    def test_night_case(self) -> None:
        now = datetime.datetime(2025, 6, 12, 14, 33, 16)
        plan = {
            "nodes": ["abc"],
            "start_day": "Thursday",
            "resume_at": "21:00",
            "suspend_at": "1:00",
            "minimum_minutes_per_batch": 60,
        }
        exp = [
            "Thursday 21:00",
            "Thursday 22:00",
            "Thursday 23:00",
            "Friday 00:00",
            "Friday 21:00",
            "Friday 22:00",
            "Friday 23:00",
            "Saturday 00:00",
        ]
        res = [fmtdate(d) for d in boundary_node_batch_timetable(plan, 8, now=now)]
        self.assertListEqual(exp, res)

    def test_barely_fits_in_window(self) -> None:
        now = datetime.datetime(2025, 6, 12, 14, 33, 16)
        plan = {
            "nodes": ["abc"],
            "start_day": "Thursday",
            "resume_at": "21:00",
            "suspend_at": "23:00",
            "minimum_minutes_per_batch": 120,
        }
        exp = [
            "Thursday 21:00",
            "Friday 21:00",
            "Saturday 21:00",
        ]
        res = [fmtdate(d) for d in boundary_node_batch_timetable(plan, 3, now=now)]
        self.assertListEqual(exp, res)

    def test_does_not_fit_in_window(self) -> None:
        now = datetime.datetime(2025, 6, 12, 14, 33, 16)
        plan = {
            "nodes": ["abc"],
            "start_day": "Thursday",
            "resume_at": "21:00",
            "suspend_at": "23:00",
            "minimum_minutes_per_batch": 121,
        }
        self.assertRaises(
            ValueError, lambda: boundary_node_batch_timetable(plan, 3, now=now)
        )

    def test_absent_start_day(self) -> None:
        now = datetime.datetime(2025, 6, 11, 14, 33, 16)
        plan = {
            "nodes": ["abc"],
            "resume_at": "21:00",
            "suspend_at": "23:00",
            "minimum_minutes_per_batch": 120,
        }
        exp = [
            "Wednesday 21:00",
            "Thursday 21:00",
            "Friday 21:00",
        ]
        res = [fmtdate(d) for d in boundary_node_batch_timetable(plan, 3, now=now)]
        self.assertListEqual(exp, res)

    def test_starts_tomorrow(self) -> None:
        now = datetime.datetime(2025, 6, 11, 14, 33, 16)
        plan = {
            "nodes": ["abc"],
            "resume_at": "21:00",
            "start_day": "Thursday",
            "suspend_at": "23:00",
            "minimum_minutes_per_batch": 120,
        }
        exp = [
            "Thursday 21:00",
            "Friday 21:00",
            "Saturday 21:00",
        ]
        res = [fmtdate(d) for d in boundary_node_batch_timetable(plan, 3, now=now)]
        self.assertListEqual(exp, res)

    def test_starts_next_week_tomorrow(self) -> None:
        """
        Test that a plan computed Wed 11th for Thu next week starts the 19th.
        """
        now = datetime.datetime(2025, 6, 11, 14, 33, 16)
        plan = {
            "nodes": ["abc"],
            "resume_at": "21:00",
            "start_day": "Thursday next week",
            "suspend_at": "23:00",
            "minimum_minutes_per_batch": 120,
        }
        exp = [
            "Thursday 19 21:00",
            "Friday 20 21:00",
            "Saturday 21 21:00",
        ]
        res = [
            fmtdate(d, day=True)
            for d in boundary_node_batch_timetable(plan, 3, now=now)
        ]
        self.assertListEqual(exp, res)


class TestExponentialIncrease(unittest.TestCase):
    def expect(
        self, expected: str, count: int, batch_count: int, **kwargs: typing.Any
    ) -> None:
        self.assertEqual(
            expected,
            " ".join(
                str(s) for s in exponential_increase(count, batch_count, **kwargs)
            ),
        )

    def test_basic(self) -> None:
        self.expect("1 1 1 1 1", count=5, batch_count=5)

    def test_basic_increase(self) -> None:
        self.expect("2 2 2 2 2", count=10, batch_count=5)

    def test_basic_big_increase(self) -> None:
        self.expect("3 3 4 4 6", count=20, batch_count=5)

    def test_count_lower_than_batch_count(self) -> None:
        self.expect("1 1 1 0 0", count=3, batch_count=5)

    def test_large(self) -> None:
        self.expect("7 8 10 11 14", count=50, batch_count=5)

    def test_sharper_knee(self) -> None:
        self.expect(
            "1 1 1 1 2 3 4 6 9 22", count=50, batch_count=10, increase_factor=0.5
        )


class TestBoundaryNodeBatchCreate(unittest.TestCase):
    def expect(
        self,
        expected: list[str],
        nodes: list[str],
        batch_count: int,
        **kwargs: typing.Any,
    ) -> None:
        self.assertListEqual(
            expected,
            [
                "".join(x)
                for x in boundary_node_batch_create(nodes, batch_count, **kwargs)
            ],
        )

    def test_basic(self) -> None:
        self.expect(["a", "bc"], list("abc"), 2)

    def test_twenty_items_in_twenty_batches(self) -> None:
        self.expect(
            list("abcdefghijklmnopqrst"),
            list("abcdefghijklmnopqrst"),
            20,
        )

    def test_twenty_six_items_in_twenty_batches(self) -> None:
        self.expect(
            list("abcdefghijklmnop") + ["qr", "st", "uv", "wxyz"],
            list("abcdefghijklmnopqrstuvwxyz"),
            20,
        )
