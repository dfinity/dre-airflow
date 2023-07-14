"""
Rollout IC os to subnets.
"""

import datetime
from typing import Any, Callable, List, Dict

from dfinity.ic_types import SubnetRolloutInstance

SLACK_CHANNEL = "#eng-release-bots"
SLACK_CONNECTION_ID = "slack.ic_os_rollout"


def week_planner(now: datetime.datetime | None = None) -> dict[str, datetime.datetime]:
    if now is None:
        now = datetime.datetime.utcnow()
    days = {
        "Monday": now - datetime.timedelta(days=now.weekday()),
        "Tuesday": now - datetime.timedelta(days=now.weekday() - 1),
        "Wednesday": now - datetime.timedelta(days=now.weekday() - 2),
        "Thursday": now - datetime.timedelta(days=now.weekday() - 3),
        "Friday": now - datetime.timedelta(days=now.weekday() - 4),
        "Saturday": now - datetime.timedelta(days=now.weekday() - 5),
        "Sunday": now - datetime.timedelta(days=now.weekday() - 6),
    }
    for k, v in list(days.items()):
        days[k + " next week"] = v + datetime.timedelta(days=7)
    for k, v in list(days.items()):
        days[k] = v.replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
            tzinfo=datetime.timezone.utc,
        )
    if now.weekday() in (5, 6):
        for k, v in list(days.items()):
            days[k] = v + datetime.timedelta(days=7)
    return days


def rollout_planner(
    plan: dict[str, Any],
    subnet_list_source: Callable[[], list[str]],
    now: datetime.datetime | None = None,
) -> list[SubnetRolloutInstance]:
    res = []
    subnet_list = subnet_list_source()
    week_plan = week_planner(now)
    for dayname, hours in plan.items():
        try:
            date = week_plan[dayname]
        except KeyError:
            raise ValueError(f"{dayname} is not a valid day")
        for time_s, subnet_numbers in hours.items():
            org_time_s = time_s
            if isinstance(time_s, int):
                hour = int(time_s / 60)
                minute = time_s % 60
                time_s = f"{hour}:{minute}"
            try:
                time = datetime.datetime.strptime(time_s, "%H:%M")
            except Exception as exc:
                raise ValueError(
                    f"{org_time_s} is not a valid hh:mm time on {dayname}"
                ) from exc
            date_and_time = date.replace(hour=time.hour, minute=time.minute)
            for n, sn in enumerate(subnet_numbers):
                if isinstance(sn, str):
                    prefix_matches = [
                        (m, s) for m, s in enumerate(subnet_list) if s.startswith(sn)
                    ]
                    if not prefix_matches:
                        raise ValueError(
                            f"subnet specification {sn} not in known subnet list"
                            " for this network"
                        )
                    if len(prefix_matches) > 1:
                        raise ValueError(f"subnet specification {sn} is ambiguous")
                    subnet_numbers[n] = prefix_matches[0][0]
                elif not isinstance(sn, int) or sn < 0:
                    raise ValueError(
                        f"subnet number {sn} in {hours} of {dayname} is"
                        " not a zero/positive integer or a valid subnet ID"
                    )
            for sn in subnet_numbers:
                if sn >= len(subnet_list):
                    raise ValueError(
                        f"subnet number {sn} in {hours} of {dayname} is"
                        " larger than the known total number of subnets"
                    )
            for sn in subnet_numbers:
                res.append(SubnetRolloutInstance(date_and_time, sn, subnet_list[sn]))
    return res

def rollout_planner_static(
    plan: dict[str, dict[str, List[int]]],
    subnet_list_source: Callable[[], list[str]],
    now: datetime.datetime | None = None,
) -> Dict[str, List[SubnetRolloutInstance]]:
    res = dict()
    subnet_list = subnet_list_source()
    week_plan = week_planner(now)
    for dayname, hours in plan.items():
        try:
            date = week_plan[dayname]
        except KeyError:
            raise ValueError(f"{dayname} is not a valid day")
        for time_s, subnet_numbers in hours.items():
            org_time_s = time_s
            if isinstance(time_s, int):
                hour = int(time_s / 60)
                minute = time_s % 60
                time_s = f"{hour}:{minute}"
            try:
                time = datetime.datetime.strptime(time_s, "%H:%M")
            except Exception as exc:
                raise ValueError(
                    f"{org_time_s} is not a valid hh:mm time on {dayname}"
                ) from exc
            date_and_time = date.replace(hour=time.hour, minute=time.minute)
            date_and_time_str = date_and_time.strftime("%A_%B_%d__%Y_%I_%M_%S_%p")
            res[date_and_time_str] = list()

            for n, sn in enumerate(subnet_numbers):
                if isinstance(sn, str):
                    prefix_matches = [
                        (m, s) for m, s in enumerate(subnet_list) if s.startswith(sn)
                    ]
                    if not prefix_matches:
                        raise ValueError(
                            f"subnet specification {sn} not in known subnet list"
                            " for this network"
                        )
                    if len(prefix_matches) > 1:
                        raise ValueError(f"subnet specification {sn} is ambiguous")
                    subnet_numbers[n] = prefix_matches[0][0]
                elif not isinstance(sn, int) or sn < 0:
                    raise ValueError(
                        f"subnet number {sn} in {hours} of {dayname} is"
                        " not a zero/positive integer or a valid subnet ID"
                    )
            for sn in subnet_numbers:
                if sn >= len(subnet_list):
                    raise ValueError(
                        f"subnet number {sn} in {hours} of {dayname} is"
                        " larger than the known total number of subnets"
                    )
            
            for sn in subnet_numbers:
                res[date_and_time_str].append(SubnetRolloutInstance(date_and_time, sn, subnet_list[sn]))
    return res
