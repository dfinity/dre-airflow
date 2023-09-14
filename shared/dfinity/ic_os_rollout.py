"""
Rollout IC os to subnets.
"""

import datetime
from typing import Any, Callable, TypeAlias

from dfinity.ic_types import SubnetRolloutInstance

SLACK_CHANNEL = "#eng-release-bots"
SLACK_CONNECTION_ID = "slack.ic_os_rollout"
MAX_BATCHES: int = 30


def week_planner(now: datetime.datetime | None = None) -> dict[str, datetime.datetime]:
    if now is None:
        now = datetime.datetime.now()
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


"""Zero-indexed rollout plan with batches of subnet instances to roll out."""
RolloutPlan: TypeAlias = dict[
    str, tuple[datetime.datetime, list[SubnetRolloutInstance]]
]


def rollout_planner(
    plan: dict[str, Any],
    subnet_list_source: Callable[[], list[str]],
    now: datetime.datetime | None = None,
) -> RolloutPlan:
    subnet_list = subnet_list_source()
    week_plan = week_planner(now)
    batches: RolloutPlan = {}
    current_batch_index: int = 0
    batch_index_for_sn: dict[int, int] = {}

    for dayname, hours in sorted(plan.items(), key=lambda m: week_plan[m[0]]):
        try:
            date = week_plan[dayname]
        except KeyError:
            raise ValueError(f"{dayname} is not a valid day")

        realhours: dict[datetime.datetime, Any] = {}

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
            realhours[date_and_time] = subnet_numbers

        for date_and_time, subnet_numbers in sorted(
            realhours.items(), key=lambda m: m[0]
        ):
            if isinstance(subnet_numbers, dict):
                subnet_numbers, batch_number = subnet_numbers[
                    "subnets"
                ], subnet_numbers.get("batch")
                if batch_number is None:
                    batch_index = current_batch_index
                else:
                    batch_index = batch_number - 1
            else:
                batch_index = current_batch_index

            if str(batch_index) in batches:
                raise ValueError(
                    f"batch {batch_index+1} requested by {date_and_time} has"
                    f" already been assigned to a prior batch"
                )
            if batch_index >= MAX_BATCHES:
                raise ValueError(
                    f"batch {batch_index+1} requested by {date_and_time} exceeds"
                    f" the maximum batch count of {MAX_BATCHES}"
                )
            if batch_index < current_batch_index:
                raise ValueError(
                    f"batch {batch_index+1} requested by {date_and_time} is"
                    f" lower than already-assigned batch {current_batch_index}"
                )

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
                        f"subnet number {sn} requested by {date_and_time} is"
                        " not a zero/positive integer or a valid subnet ID"
                    )

            batches[str(batch_index)] = (date_and_time, [])
            for sn in subnet_numbers:
                principal = subnet_list[sn]
                if sn >= len(subnet_list):
                    raise ValueError(
                        f"subnet number {principal} requested by {date_and_time} is"
                        " larger than the known total number of subnets"
                    )

                bsn = batch_index_for_sn.get(sn)
                if bsn is not None:
                    raise ValueError(
                        f"subnet number {principal} requested by {date_and_time} is"
                        f" already being rolled out as part of batch {bsn+1}"
                    )
                batch_index_for_sn[sn] = batch_index

                batches[str(batch_index)][1].append(
                    SubnetRolloutInstance(date_and_time, sn, principal),
                )

            current_batch_index = batch_index + 1

    return batches
