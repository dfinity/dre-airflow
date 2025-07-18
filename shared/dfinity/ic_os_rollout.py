"""
Rollout IC os to subnets.
"""

import datetime
import re
from typing import Callable, NotRequired, TypeAlias, TypedDict, cast

from dfinity.ic_types import (
    SubnetRolloutInstance,
    SubnetRolloutInstanceWithRevision,
)
from dfinity.rollout_types import (
    ApiBoundaryNodeRolloutPlanSpec,
    DaysOfWeek,
    HostOSRolloutPlanSpec,
    ProvisionalHostOSPlan,
    SubnetNameOrNumber,
    SubnetNameOrNumberWithRevision,
    SubnetNumberWithRevision,
    SubnetOrderSpec,
    SubnetRolloutPlanSpec,
)

SLACK_CHANNEL = "#eng-release-bots"
SLACK_CONNECTION_ID = "slack.ic_os_rollout"
DR_DRE_SLACK_ID = "S05GPUNS7EX"
MAX_BATCHES: int = 30

# To be deleted when we upgrade to Airflow 2.11.
PLAN_FORM = """
    <textarea class="form-control" name="{name}" 
           id="{name}" placeholder=""
           type="text"
           style="font-family: monospace"
           required="" rows="24">{value}</textarea>
"""


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


class SubnetIdWithRevision(TypedDict):
    subnet_id: str
    git_revision: str


def subnet_id_and_git_revision_from_args(
    original_subnet_id: str | SubnetIdWithRevision,
    original_git_revision: str | int,
) -> tuple[str, str]:
    if isinstance(original_git_revision, int):
        # Pad with zeroes in front, as Airflow "helpfully" downcast it.
        git_revision = "{:040}".format(original_git_revision)
    else:
        git_revision = original_git_revision
    if isinstance(original_subnet_id, dict):
        subnet_id = original_subnet_id["subnet_id"]
        git_revision = original_subnet_id["git_revision"]
    else:
        subnet_id = original_subnet_id
    return subnet_id, git_revision


"""Zero-indexed rollout plan with batches of subnet instances to roll out."""
SubnetRolloutPlan: TypeAlias = dict[
    str, tuple[datetime.datetime, list[SubnetRolloutInstance]]
]

SubnetRolloutPlanWithRevision: TypeAlias = dict[
    str, tuple[datetime.datetime, list[SubnetRolloutInstanceWithRevision]]
]


def assign_default_revision(
    r: SubnetRolloutPlan, fallback_git_revision: str
) -> SubnetRolloutPlanWithRevision:
    finalplan: SubnetRolloutPlanWithRevision = {}
    for nstr, (start_time, members) in r.items():
        finalmembers: list[SubnetRolloutInstanceWithRevision] = []
        for item in members:
            finalmembers.append(
                SubnetRolloutInstanceWithRevision(
                    item.start_at,
                    item.subnet_num,
                    item.subnet_id,
                    item.git_revision or fallback_git_revision,
                )
            )
        finalplan[nstr] = (start_time, finalmembers)
    return finalplan


def convert_timespec_or_minutes_to_datetime(
    time_s: str | int,
) -> datetime.datetime:
    org_time_s = time_s
    if isinstance(time_s, int):
        hour = int(time_s / 60)
        minute = time_s % 60
        time_s = f"{hour}:{minute}"
    try:
        return datetime.datetime.strptime(time_s, "%H:%M")
    except Exception as exc:
        raise ValueError(f"{org_time_s} is not a valid hh:mm time") from exc


def next_day_of_the_week(
    day_of_the_week: str, now: datetime.datetime | None = None
) -> datetime.datetime:
    """
    Given a day of the week, create a date/time whose time is the same as now, but the
    date is the same day of the week seven days from now.

    If the given day of the week is the same as now, this will return a date/time
    referring to today, rather than the next week.  To refer to the same day next week,
    pass "X next week" as the day of the week.  To give a clearer example, if this
    function runs at 14:00 of Monday, and the passed day of the week is "Monday",
    then the returned date/time will be today (Monday) at 14:00.
    """

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
        if v < now:
            days[k] = v + datetime.timedelta(days=7)
    for k, v in list(days.items()):
        days[k + " next week"] = v + datetime.timedelta(days=7)
    try:
        return days[day_of_the_week]
    except KeyError:
        raise ValueError("Invalid day of the week %r" % day_of_the_week)


def rollout_planner(
    plan: SubnetRolloutPlanSpec,
    subnet_list_source: Callable[[], list[str]],
    now: datetime.datetime | None = None,
) -> SubnetRolloutPlan:
    subnet_list = subnet_list_source()
    week_plan = week_planner(now)
    batches: SubnetRolloutPlan = {}
    current_batch_index: int = 0
    batch_index_for_subnet: dict[int, int] = {}

    # Convert date specifications to dates and times.
    for dayname, hours in sorted(plan.items(), key=lambda m: week_plan[m[0]]):
        try:
            date = week_plan[dayname]
        except KeyError:
            raise ValueError(f"{dayname} is not a valid day")

        realhours: dict[
            datetime.datetime,
            list[SubnetNameOrNumber | SubnetNameOrNumberWithRevision] | SubnetOrderSpec,
        ] = {}

        for time_s, subnet_numbers_raw in hours.items():
            try:
                time = convert_timespec_or_minutes_to_datetime(time_s)
            except ValueError as e:
                raise ValueError(str(e) + f" on {dayname}") from e
            date_and_time = date.replace(hour=time.hour, minute=time.minute)
            realhours[date_and_time] = subnet_numbers_raw

        # Convert subnet specifications to lists of subnets and Git
        # revisions, grouped and ordered by time and date, and
        # associated with their respective batches.
        for date_and_time, subnet_numbers_list_or_dict in sorted(
            realhours.items(), key=lambda m: m[0]
        ):
            if isinstance(subnet_numbers_list_or_dict, dict):
                try:
                    subnets_list = subnet_numbers_list_or_dict["subnets"]
                except KeyError:
                    raise ValueError(
                        f"subnets {subnet_numbers_list_or_dict} are improperly"
                        ' specified, lacking a "subnets" entry'
                    )
                batch_number = subnet_numbers_list_or_dict.get("batch")
                if batch_number is None:
                    batch_index: int = current_batch_index
                else:
                    batch_index = batch_number - 1
            else:
                subnets_list = subnet_numbers_list_or_dict
                batch_index = current_batch_index

            def convert(
                ww: SubnetNameOrNumber | SubnetNameOrNumberWithRevision,
            ) -> SubnetNameOrNumberWithRevision:
                if isinstance(ww, dict):
                    if "subnet" not in ww:
                        raise ValueError(
                            f"subnet {ww} is"
                            '  improperly specified, lacking a "subnet" entry'
                        )
                    return ww
                else:
                    return {"subnet": ww, "git_revision": None}

            subnets_and_revs = [convert(x) for x in subnets_list]

            if str(batch_index) in batches:
                raise ValueError(
                    f"batch {batch_index + 1} requested by {date_and_time} has"
                    f" already been assigned to a prior batch"
                )
            if batch_index >= MAX_BATCHES:
                raise ValueError(
                    f"batch {batch_index + 1} requested by {date_and_time} exceeds"
                    f" the maximum batch count of {MAX_BATCHES}"
                )
            if batch_index < current_batch_index:
                raise ValueError(
                    f"batch {batch_index + 1} requested by {date_and_time} is"
                    f" lower than already-assigned batch {current_batch_index}"
                )

            for n, snwrev in enumerate(subnets_and_revs):
                # Let's first check that only two keys are accepted here.
                checked = dict(snwrev)
                for key in ("subnet", "git_revision"):
                    if key in checked:
                        del checked[key]

                sn = snwrev["subnet"]
                if len(checked) > 0:
                    raise ValueError(
                        f"subnet specification {sn} contains extraneous keys"
                        f" {', '.join(repr(s) for s in checked.keys())}"
                    )

                # Now let's check subnet validity.
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
                    subnets_and_revs[n]["subnet"] = prefix_matches[0][0]
                elif not isinstance(sn, int) or sn < 0:
                    raise ValueError(
                        f"subnet number {sn} requested by {date_and_time} is"
                        " not a zero/positive integer or a valid subnet ID"
                    )
            subnet_numbers_and_revs = cast(
                list[SubnetNumberWithRevision], subnets_and_revs
            )

            batches[str(batch_index)] = (date_and_time, [])
            for subnet in subnet_numbers_and_revs:
                subnet_number = subnet["subnet"]
                if subnet_number >= len(subnet_list):
                    raise ValueError(
                        f"subnet number {subnet_number} requested by"
                        f" {date_and_time} is larger than the known"
                        " total number of subnets"
                    )

                principal = subnet_list[subnet_number]
                bsn = batch_index_for_subnet.get(subnet_number)
                if bsn is not None:
                    raise ValueError(
                        f"subnet number {principal} requested by {date_and_time} is"
                        f" already being rolled out as part of batch {bsn + 1}"
                    )
                batch_index_for_subnet[subnet_number] = batch_index
                git_revision = subnet.get("git_revision")
                if git_revision is not None and not re.match(
                    "^[0-9a-f]{40}$", git_revision
                ):
                    raise ValueError(
                        f"subnet number {principal} requested by {date_and_time} is"
                        f" specifies invalid git revision {subnet['git_revision']}"
                    )

                batches[str(batch_index)][1].append(
                    SubnetRolloutInstance(
                        date_and_time,
                        subnet_number,
                        principal,
                        git_revision,
                    ),
                )

            current_batch_index = batch_index + 1

    return batches


def check_plan(plan: SubnetRolloutPlanWithRevision) -> None:
    # Check that uzr34 predates pzp6e by at least 24 hours.
    # https://dfinity.atlassian.net/browse/REL-2675 .
    uzr34_start_time: datetime.datetime | None = None
    pzp6e_start_time: datetime.datetime | None = None
    for _, batch in plan.values():
        for subnet in batch:
            if subnet.subnet_id.startswith("uzr34"):
                uzr34_start_time = subnet.start_at
            if subnet.subnet_id.startswith("pzp6e"):
                pzp6e_start_time = subnet.start_at
    if uzr34_start_time and pzp6e_start_time:
        delta = pzp6e_start_time - uzr34_start_time
        if delta < datetime.timedelta(days=1):
            raise ValueError(
                (
                    "The update time %s of pzp6e is too "
                    "close to the update time %s of uzr34 (%s)"
                )
                % (pzp6e_start_time, uzr34_start_time, delta)
            )


class TimetableSpec(TypedDict):
    resume_at: str
    suspend_at: str
    minimum_minutes_per_batch: int
    allowed_days: NotRequired[list[DaysOfWeek]]
    start_day: NotRequired[DaysOfWeek]


def api_boundary_node_batch_timetable(
    spec: TimetableSpec
    | ApiBoundaryNodeRolloutPlanSpec
    | HostOSRolloutPlanSpec
    | ProvisionalHostOSPlan,
    batch_count: int,
    now: datetime.datetime | None = None,
) -> list[datetime.datetime]:
    """
    Formulates a timetable (list of datetimes) with a batch_count number of elements
    based on the plan spec passed, with each element respecting the time window
    between resume_at and suspend_at of the spec, staggered by the number of
    minimum_minutes_per_batch in the spec.  The start_day in the spec (optional)
    defines which day is picked for the first item in the timetable -- if absent,
    the day will correspond to now (or the value of now passed to this function).
    If the spec does not contain allowed_days, that will default to weekdays.
    """
    if batch_count < 1:
        raise ValueError("batch_count must be a positive integer")

    run_at = convert_timespec_or_minutes_to_datetime(spec["resume_at"])
    stop_at = convert_timespec_or_minutes_to_datetime(spec["suspend_at"])

    increment = datetime.timedelta(minutes=spec["minimum_minutes_per_batch"])

    if run_at < stop_at:
        if increment > stop_at - run_at:
            raise ValueError(
                f"the minimum time per batch {increment} is smaller than the operating"
                f" window of time {stop_at - run_at}"
            )
    elif run_at > stop_at:
        if increment > run_at - stop_at:
            raise ValueError(
                f"the minimum time per batch {increment} is smaller than the operating"
                f" window of time {run_at - stop_at}"
            )
    else:
        raise ValueError(
            f"cannot use the same resume and suspend times ({spec['resume_at']}"
            f" and {spec['suspend_at']})"
        )

    if now is None:
        now = datetime.datetime.now()

    def at(now: datetime.datetime, tgt: datetime.datetime) -> datetime.datetime:
        return now.replace(
            hour=tgt.hour,
            minute=tgt.minute,
            second=tgt.second,
            microsecond=tgt.microsecond,
        )

    def hm(d: datetime.datetime) -> float:
        return d.hour * 3600 + d.minute * 60 + d.second + d.microsecond / 1000000

    allowed_days = spec.get("allowed_days") or [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
    ]

    batch_start_time = (
        at(
            next_day_of_the_week(spec["start_day"], now=now),
            run_at,
        )
        if "start_day" in spec
        else at(now, run_at)
    )
    batches = []
    tries = 0
    max_tries = batch_count * 24 * 60 / spec["minimum_minutes_per_batch"]
    for _ in range(batch_count):
        while True:
            tries = tries + 1
            if tries > max_tries:
                raise ValueError(
                    "could not find a timetable in a reasonable amount of attempts"
                )
            run_at = at(batch_start_time, run_at)
            stop_at = at(batch_start_time, stop_at)
            if run_at > stop_at:
                if batch_start_time < stop_at:
                    run_at = run_at - datetime.timedelta(days=1)
                elif batch_start_time >= run_at:
                    stop_at = stop_at + datetime.timedelta(days=1)
            if (
                run_at <= batch_start_time <= (stop_at - increment)
                and batch_start_time.strftime("%A") in allowed_days
                and batch_start_time >= now
            ):
                break
            if (
                batch_start_time.strftime("%A") not in allowed_days
                or batch_start_time < now
            ):
                tries = tries - 1
            batch_start_time = batch_start_time + increment
        batches.append(batch_start_time)
        batch_start_time = batch_start_time + increment
    return batches


def exponential_increase(
    count: int,
    batch_count: int,
    increase_factor: float = 0.2,
) -> list[int]:
    """
    Apportion a count (positive integer) of things into a number of
    batches given by batch_count.  Returns a list of integers, batch_count
    in length, where each item is the number of things to be processed in
    that batch.

    The apportionment of things to process per batch follows an exponential
    increase given by the `increase_factor variable`.  The function will
    ensure that the sum of integers returned always matches the `batch_count`
    supplied by the caller.  Each batch returned will contain at least one
    item, provided that the count is greater than the batch count, at the
    expense of the apportionment curve, so if `count` is equal or slightly
    higher than `batch_count`, the returned batch sizes will be mostly ones
    with the small excess loaded to the end of the returned list.
    """
    try:
        assert increase_factor > 0.0, "increase_factor must be a positive float"
        assert count > 0, "count must be a positive integer"
        assert batch_count > 0, "batch_count must be a positive integer"
    except AssertionError as e:
        raise ValueError(str(e))

    batches: list[int] = [0 for _ in range(batch_count)]
    remaining = count
    for n, b in enumerate(batches):
        if remaining > 0:
            batches[n] = 1
            remaining = remaining - 1
        else:
            break
    if remaining == 0:
        return batches

    remaining_batches: list[float] = [1 + increase_factor]
    while len(remaining_batches) < batch_count:
        remaining_batches.append(remaining_batches[-1] * (1 + increase_factor))
    scalefactor = sum(remaining_batches)
    scaled = [b / scalefactor for b in remaining_batches]
    rounded = [round(b * (count - sum(batches))) for b in scaled]

    # Due to inevitable imprecision when rounded, we must fudge some values.
    while sum(rounded) > count - len(batches):
        for n, b in enumerate(rounded):
            if b == 0:
                continue
            if sum(rounded) == count:
                break
            rounded[n] = rounded[n] - 1
    while sum(rounded) < count - len(batches):
        rounded[-1] = rounded[-1] + 1

    while len(rounded) < len(batches):
        rounded.insert(0, 0)
    return [a + b for a, b in zip(batches, rounded)]


def api_boundary_node_batch_create(
    nodes: list[str], batch_count: int, increase_factor: float = 0.2
) -> list[list[str]]:
    """
    Creates a list of batches from the provided node list.

    The size of each batch follows an exponential distribution, with.
    one caveat: each batch will have at least one node, provided that
    there are sufficient nodes to apportion to each batch.
    """
    batch_sizes = exponential_increase(
        len(nodes), batch_count, increase_factor=increase_factor
    )
    assert sum(batch_sizes) == len(nodes), (batch_sizes, nodes)
    nodes = list(nodes)
    batches: list[list[str]] = []
    for sz in batch_sizes:
        batches.append([])
        while sz:
            batches[-1].append(nodes.pop(0))
            sz = sz - 1
    assert not nodes, "nodes remaining after apportionment: %s" % nodes
    return batches


if __name__ == "__main__":
    import pprint

    pprint.pprint(
        api_boundary_node_batch_timetable(
            {
                "nodes": ["abc"],
                "start_day": "Monday",
                "resume_at": "9:00",
                "suspend_at": "18:00",
                "minimum_minutes_per_batch": 60,
            },
            20,
        )
    )
