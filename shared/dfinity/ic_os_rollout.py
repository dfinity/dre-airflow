"""
Rollout IC os to subnets.
"""

import datetime
import re
from typing import Callable, TypeAlias, TypedDict, cast

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
RolloutPlan: TypeAlias = dict[
    str, tuple[datetime.datetime, list[SubnetRolloutInstance]]
]


SubnetNameOrNumber: TypeAlias = int | str


class SubnetNameOrNumberWithRevision(TypedDict):
    subnet: SubnetNameOrNumber
    git_revision: str | None


class SubnetNumberWithRevision(TypedDict):
    subnet: int
    git_revision: str | None


class SubnetOrderSpec(TypedDict):
    subnets: list[SubnetNameOrNumber | SubnetNameOrNumberWithRevision]
    batch: int | None


def rollout_planner(
    plan: dict[
        str,
        dict[
            str | int,
            list[SubnetNameOrNumber | SubnetNameOrNumberWithRevision] | SubnetOrderSpec,
        ],
    ],
    subnet_list_source: Callable[[], list[str]],
    now: datetime.datetime | None = None,
) -> RolloutPlan:
    subnet_list = subnet_list_source()
    week_plan = week_planner(now)
    batches: RolloutPlan = {}
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
                        f" already being rolled out as part of batch {bsn+1}"
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
