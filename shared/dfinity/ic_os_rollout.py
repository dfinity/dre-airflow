"""
Rollout IC os to subnets.
"""

import datetime
import re
import textwrap
from typing import Callable, TypeAlias, TypedDict, cast

from dfinity.ic_types import (
    SubnetRolloutInstance,
    SubnetRolloutInstanceWithRevision,
)
from dfinity.rollout_types import (
    SubnetNameOrNumber,
    SubnetNameOrNumberWithRevision,
    SubnetNumberWithRevision,
    SubnetOrderSpec,
    SubnetRolloutPlanSpec,
    SubnetRolloutPlanSpec_doc,
)

SLACK_CHANNEL = "#eng-release-bots"
SLACK_CONNECTION_ID = "slack.ic_os_rollout"
DR_DRE_SLACK_ID = "S05GPUNS7EX"
MAX_BATCHES: int = 30

DEFAULT_SUBNET_ROLLOUT_PLANS: dict[str, str] = {
    "mainnet": """
# See documentation at the end of this configuration block.
Monday:
  9:00:      [io67a]
  11:00:     [shefu, fuqsr]
Tuesday:
  7:00:      [pjljw, qdvhd, 2fq7c]
  9:00:      [snjp4, w4asl, qxesv]
  11:00:     [4zbus, ejbmu, uzr34]
Wednesday:
  7:00:      [pae4o, 5kdm2, csyj4]
  9:00:      [eq6en, lhg73, brlsh]
  11:00:     [k44fs, cv73p, 4ecnw]
  13:00:     [opn46, lspz2, o3ow2]
Thursday:
  7:00:      [w4rem, 6pbhf, e66qm]
  9:00:      [yinp6, bkfrj, jtdsg]
  11:00:     [mpubz, x33ed, gmq5v]
  13:00:     [3hhby, nl6hn, pzp6e]
Monday next week:
  7:00:
    subnets: [tdb26]
    batch: 30
"""
    + (
        "# "
        + "# ".join(textwrap.dedent(SubnetRolloutPlanSpec_doc).strip().splitlines(True))
    )
}
PLAN_FORM = """
    <textarea class="form-control" name="{name}" 
           id="{name}" placeholder=""
           type="text"
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
