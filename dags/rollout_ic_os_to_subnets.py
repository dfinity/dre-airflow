"""
Example DAG demonstrating the usage of the BashOperator and custom
TimedPythonOperator.
"""

import datetime
from typing import Any, Callable

import pendulum
import yaml  # type: ignore
from dfinity.ic_admin import get_subnet_list
from dfinity.types import SubnetRolloutInstance
from operators.ic_os_rollout import CreateProposalIdempotently
from sensors.ic_os_rollout import (
    CustomDateTimeSensorAsync,
    WaitForProposalAcceptance,
    WaitForReplicaRevisionUpdated,
    WaitUntilNoAlertsOnSubnet,
)

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models.param import Param

DEFAULT_PLAN = """
# See documentation at the end of this configuration block.
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
# Remarks:
# * All times are expressed in the UTC time zone.
# * Days refer to dates relative to your current work week
#   if starting a rollout during a workday, or next week if
#   the rollout is started during a weekend.
# * A day name with " next week" added at the end means
#   "add one week to this day".
"""

_PLAN_FORM = """
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


def rollout_planner(
    plan: dict[str, Any],
    now: datetime.datetime | None = None,
    subnet_list_source: Callable[[], list[str]] = get_subnet_list,
) -> list[SubnetRolloutInstance]:
    res = []
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
            for sn in subnet_numbers:
                if not isinstance(sn, int) or sn < 0:
                    raise ValueError(
                        f"subnet number {sn} in {hours} of {dayname} is"
                        " not a zero/positive integer"
                    )
            subnet_list = subnet_list_source()
            for sn in subnet_numbers:
                if sn >= len(subnet_list):
                    raise ValueError(
                        f"subnet number {sn} in {hours} of {dayname} is"
                        " larger than the total number of subnets"
                    )
            for sn in subnet_numbers:
                res.append(SubnetRolloutInstance(date_and_time, sn, subnet_list[sn]))
    return res


with DAG(
    dag_id="rollout_git_revision_to_subnets",
    schedule=None,
    start_date=pendulum.datetime(2020, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["testing"],
    params={
        "plan": Param(
            default=DEFAULT_PLAN.strip(),
            type="string",
            title="Rollout plan",
            description="A YAML-formatted string describing the rollout schedule",
            custom_html_form=_PLAN_FORM,
        ),
        "git_revision": Param(
            "0000000000000000000000000000000000000000",
            type="string",
            pattern="^[a-f0-9]{40}$",
            title="Git revision",
            description="Git revision of the IC-OS release to roll out to subnets",
        ),
    },
) as dag:

    @task
    def schedule(**context):  # type: ignore
        plan_data_structure = yaml.safe_load(
            context["task"].render_template("{{ params.plan }}", context)
        )
        return rollout_planner(plan_data_structure)

    @task_group()
    def per_subnet(x):  # type:ignore
        @task
        def report(x: SubnetRolloutInstance) -> datetime.datetime:
            print(
                f"Plan for subnet {x.subnet_num} with ID {x.subnet_id}"
                f" starts at {x.start_at}"
            )
            return x  # type:ignore

        rep = report(x)

        @task
        def to_start_at(x: SubnetRolloutInstance) -> str:
            """Adapt SRI start time to str, expected by CustomDateTimeSensorAsync."""
            return str(x.start_at)

        td = to_start_at(rep)

        @task
        def to_subnet_id(x: SubnetRolloutInstance) -> str:
            return x.subnet_id  # type:ignore

        ts = to_subnet_id(rep)

        (
            CustomDateTimeSensorAsync(
                task_id="wait_until_start_time",
                target_time=td,
            )
            >> CreateProposalIdempotently(
                task_id="create_proposal",
                subnet_id=ts,
                git_revision="{{ params.git_revision }}",
            )
            >> WaitForProposalAcceptance(
                task_id="wait_until_proposal_is_accepted",
                subnet_id=ts,
                git_revision="{{ params.git_revision }}",
            )
            >> WaitForReplicaRevisionUpdated(
                task_id="wait_for_replica_revision",
                subnet_id=ts,
                git_revision="{{ params.git_revision }}",
            )
            >> WaitUntilNoAlertsOnSubnet(
                task_id="wait_until_no_alerts_on_subnets",
                subnet_id=ts,
                git_revision="{{ params.git_revision }}",
            )
        )

    per_subnet.expand(x=schedule())


if __name__ == "__main__":
    dag.test()
