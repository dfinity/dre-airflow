"""
Rollout IC os to subnets.
"""

import datetime
import functools
from typing import Any, Callable

import operators.ic_os_rollout as ic_os_rollout
import pendulum
import sensors.ic_os_rollout as ic_os_sensor
import yaml  # type: ignore
from dfinity.ic_admin import get_subnet_list
from dfinity.ic_api import IC_NETWORKS
from dfinity.ic_types import SubnetRolloutInstance

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models.param import Param

DEFAULT_PLANS: dict[str, str] = {
    "mainnet": """
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
# * Subnets may be specified by integer number from 0
#   to the maximum subnet number, or may be specified
#   as full or abbreviated subnet principal ID.
"""
}

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


DAGS: dict[str, DAG] = {}
for network_name, network in IC_NETWORKS.items():
    with DAG(
        dag_id=f"rollout_ic_os_to_{network_name}_subnets",
        schedule=None,
        start_date=pendulum.datetime(2020, 1, 1, tz="UTC"),
        catchup=False,
        dagrun_timeout=datetime.timedelta(days=14),
        tags=["testing"],
        params={
            "git_revision": Param(
                "0000000000000000000000000000000000000000",
                type="string",
                pattern="^[a-f0-9]{40}$",
                title="Git revision",
                description="Git revision of the IC-OS release to roll out to subnets",
            ),
            "plan": Param(
                default=DEFAULT_PLANS[network_name].strip(),
                type="string",
                title="Rollout plan",
                description="A YAML-formatted string describing the rollout schedule",
                custom_html_form=_PLAN_FORM,
            ),
            "simulate_proposal": Param(
                True,
                type="boolean",
                title="Simulate proposal",
                description="If enabled (the default), the update proposal will be"
                " simulated but not created, and its acceptance will be simulated too",
            ),
        },
    ) as dag:
        DAGS[network_name] = dag
        retries = int(86400 / 60 / 5)  # one day worth of retries

        @task
        def schedule(**context):  # type: ignore
            plan_data_structure = yaml.safe_load(
                context["task"].render_template("{{ params.plan }}", context)
            )
            ic_admin_version = yaml.safe_load(
                context["task"].render_template("{{ params.git_revision }}", context)
            )
            kwargs = {"network": network}
            if not ic_admin_version == "0000000000000000000000000000000000000000":
                kwargs["ic_admin_version"] = ic_admin_version
            subnet_list_source = functools.partial(get_subnet_list, **kwargs)
            return rollout_planner(
                plan_data_structure,
                subnet_list_source=subnet_list_source,
            )

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
                """
                Adapt SRI start time to str, expected by CustomDateTimeSensorAsync.\
                """
                return str(x.start_at)

            td = to_start_at(rep)

            @task
            def to_subnet_id(x: SubnetRolloutInstance) -> str:
                return x.subnet_id  # type:ignore

            ts = to_subnet_id(rep)

            (
                ic_os_sensor.CustomDateTimeSensorAsync(
                    task_id="wait_until_start_time",
                    target_time=td,
                )
                >> ic_os_rollout.CreateProposalIdempotently(
                    task_id="create_proposal",
                    subnet_id=ts,
                    git_revision="{{ params.git_revision }}",
                    simulate_proposal="{{ params.simulate_proposal }}",  # type:ignore
                    retries=retries,
                    network=network,
                )
                >> ic_os_sensor.WaitForProposalAcceptance(
                    task_id="wait_until_proposal_is_accepted",
                    subnet_id=ts,
                    git_revision="{{ params.git_revision }}",
                    simulate_proposal_acceptance="""{{
                        params.simulate_proposal
                    }}""",  # type:ignore
                    retries=retries,
                    network=network,
                )
                >> ic_os_sensor.WaitForReplicaRevisionUpdated(
                    task_id="wait_for_replica_revision",
                    subnet_id=ts,
                    git_revision="{{ params.git_revision }}",
                    retries=retries,
                    network=network,
                )
                >> ic_os_sensor.WaitUntilNoAlertsOnSubnet(
                    task_id="wait_until_no_alerts_on_subnets",
                    subnet_id=ts,
                    git_revision="{{ params.git_revision }}",
                    retries=retries,
                    network=network,
                )
            )

        per_subnet.expand(x=schedule())


if __name__ == "__main__":
    import os
    import sys

    try:
        rev = sys.argv[1]
    except Exception:
        print(
            "Error: to run this DAG you must specify the revision"
            " as the first argument on the command line, optionally"
            " with the rollout plan in YAML form as second argument.",
            file=sys.stderr,
        )
        sys.exit(os.EX_USAGE)
    try:
        plan = sys.argv[2]
    except Exception:
        plan = DEFAULT_PLANS["mainnet"]
    dag = DAGS["mainnet"]
    dag.test(
        run_conf={
            "git_revision": rev,
            "plan": plan,
        }
    )
