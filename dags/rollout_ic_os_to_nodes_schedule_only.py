# mypy: disable-error-code=unused-ignore
"""
Schedule-only DAG for IC OS HostOS rollout.

This DAG only runs the schedule task to show which nodes would be
selected for each batch. It does NOT perform any actual rollout actions.
"""

import datetime
import os
import sys

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from dfinity.ic_types import IC_NETWORKS
from operators import hostos_rollout as hostos_operators

from airflow import __version__

# Temporarily add the DAGs folder to import defaults.py.
sys.path.append(os.path.dirname(__file__))
try:
    from defaults import DEFAULT_HOSTOS_ROLLOUT_PLANS as DEFAULT_ROLLOUT_PLANS
finally:
    sys.path.pop()

if "2.9" in __version__:
    # To be deleted when we upgrade to Airflow 2.11.
    from dfinity.ic_os_rollout import PLAN_FORM

    format = dict(custom_html_form=PLAN_FORM)
else:
    format = {"format": "multiline"}

ROLLOUT_PLAN_HELP = """\
Represents the plan that the HostOS rollout will follow.

This is a SCHEDULE-ONLY DAG that shows which nodes would be rolled out
based on the plan configuration, without performing any actual rollout.
"""

for network_name, network in IC_NETWORKS.items():

    @dag(
        dag_id=f"rollout_ic_os_to_{network_name}_nodes_schedule_only",
        schedule=None,
        start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
        catchup=False,
        dagrun_timeout=datetime.timedelta(hours=24),
        tags=["rollout", "DRE", "IC OS", "HostOS", "testing"],
        render_template_as_native_obj=True,
        params={
            "git_revision": Param(
                "0000000000000000000000000000000000000000",
                type="string",
                pattern="^[a-f0-9]{40}$",
                title="Git revision",
                description="Git revision of the IC OS HostOS release to use for"
                " planning; the version does not need to be elected since this"
                " DAG only plans without performing actual rollout.",
            ),
            "plan": Param(
                default=DEFAULT_ROLLOUT_PLANS[network_name].strip(),
                type="string",
                title="Rollout plan",
                description_md=ROLLOUT_PLAN_HELP,
                **format,
            ),
        },
    )
    def rollout_ic_os_to_nodes_schedule_only() -> None:
        schedule = task(hostos_operators.schedule)
        schedule(network=network)  # type: ignore

    rollout_ic_os_to_nodes_schedule_only()
