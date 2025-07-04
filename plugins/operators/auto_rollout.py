"""
IC-OS rollout operators.
"""

import datetime
from typing import Any, cast

import yaml
from dfinity.rollout_types import (
    Releases,
    RolloutFeatures,
    SubnetNameOrNumber,
    SubnetNameOrNumberWithRevision,
    SubnetRolloutPlanSpec,
    yaml_to_ApiBoundaryNodeRolloutPlanSpec,
)

from airflow.exceptions import DagRunAlreadyExists
from airflow.models.baseoperator import BaseOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.context import Context


def next_weekday(d: datetime.date, weekday: int) -> datetime.date:
    days_ahead = weekday - d.weekday()
    if days_ahead <= 0:  # Target day already happened this week
        days_ahead += 7
    return d + datetime.timedelta(days_ahead)


class AutoComputeRolloutPlan(BaseOperator):
    template_fields = [
        "max_days_lookbehind",
        "guestos_rollout_plan",
        "api_boundary_nodes_rollout_plan",
    ]

    def __init__(
        self,
        release_versions_data_task_id: str,
        feature_rollout_plan_task_id: str,
        max_days_lookbehind: int,
        guestos_rollout_plan: str,
        api_boundary_nodes_rollout_plan: str,
        _ignored: Any = None,
        **kwargs: Any,
    ) -> None:
        self.release_versions_data_task_id = release_versions_data_task_id
        self.feature_rollout_plan_task_id = feature_rollout_plan_task_id
        self.max_days_lookbehind = max_days_lookbehind
        self.guestos_rollout_plan = guestos_rollout_plan
        self.api_boundary_nodes_rollout_plan = api_boundary_nodes_rollout_plan
        super().__init__(**kwargs)

    def execute(self, context: Context) -> tuple[str, str, str, str]:
        release_versions = cast(
            Releases,
            context["task_instance"].xcom_pull(
                task_ids=self.release_versions_data_task_id,
            ),
        )
        rollout_features = cast(
            RolloutFeatures,
            context["task_instance"].xcom_pull(
                task_ids=self.feature_rollout_plan_task_id,
            ),
        )

        max_days_lookbehind = int(self.max_days_lookbehind)
        # Select latest release no later than now and no earlier than the last X days.
        now = datetime.datetime.now().replace(tzinfo=datetime.timezone.utc)
        self.log.info('Using %s as "now" date/time for rollout computation', now)
        xdaysago = now - datetime.timedelta(days=max_days_lookbehind)
        print("::group::Dates")  # This will work in Airflow 2.9.x and above.
        self.log.info("Now: %s", now)
        for r in release_versions:
            self.log.info("%s: %s", r["rc_name"], r["rc_date"])
        print("::endgroup::")
        release_versions = [
            r
            for r in release_versions
            if r["rc_date"] < now and r["rc_date"] >= xdaysago
        ]
        try:
            selected_release = list(
                sorted(release_versions, key=lambda m: m["rc_date"])
            )[-1]
            self.log.info("Selected release: %s", selected_release)
        except IndexError:
            v = (
                f"Release list contains no releases before {now} and after {xdaysago};"
                " if you are running this later than expected, re-run this DAG"
                " with an increased maximum lookbehind in days"
            )
            self.log.error("%s.  Aborting.", v)
            raise ValueError(v)

        # Select rollout features depending on the day of the week of this rollout.
        # If today is a Saturday or a Sunday, assume the rollout feature plan will
        # be next Monday's.  Else use the most recent feature plan no later than today,
        # and no earlier than the last X days, to prevent accidental selection of a
        # feature plan from two, three weeks ago.
        if now.weekday() in [5, 6]:
            next_monday = next_weekday(now.date(), 0)
            self.log.info(
                "It's a weekend; using next Monday %s to select a feature plan",
                next_monday,
            )
            try:
                feature_plan = [
                    f for f in rollout_features if f["date"] == next_monday
                ][0]
                subnet_id_feature_map = feature_plan["subnet_id_feature_map"]
                self.log.info(
                    "Feature map with date %s selected: %s",
                    feature_plan["date"],
                    subnet_id_feature_map,
                )
            except IndexError:
                # No feature plan for next week.
                subnet_id_feature_map = {}
                self.log.info("No feature map for next Monday")
        else:
            self.log.info(
                "It's a weekday; looking for feature plan no later than today"
            )
            feature_plans = [
                f
                for f in rollout_features
                if (f["date"] <= now.date() and f["date"] >= xdaysago.date())
            ]
            if feature_plans:
                feature_plan = list(sorted(feature_plans, key=lambda m: m["date"]))[-1]
                subnet_id_feature_map = feature_plan["subnet_id_feature_map"]
                self.log.info(
                    "Feature map with date %s selected: %s",
                    feature_plan["date"],
                    subnet_id_feature_map,
                )
            else:
                # No feature plan for this week.
                subnet_id_feature_map = {}
                self.log.info(
                    "No feature map for any of the last %s days", max_days_lookbehind
                )

        selected_release_versions: dict[str, str] = {}
        for vv in selected_release["versions"]:
            selected_release_versions[vv["name"]] = vv["version"]

        def add_release(
            subnet: SubnetNameOrNumber | SubnetNameOrNumberWithRevision,
        ) -> SubnetNameOrNumberWithRevision:
            if isinstance(subnet, dict) and subnet.get("git_revision"):
                # Manually overridden, return the same.
                return subnet

            if isinstance(subnet, dict):
                subnet = subnet["subnet"]

            if isinstance(subnet, str):
                for featured_subnet, feature in subnet_id_feature_map.items():
                    if feature not in selected_release_versions:
                        raise ValueError(
                            f"Cannot find a variant named {feature} among the selected"
                            f" feature map {subnet_id_feature_map} in the selected"
                            f" release {selected_release}"
                        )
                    if featured_subnet.startswith(subnet):
                        return {
                            "subnet": subnet,
                            "git_revision": selected_release_versions[feature],
                        }

                if "base" not in selected_release_versions:
                    raise ValueError(
                        f"Cannot find a variant named base in the selected"
                        f" release {selected_release}"
                    )
                return {
                    "subnet": subnet,
                    "git_revision": selected_release_versions["base"],
                }

            raise ValueError(
                "Do not know how to deal with subnet %r typed %s", subnet, type(subnet)
            )

        # Now compute rollout map.
        spec = cast(SubnetRolloutPlanSpec, yaml.safe_load(self.guestos_rollout_plan))

        for hours in spec.values():
            for subnets in hours.values():
                if isinstance(subnets, dict):
                    if "subnets" in subnets:
                        subnets = subnets["subnets"]
                        for n, subnet in enumerate(subnets):
                            subnets[n] = add_release(subnet)
                elif isinstance(subnets, list):
                    for n, subnet in enumerate(subnets):
                        subnets[n] = add_release(subnet)

        yamlified_spec = yaml.safe_dump(spec, sort_keys=False)
        self.log.info("GuestOS rollout plan prepared:\n%s", yamlified_spec)

        # Also parse the rollout plan spec.
        apibnspec = yaml_to_ApiBoundaryNodeRolloutPlanSpec(
            self.api_boundary_nodes_rollout_plan
        )
        yamlified_apibnspec = yaml.safe_dump(apibnspec, sort_keys=False)
        self.log.info(
            "Rollout plan for API boundary nodes prepared:\n%s", yamlified_apibnspec
        )

        self.log.info("Base version of release: %s", selected_release_versions["base"])

        return (
            selected_release["rc_name"],
            selected_release_versions["base"],
            yamlified_spec,
            yamlified_apibnspec,
        )


class TriggerGuestOSRollout(TriggerDagRunOperator):
    def __init__(
        self, plan_task_id: str, simulate_rollout: bool, *args: Any, **kwargs: Any
    ) -> None:
        self.plan_task_id = plan_task_id
        self.simulate_rollout = simulate_rollout
        TriggerDagRunOperator.__init__(self, *args, **kwargs)

    def execute(self, context: Context) -> None:
        rc_name, base_git_revision, rollout_plan, _ = cast(
            tuple[str, str, str, str],
            context["task_instance"].xcom_pull(task_ids=self.plan_task_id),
        )
        self.conf = dict(
            git_revision=base_git_revision,
            plan=rollout_plan,
            simulate=self.simulate_rollout,
        )
        try:
            # Optimistically trigger DAG.
            self.trigger_run_id = f"{rc_name}"
            super().execute(context)
        except DagRunAlreadyExists:
            # Oh, another DAG already triggered with the same name.
            # Trigger the DAG with a second name.  This second DAG
            # will pause until all prior DAGs have finished, so for
            # the new DAG run to continue, the prior DAG will have
            # to be canceled by the operator.
            retrigger_date = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
            self.trigger_run_id = f"{rc_name}_triggered_at_{retrigger_date}"
            super().execute(context)


class TriggerAPIBoundaryNodesRollout(TriggerDagRunOperator):
    def __init__(
        self, plan_task_id: str, simulate_rollout: bool, *args: Any, **kwargs: Any
    ) -> None:
        self.plan_task_id = plan_task_id
        self.simulate_rollout = simulate_rollout
        TriggerDagRunOperator.__init__(self, *args, **kwargs)

    def execute(self, context: Context) -> None:
        rc_name, base_git_revision, _, rollout_plan = cast(
            tuple[str, str, str, str],
            context["task_instance"].xcom_pull(task_ids=self.plan_task_id),
        )
        self.conf = dict(
            git_revision=base_git_revision,
            simulate=self.simulate_rollout,
        )
        try:
            # Optimistically trigger DAG.
            self.trigger_run_id = f"{rc_name}"
            super().execute(context)
        except DagRunAlreadyExists:
            # Oh, another DAG already triggered with the same name.
            # Trigger the DAG with a second name.  This second DAG
            # will pause until all prior DAGs have finished, so for
            # the new DAG run to continue, the prior DAG will have
            # to be canceled by the operator.
            retrigger_date = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
            self.trigger_run_id = f"{rc_name}_triggered_at_{retrigger_date}"
            super().execute(context)
