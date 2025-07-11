"""
IC-OS rollout operators.
"""

import collections
import datetime
import logging
import pprint
import typing
from copy import deepcopy
from typing import Literal, cast

import dfinity.dre as dre
import dfinity.ic_types as ic_types
import dfinity.rollout_types as rollout_types
from airflow.hooks.subprocess import SubprocessHook
from airflow.models.taskinstance import TaskInstance
from dfinity.ic_os_rollout import api_boundary_node_batch_timetable
from dfinity.rollout_types import (
    HostOSStage,
    NodeBatch,
    NodeSelectors,
    ProvisionalHostOSBatches,
    ProvisionalHostOSPlan,
    ProvisionalHostOSPlanBatch,
    yaml_to_HostOSRolloutPlanSpec,
)

LOGGER = logging.getLogger(__name__)

DFINITY: rollout_types.NodeProviderId = (
    "bvcsg-3od6r-jnydw-eysln-aql7w-td5zn-ay5m6-sibd2-jzojt-anwag-mqe"
)
CANARY_BATCH_COUNT: int = 5
MAIN_BATCH_COUNT: int = 50
UNASSIGNED_BATCH_COUNT: int = 15
MAX_NODES_PER_BATCH: int = 150


def node_info(n: dre.RegistryNode) -> rollout_types.NodeInfo:
    return {
        "node_id": n["node_id"],
        "node_provider_id": n["node_provider_id"],
        "dc_id": n["dc_id"],
        "subnet_id": n["subnet_id"],
        "status": n["status"],
    }


def apply_selectors(
    pool: list[dre.RegistryNode],
    selectors: rollout_types.NodeSelectors,
    dcs_owned_by_dfinity: set[rollout_types.DCId],
) -> tuple[list[rollout_types.NodeInfo], list[dre.RegistryNode]]:
    """
    Take as many nodes from the pool as selectors prescribe, then return info
    on those nodes and a new, reduced pool without those nodes
    """
    remaining_nodes = deepcopy(pool)
    for selector in selectors:
        if assignment := selector.get("assignment"):
            pool = [
                n
                for n in pool
                if (assignment == "unassigned" and n["subnet_id"] is None)
                or (assignment == "assigned" and n["subnet_id"] is not None)
            ]
        # assert 0, this_batch
        if owner := selector.get("owner"):
            pool = [
                n
                for n in pool
                if (owner == "DFINITY" and n["dc_id"] in dcs_owned_by_dfinity)
                or (owner == "others" and n["dc_id"] not in dcs_owned_by_dfinity)
            ]
        if status := selector.get("status"):
            pool = [n for n in pool if status == n["status"]]
        groups: dict[str | None, list[dre.RegistryNode]] = collections.defaultdict(list)
        if group_by := selector.get("group_by"):
            for node in pool:
                if group_by == "datacenter":
                    groups[node["dc_id"]].append(node)
                elif group_by == "subnet":
                    groups[node["subnet_id"]].append(node)
        else:
            groups[None] = pool
        if nodes_per_group := selector.get("nodes_per_group"):
            if isinstance(nodes_per_group, int):
                for k, v in groups.items():
                    groups[k] = v[:nodes_per_group]
            elif isinstance(nodes_per_group, float):
                for k, v in groups.items():
                    perc = round(len(v) * nodes_per_group)
                    groups[k] = v[:perc]
        pool = [n for m in groups.values() for n in m]
    node_ids = [n["node_id"] for n in pool]
    spent = set(node_ids)
    remaining_nodes = [n for n in remaining_nodes if n["node_id"] not in spent]

    return [node_info(n) for n in pool], remaining_nodes


def compute_provisional_node_batches(
    git_revision: rollout_types.GitCommit,
    stages: rollout_types.HostOSRolloutStages,
    registry: dre.RegistrySnapshot,
) -> ProvisionalHostOSBatches:
    """
    Computes a provisional plan for the rollout.

    The plan excludes all nodes that are already at the revision specified here.
    Each batch formulated by this generator subtracts the nodes it selected from
    the pool of nodes that will be considered by subsequent batches.  Iteratively,
    this achieves the goal of covering all nodes (based on the selectors supplied).

    It is expected that, due to node membership and health changes, the plan
    computed here will not remain valid for the duration of the HostOS rollout
    (up to a month).

    For computing the nodes that each batch must roll out to when its turn has
    come, we have the companion function below, `compute_this_batch_plan`, which
    expects the specific selectors for the single batch that will do the work,
    and does not iteratively reduce the number of nodes available.
    """
    # Exclude all upgraded nodes already.
    remaining_nodes = [
        n for n in registry["nodes"] if n["hostos_version_id"] != git_revision
    ]
    dcs_owned_by_dfinity = set(
        d["dc_id"]
        for d in registry["node_operators"]
        if d["node_provider_principal_id"] == DFINITY
    )
    p: ProvisionalHostOSBatches = {
        "canary": [],
        "main": [],
        "unassigned": [],
        "stragglers": None,
    }
    for canary_stage_selectors in stages.get("canary", []):
        node_ids, remaining_nodes = apply_selectors(
            remaining_nodes, canary_stage_selectors, dcs_owned_by_dfinity
        )
        p["canary"].append({"selectors": canary_stage_selectors, "nodes": node_ids})
    if "main" in stages:
        selectors = stages["main"]
        while True:
            node_ids, remaining_nodes = apply_selectors(
                remaining_nodes, selectors, dcs_owned_by_dfinity
            )
            if not node_ids:
                break
            p["main"].append({"selectors": selectors, "nodes": node_ids})
    if "unassigned" in stages:
        selectors = stages["unassigned"]
        while True:
            node_ids, remaining_nodes = apply_selectors(
                remaining_nodes, selectors, dcs_owned_by_dfinity
            )
            if not node_ids:
                break
            p["unassigned"].append({"selectors": selectors, "nodes": node_ids})
    for n in cast(
        list[Literal["stragglers"]],
        ["stragglers"],
    ):
        s_selectors = stages.get(n)
        if s_selectors is None:
            pass
        else:
            node_ids, remaining_nodes = apply_selectors(
                remaining_nodes, s_selectors, dcs_owned_by_dfinity
            )
            p[n] = {
                "selectors": s_selectors,
                "nodes": node_ids,
            }

    return p


def compute_provisional_plan(
    git_revision: rollout_types.GitCommit,
    max_canary_batches: int,
    max_main_batches: int,
    max_unassigned_batches: int,
    spec: rollout_types.HostOSRolloutPlanSpec,
    registry: dre.RegistrySnapshot,
    now: datetime.datetime | None = None,
) -> rollout_types.ProvisionalHostOSPlan:
    """
    Much like compute_provisional_node_batches, this function computes
    the batches, but this one returns a provisional timetable alongside
    the batches, and the selectors that were used to generate each batch.
    """
    # Compute a provisional set of batches of nodes.
    # The nodes here won't necessarily be what is rolled out during the rollout,
    # but it serves us to inform what is *likely* to be rolled out as well as
    # general proportion / shape of the rollout.

    # Compute a timetable to rely upon.
    # FIXME: the timetable has to have exclusion days, not just start day,
    # because the rollout cannot happen during the weekend.
    timetable: list[datetime.datetime] = api_boundary_node_batch_timetable(
        spec,
        batch_count=max_canary_batches
        + max_main_batches
        + max_unassigned_batches
        + 1,  # stragglers
        now=now,
    )

    batches = compute_provisional_node_batches(git_revision, spec["stages"], registry)

    assert len(batches["canary"]) <= max_canary_batches, (
        f"The number of canary batches requested {len(batches['canary'])}"
        f" is larger than the maximum canary batch count {max_canary_batches}"
    )

    assert len(batches["main"]) <= max_main_batches, (
        f"The number of main batches requested {len(batches['main'])}"
        f" is larger than the maximum main batch count {max_main_batches}"
    )

    assert len(batches["unassigned"]) <= max_unassigned_batches, (
        f"The number of unassigned batches requested {len(batches['main'])}"
        f" is larger than the maximum unassigned batch count {max_unassigned_batches}"
    )

    len_of_work = (
        len(batches["canary"]) + len(batches["main"]) + len(batches["unassigned"]) + 1
    )
    assert len_of_work <= len(timetable), (
        "The length of the timetable %s is shorter than the "
        "total length of all batches %s"
    ) % (len(timetable), len_of_work)

    plan: rollout_types.ProvisionalHostOSPlan = {
        "canary": [],
        "main": [],
        "unassigned": [],
        "stragglers": None,
        "resume_at": spec["resume_at"],
        "suspend_at": spec["suspend_at"],
        "minimum_minutes_per_batch": spec["minimum_minutes_per_batch"],
    }
    if "allowed_days" in spec:
        plan["allowed_days"] = spec["allowed_days"]

    for n in range(max_canary_batches):
        try:
            batch = batches.get("canary", [])[n]
        except IndexError:
            continue
        plan["canary"].append(
            {
                "start_at": timetable.pop(0),
                "nodes": batch["nodes"],
                "selectors": batch["selectors"],
            }
        )
    for n in range(max_main_batches):
        try:
            batch = batches.get("main", [])[n]
        except IndexError:
            continue
        plan["main"].append(
            {
                "start_at": timetable.pop(0),
                "nodes": batch["nodes"],
                "selectors": batch["selectors"],
            }
        )
    for n in range(max_unassigned_batches):
        try:
            batch = batches.get("unassigned", [])[n]
        except IndexError:
            continue
        plan["unassigned"].append(
            {
                "start_at": timetable.pop(0),
                "nodes": batch["nodes"],
                "selectors": batch["selectors"],
            }
        )
    for x in ["stragglers"]:
        x = typing.cast(typing.Literal["stragglers"], x)
        s_batch = batches[x]
        if s_batch:
            plan[x] = {
                "start_at": timetable.pop(0),
                "nodes": s_batch["nodes"],
                "selectors": s_batch["selectors"],
            }
    return plan


def compute_this_batch_plan(
    git_revision: str,
    selectors: rollout_types.NodeSelectors,
    registry: dre.RegistrySnapshot,
) -> rollout_types.NodeBatch:
    """
    Computes a formal plan for the rollout of a single batch.

    Because the nodes are limited to only those that don't already have the
    specified git revision, it is expected during production that the list of
    nodes to be considered by this batch will be equivalent to what the
    provisional plan computed ahead of time.  That is, equivalent minus the
    possibility that nodes may have changed assignment in the meantime, or
    may have changed health status.
    """
    # Exclude all upgraded nodes already.
    remaining_nodes = [
        n for n in registry["nodes"] if n["hostos_version_id"] != git_revision
    ]
    dcs_owned_by_dfinity = set(
        d["dc_id"]
        for d in registry["node_operators"]
        if d["node_provider_principal_id"] == DFINITY
    )
    node_ids, _ = apply_selectors(remaining_nodes, selectors, dcs_owned_by_dfinity)
    return node_ids


class DagParams(typing.TypedDict):
    git_revision: str
    plan: str
    simulate: bool


def stage_name(batch_name: HostOSStage, batch_index: int) -> str:
    if batch_name in ["canary", "main", "unassigned"]:
        return f"{batch_name}_{batch_index + 1}"
    return batch_name


def precedent_batches(batch_name: HostOSStage, batch_index: int) -> list[str]:
    if batch_name == "canary":
        return [stage_name(batch_name, n) for n in range(batch_index)]
    if batch_name == "main":
        return [stage_name("canary", n) for n in range(CANARY_BATCH_COUNT)] + [
            stage_name(batch_name, n) for n in range(batch_index)
        ]
    if batch_name == "unassigned":
        return (
            [stage_name("canary", n) for n in range(CANARY_BATCH_COUNT)]
            + [stage_name("main", n) for n in range(MAIN_BATCH_COUNT)]
            + [stage_name("unassigned", n) for n in range(batch_index)]
        )
    if batch_name == "stragglers":
        return (
            [stage_name("canary", n) for n in range(CANARY_BATCH_COUNT)]
            + [stage_name("main", n) for n in range(MAIN_BATCH_COUNT)]
            + [stage_name("unassigned", n) for n in range(UNASSIGNED_BATCH_COUNT)]
        )
    assert 0, "not possible: %r" % batch_name


def plan(batch_name: HostOSStage, batch_index: int, ti: TaskInstance) -> list[str]:
    schedule: ProvisionalHostOSPlan = ti.xcom_pull("schedule")
    batch: ProvisionalHostOSPlanBatch | None
    bn = stage_name(batch_name, batch_index)
    if batch_name == "canary":
        try:
            print(f"Attempting to retrieve the {batch_name} {batch_index + 1} batch ")
            batch = schedule[batch_name][batch_index]
        except IndexError:
            print("No prepared batch, skipping this batch")
            batch = None
    elif batch_name == "stragglers":
        print(f"Attempting to retrieve the schedule for the {batch_name} batch")
        batch = schedule[batch_name]
        if batch is None:
            print("No prepared batch, skipping this batch")
    elif batch_name == "main" or batch_name == "unassigned":
        try:
            print(f"Attempting to retrieve the {batch_name} {batch_index + 1} batch ")
            batch = schedule[batch_name][batch_index]
        except IndexError:
            print(
                "No prepared batch, attempting to retrieve"
                f" the very last {batch_name} batch"
            )
            try:
                batch = schedule[batch_name][-1]
            except IndexError:
                print(
                    f"No prepared batch for any {batch_name} batch, skipping this batch"
                )
                batch = None

    print(f"Original schedule:\n{pprint.pformat(batch)}")
    if batch:
        selectors = batch["selectors"]
        start_at = batch["start_at"]
        previous_tasks_that_had_nodes = [
            b
            for b in precedent_batches(batch_name, batch_index)
            if ti.xcom_pull(f"{b}.collect_nodes", key="nodes")
        ]
        if previous_tasks_that_had_nodes:
            pt = previous_tasks_that_had_nodes[-1]
            print(f"Investigating batch {pt} to see what its start time was")
            previous_task_started_at = ti.xcom_pull(f"{pt}.plan", key="start_at")
            if previous_task_started_at:
                print(
                    f"The latest batch to run started at {previous_task_started_at},"
                    " computing a more accurate start time"
                )
                new_start_at = api_boundary_node_batch_timetable(
                    schedule,
                    batch_count=2,
                    now=previous_task_started_at,
                )[1]
                print(f"Original start date: {start_at}")
                print(f"Updated start date: {new_start_at}")
                start_at = new_start_at
            else:
                print(
                    "The latest batch to run does not have a start date,"
                    f" using the start date of the original plan: {start_at}."
                )
        else:
            print(
                "No recorded start date for any previous batches,"
                f" using the start date of the original plan: {start_at}"
            )

        ti.xcom_push("selectors", selectors)
        ti.xcom_push("start_at", start_at)
        if selectors is not None:
            return [f"{bn}.wait_until_start_time"]

    return [f"{bn}.join"]


def schedule(network: ic_types.ICNetwork, params: DagParams) -> ProvisionalHostOSPlan:
    # Import the plan into data structure.
    spec = yaml_to_HostOSRolloutPlanSpec(params["plan"])

    # Fetch the list of nodes.
    runner = dre.DRE(network=network, subprocess_hook=SubprocessHook())
    registry = runner.get_registry()
    plan = compute_provisional_plan(
        params["git_revision"],
        CANARY_BATCH_COUNT,
        MAIN_BATCH_COUNT,
        UNASSIGNED_BATCH_COUNT,
        spec,
        registry,
    )

    print("Prospective timetable:\n%s" % pprint.pformat(plan))
    print("Summary of prospective timetable:")
    bad: str | None = None
    for batch_name in cast(
        list[HostOSStage],
        [
            "canary",
            "main",
            "unassigned",
            "stragglers",
        ],
    ):
        if batch_name == "stragglers":
            if plan["stragglers"] is not None:
                b = plan["stragglers"]
                sn = stage_name(batch_name, 1)
                print(f"* {sn}: {len(b['nodes'])} nodes at {b['start_at']}")
                if len(b["nodes"]) > MAX_NODES_PER_BATCH:
                    bad = sn
        else:
            for n, b in enumerate(plan[batch_name]):
                sn = stage_name(batch_name, n)
                print(f"* {sn}: {len(b['nodes'])} at {b['start_at']}")
                if len(b["nodes"]) > MAX_NODES_PER_BATCH:
                    bad = sn

    if bad:
        LOGGER.error(
            f"The list of nodes in batch {bad} is too long (> {MAX_NODES_PER_BATCH})."
            "  Failing preemptively to protect the IC."
        )
        assert 0

    return plan


def collect_nodes(
    batch_name: HostOSStage,
    batch_index: int,
    network: ic_types.ICNetwork,
    ti: TaskInstance,
    params: DagParams,
) -> list[str]:
    # Fetch the list of nodes.
    selectors = typing.cast(
        NodeSelectors,
        ti.xcom_pull(
            stage_name(batch_name, batch_index) + ".plan",
            key="selectors",
        ),
    )
    print("Selectors:\n%s" % pprint.pformat(selectors))
    runner = dre.DRE(network=network, subprocess_hook=SubprocessHook())
    registry = runner.get_registry()
    if params["simulate"]:
        already_simulated_node_id_batches: list[NodeBatch | None] = [
            ti.xcom_pull(f"{b}.collect_nodes", key="nodes")
            for b in precedent_batches(batch_name, batch_index)
        ]
        confirmed_batches = [
            s for s in already_simulated_node_id_batches if s is not None
        ]
        already_simulated_node_ids = set(
            [s["node_id"] for b in confirmed_batches for s in b]
        )
        print(f"Simulation -- ignoring {len(already_simulated_node_ids)} nodes")
        registry["nodes"] = [
            n
            for n in registry["nodes"]
            if n["node_id"] not in already_simulated_node_ids
        ]

    nodes = compute_this_batch_plan(params["git_revision"], selectors, registry)
    print("Nodes to roll out to:\n%s" % pprint.pformat(nodes))

    if len(nodes) > MAX_NODES_PER_BATCH:
        LOGGER.error(
            f"The list of nodes is too long (> {MAX_NODES_PER_BATCH})."
            "  Failing preemptively to protect the IC."
        )
        assert 0

    ti.xcom_push("nodes", nodes)
    if nodes:
        return [f"{stage_name(batch_name, batch_index)}.create_proposal_if_none_exists"]
    return [f"{stage_name(batch_name, batch_index)}.join"]


def create_proposal_if_none_exists(
    batch_name: HostOSStage,
    batch_index: int,
    network: ic_types.ICNetwork,
    ti: TaskInstance,
    params: DagParams,
) -> int:
    nodes = ti.xcom_pull(
        f"{stage_name(batch_name, batch_index)}.collect_nodes", key="nodes"
    )
    print("proposing for these nodes:", nodes)
    return -123456


def request_proposal_vote(
    batch_name: HostOSStage,
    batch_index: int,
    network: ic_types.ICNetwork,
    ti: TaskInstance,
    params: DagParams,
) -> None:
    print("FIXME")


wait_until_proposal_is_accepted = request_proposal_vote
wait_for_revision_adoption = request_proposal_vote
wait_until_nodes_healthy = request_proposal_vote

if __name__ == "__main__":
    network = ic_types.ICNetwork(
        "https://ic0.app/",
        "https://dashboard.internetcomputer.org/proposal",
        "https://dashboard.internetcomputer.org/release",
        ["https://victoria.mainnet.dfinity.network/select/0/prometheus/api/v1/query"],
        80,
        "dfinity.ic_admin.mainnet.proposer_key_file",
    )
    runner = dre.DRE(network=network, subprocess_hook=SubprocessHook())
    spec: rollout_types.HostOSRolloutStages = {
        "canary": [
            [
                {
                    "assignment": "unassigned",
                    "owner": "DFINITY",
                    "nodes_per_group": 1,
                    "status": "Healthy",
                }
            ],
            [
                {
                    "assignment": "unassigned",
                    "owner": "DFINITY",
                    "nodes_per_group": 5,
                    "status": "Healthy",
                }
            ],
            [
                {
                    "assignment": "assigned",
                    "owner": "DFINITY",
                    "nodes_per_group": 40.0,
                    "status": "Healthy",
                }
            ],
            [
                {
                    "assignment": "assigned",
                    "owner": "others",
                    "group_by": "subnet",
                    "nodes_per_group": 1,
                    "status": "Healthy",
                }
            ],
        ],
        "main": [
            {
                "assignment": "assigned",
                "group_by": "subnet",
                "nodes_per_group": 1,
                "status": "Healthy",
            }
        ],
        "unassigned": [
            {
                "assignment": "unassigned",
                "status": "Healthy",
            }
        ],
        "stragglers": [],
    }
    """
    import yaml

    x = compute_provisional_plan("012345", spec, runner)
    for key, batches in x.items():
        if key in "canary main":
            for n, batch in enumerate(batches):
                print(f"{key} {n}: {len(batch)} nodes")
                print(yaml.safe_dump(batch))
                print()
        else:
            print(f"{key}: {len(batches)} nodes")
            print(yaml.safe_dump(batches))
            print()
    """
