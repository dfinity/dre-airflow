import datetime
from typing import Any, Literal, NotRequired, TypedDict, cast

import yaml

type GitCommit = str
type FeatureName = str
type SubnetId = str
type NodeId = str
type NodeOperatorId = str
type NodeProviderId = str
type DCId = str
type NodeStatus = Literal["Healthy"] | Literal["Degraded"] | Literal["Down"]
type HostOsVersion = str

type DaysOfWeek = (
    Literal["Monday"]
    | Literal["Tuesday"]
    | Literal["Wednesday"]
    | Literal["Thursday"]
    | Literal["Friday"]
    | Literal["Saturday"]
    | Literal["Sunday"]
)

type DaysOfNextWeek = (
    Literal["Monday next week"]
    | Literal["Tuesday next week"]
    | Literal["Wednesday next week"]
    | Literal["Thursday next week"]
    | Literal["Friday next week"]
    | Literal["Saturday next week"]
    | Literal["Sunday next week"]
)


class ReleaseVersion(TypedDict):
    name: FeatureName
    version: GitCommit


class Release(TypedDict):
    rc_name: str
    rc_date: datetime.datetime
    versions: list[ReleaseVersion]


Releases = list[Release]


class RolloutFeaturesForDate(TypedDict):
    date: datetime.date
    # If a particular subnet ID does not have a feature assigned,
    # the subnet ID will not appear in this dictionary.  Caller
    # should assume base version in that case.
    subnet_id_feature_map: dict[SubnetId, FeatureName]


"""A dictionary of rollout features keyed by rollout start date."""
RolloutFeatures = list[RolloutFeaturesForDate]

SubnetNameOrNumber = int | str


class SubnetNameOrNumberWithRevision(TypedDict):
    subnet: SubnetNameOrNumber
    git_revision: str | None


class SubnetNumberWithRevision(TypedDict):
    subnet: int
    git_revision: str | None


class SubnetOrderSpec(TypedDict):
    subnets: list[SubnetNameOrNumber | SubnetNameOrNumberWithRevision]
    batch: int | None


"""
Represents the rollout plan for subnet rollouts.

See rollout_ic_os_to_subnets.py for help.
"""
SubnetRolloutPlanSpec = dict[
    str,
    dict[
        str | int,
        list[SubnetNameOrNumber | SubnetNameOrNumberWithRevision] | SubnetOrderSpec,
    ],
]


class ApiBoundaryNodeRolloutPlanSpec(TypedDict):
    """
    Represents the shape of the rollout plan for boundary nodes input into Airflow
    by the operator.

    See rollout_ic_os_to_api_boundary_nodes.py for help.
    """

    nodes: list[str]
    start_day: NotRequired[str]
    resume_at: str
    suspend_at: str
    minimum_minutes_per_batch: int
    allowed_days: NotRequired[list[DaysOfWeek] | list[DaysOfNextWeek]]


def yaml_to_ApiBoundaryNodeRolloutPlanSpec(s: str) -> ApiBoundaryNodeRolloutPlanSpec:
    try:
        d = yaml.safe_load(s)
    except Exception as e:
        raise ValueError(str(e)) from e
    try:
        assert isinstance(d, dict), "expected a dictionary of keys and values"
        assert "nodes" in d, "expected a nodes key"
        assert "resume_at" in d, "expected a resume_at key"
        assert "suspend_at" in d, "expected a suspend_at key"
        assert "minimum_minutes_per_batch" in d, (
            "expected a minimum_minutes_per_batch key"
        )
        assert d["nodes"], "nodes cannot be empty"
        for node in d["nodes"]:
            assert isinstance(node, str), "node principal %r is not a string" % node
        assert isinstance(d["resume_at"], str) or isinstance(d["resume_at"], int), (
            "resume_at must be an HH:MM string or a number of minutes"
        )
        assert isinstance(d["suspend_at"], str) or isinstance(d["suspend_at"], int), (
            "suspend_at must be an HH:MM string or a number of minutes"
        )
        assert (
            isinstance(d["minimum_minutes_per_batch"], int)
            and d["minimum_minutes_per_batch"] > 0
        ), "minimum_minutes_per_batch must be a positive integer"
    except AssertionError as e:
        raise ValueError(str(e)) from e
    if "start_day" in d:
        try:
            datetime.datetime.strptime(d["start_day"], "%A")
        except ValueError as e:
            try:
                datetime.datetime.strptime(d["start_day"], "%A next week")
            except ValueError:
                raise ValueError("invalid start_day value: %s" % d["start_day"]) from e
    return cast(ApiBoundaryNodeRolloutPlanSpec, d)


class NodeSelector(TypedDict, total=False):
    assignment: Literal["unassigned"] | Literal["assigned"]
    owner: Literal["DFINITY"] | Literal["others"]
    group_by: Literal["datacenter"] | Literal["subnet"]
    status: NodeStatus
    # Either a literal number of nodes or a float 0-1 for a percentage of nodes.
    nodes_per_group: int | float


type NodeSelectors = list[NodeSelector]


def to_selector(selector: dict[str, Any]) -> NodeSelector:
    assert selector.get("assignment") in ["unassigned", "assigned", None], (
        "the assignment is not either one of unassigned or assigned"
    )
    assert selector.get("owner") in ["DFINITY", "others", None], (
        "the owner is not either one of DFINITY or others"
    )
    assert selector.get("group_by") in ["subnet", "datacenter", None], (
        "the group_by key is not either one of subnet or datacenter"
    )
    assert selector.get("status") in ["Healthy", "Degraded", "Down", None], (
        "the status key is not either one of Healthy, Degraded or Down"
    )
    if "nodes_per_group" in selector:
        nodes_per_group = selector["nodes_per_group"]
        if isinstance(nodes_per_group, str):
            if nodes_per_group.endswith("%"):
                nodes_per_group = float(nodes_per_group[:-1]) / 100
            else:
                nodes_per_group = int(nodes_per_group)
        elif isinstance(nodes_per_group, int) or isinstance(nodes_per_group, float):
            pass
        else:
            assert 0, "nodes_per_group is not float, integer or percentage"
        if isinstance(nodes_per_group, float) and (
            nodes_per_group < 0 or nodes_per_group > 1
        ):
            assert 0, "nodes_per_group cannot be lower than 0% or greater than 100%"
        if isinstance(nodes_per_group, int) and (nodes_per_group < 0):
            assert 0, "nodes_per_group cannot be lower than 0 nodes"
        selector["nodes_per_group"] = nodes_per_group
    assert selector.get("status") in ["Healthy", "Degraded", "Down", None], (
        "the status key is not either one of Healthy, Degraded or Down"
    )
    remaining_keys = set(selector.keys()) - set(
        ["assignment", "owner", "group_by", "status", "nodes_per_group"]
    )
    assert not remaining_keys, f"extraneous keys found in selector: {remaining_keys}"
    return cast(NodeSelector, selector)


def to_selectors(selectors: list[dict[str, Any]]) -> NodeSelectors:
    news: NodeSelectors = []
    for selnum, s in enumerate(selectors):
        try:
            news.append(to_selector(s))
        except Exception as e:
            assert 0, f"selector {selnum + 1} has problems: {e}"
    return news


class HostOSRolloutBatchSpec(TypedDict):
    selectors: NodeSelectors


class HostOSRolloutStages(TypedDict, total=False):
    canary: list[HostOSRolloutBatchSpec]
    main: HostOSRolloutBatchSpec
    unassigned: HostOSRolloutBatchSpec
    stragglers: HostOSRolloutBatchSpec


class HostOSRolloutPlanSpec(TypedDict):
    """
    Represents the plan that the HostOS rollout will follow.

    See rollout_ic_os_to_nodes.py for help on the spec.
    """

    stages: HostOSRolloutStages
    allowed_days: NotRequired[list[DaysOfWeek]]
    start_day: NotRequired[DaysOfWeek]
    resume_at: str
    suspend_at: str
    minimum_minutes_per_batch: int


def yaml_to_HostOSRolloutPlanSpec(s: str) -> HostOSRolloutPlanSpec:
    try:
        d = yaml.safe_load(s)
    except Exception as e:
        raise ValueError(str(e)) from e
    try:
        assert isinstance(d, dict), "expected a dictionary of keys and values"
        assert "stages" in d, "expected a stages key"
        assert "resume_at" in d, "expected a resume_at key"
        assert "suspend_at" in d, "expected a suspend_at key"
        assert "minimum_minutes_per_batch" in d, (
            "expected a minimum_minutes_per_batch key"
        )
        assert d["stages"], "stages cannot be empty"
        assert isinstance(d["stages"], dict), "stages are not a dictionary"
        if canary := d["stages"].get("canary"):
            assert isinstance(canary, list), "the canary stages are not a list"
            for cn, c in enumerate(canary):
                try:
                    canary[cn] = {"selectors": to_selectors(c["selectors"])}
                except Exception as e:
                    assert 0, f"while evaluating canary stage {cn + 1}: {e}"
        for stage_name in ["main", "unassigned", "stragglers"]:
            if stage_name in d:
                if stage := d["stages"].get(stage_name):
                    assert isinstance(canary, list), (
                        f"the {stage_name} stage is not a list"
                    )
                    try:
                        stage = to_selectors(stage["selectors"])
                        d["stages"][stage_name] = {"selectors": stage}
                    except Exception as e:
                        f"while evaluating {stage_name} stage: {e}"
        remaining_keys = set(d["stages"].keys()) - set(
            ["canary", "main", "unassigned", "stragglers"]
        )
        assert not remaining_keys, f"extraneous keys found in stages: {remaining_keys}"

        assert isinstance(d["resume_at"], str) or isinstance(d["resume_at"], int), (
            "resume_at must be an HH:MM string or a number of minutes"
        )
        assert isinstance(d["suspend_at"], str) or isinstance(d["suspend_at"], int), (
            "suspend_at must be an HH:MM string or a number of minutes"
        )
        assert (
            isinstance(d["minimum_minutes_per_batch"], int)
            and d["minimum_minutes_per_batch"] > 0
        ), "minimum_minutes_per_batch must be a positive integer"
    except AssertionError as e:
        raise ValueError(str(e)) from e
    if "allowed_days" in d:
        for day in d["allowed_days"]:
            try:
                datetime.datetime.strptime(day, "%A")
            except ValueError as e:
                raise ValueError(
                    "one of the days in allowed_days is not valid: %s" % day
                ) from e
    if "start_day" in d:
        try:
            datetime.datetime.strptime(d["start_day"], "%A")
        except ValueError as e:
            try:
                datetime.datetime.strptime(d["start_day"], "%A next week")
            except ValueError:
                raise ValueError("invalid start_day value: %s" % d["start_day"]) from e
    for x in ["resume_at", "suspend_at"]:
        if isinstance(d[x], int):
            pass
        elif isinstance(d[x], str):
            try:
                datetime.datetime.strptime(d[x], "%H:%M")
            except Exception:
                assert 0, f"{x} must be an HH:MM string"
        else:
            assert 0, f"{x} must be an HH:MM string or a number of minutes"
    assert (
        isinstance(d["minimum_minutes_per_batch"], int)
        and d["minimum_minutes_per_batch"] > 0
    ), "minimum_minutes_per_batch must be a positive integer"

    return cast(HostOSRolloutPlanSpec, d)


class NodeInfo(TypedDict):
    node_id: NodeId
    node_provider_id: NodeOperatorId
    dc_id: DCId
    subnet_id: SubnetId | None
    status: NodeStatus


type NodeBatch = list[NodeInfo]


class ComputedBatch(TypedDict):
    nodes: NodeBatch
    selectors: NodeSelectors


type HostOSStage = (
    Literal["canary"] | Literal["main"] | Literal["unassigned"] | Literal["stragglers"]
)


class ProvisionalHostOSBatches(TypedDict):
    canary: list[ComputedBatch]
    main: list[ComputedBatch]
    unassigned: list[ComputedBatch]
    stragglers: list[ComputedBatch]


class ProvisionalHostOSPlanBatch(TypedDict):
    nodes: NodeBatch
    selectors: NodeSelectors
    start_at: datetime.datetime


class ProvisionalHostOSPlan(TypedDict):
    canary: list[ProvisionalHostOSPlanBatch]
    main: list[ProvisionalHostOSPlanBatch]
    unassigned: list[ProvisionalHostOSPlanBatch]
    stragglers: list[ProvisionalHostOSPlanBatch]
    minimum_minutes_per_batch: int
    allowed_days: NotRequired[list[DaysOfWeek]]
    resume_at: str
    suspend_at: str
    start_day: NotRequired[DaysOfWeek]


class ProposalInfo(TypedDict):
    proposal_id: int
    proposal_url: str
    needs_vote: bool
