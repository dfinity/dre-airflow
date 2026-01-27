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


def intorfloatbounded(n: Any) -> int | float:
    """
    Convert a value from a percentage 0%-100% to a float, or an int to itself
    so long as the int is greater than zero.

    Raises ValueError if the value cannot be used.
    """
    nodes_per_group: int | float
    if isinstance(n, str):
        if n.endswith("%"):
            nodes_per_group = float(n[:-1]) / 100
        else:
            nodes_per_group = int(n)
    elif isinstance(n, int):
        nodes_per_group = n
    elif isinstance(n, float):
        nodes_per_group = n
    else:
        raise ValueError(f"not a string, float or int: {n}")
    if isinstance(nodes_per_group, float) and (
        nodes_per_group < 0 or nodes_per_group > 1
    ):
        raise ValueError(f"value {n} cannot be lower than 0% or greater than 100%")
    if isinstance(nodes_per_group, int) and (nodes_per_group < 0):
        raise ValueError(f"value {n} cannot be lower than 0")
    return nodes_per_group


# Either a literal number of nodes allowed to not succeed,
# or a float 0-1 for a percentage of nodes.
type NodeFailureTolerance = int | float


def to_NodeFailureTolerance(n: Any) -> int | float:
    return intorfloatbounded(n)


NodeSelector = TypedDict(
    "NodeSelector",
    {
        "assignment": Literal["unassigned"]
        | Literal["assigned"]
        | Literal["API boundary"],
        "owner": Literal["DFINITY"] | Literal["others"],
        "group_by": Literal["datacenter"] | Literal["subnet"],
        "datacenter": DCId,
        "status": NodeStatus,
        # Either a literal number of nodes or a float 0-1 for a percentage of nodes.
        "nodes_per_group": int | float,
        # Filter nodes to only those in subnets with healthy node count exceeding
        "subnet_healthy_threshold": int | float,
        "join": list["NodeSelector"],
        "intersect": list["NodeSelector"],
        "not": "NodeSelector",
    },
    total=False,
)


def to_specifier(specifier: dict[str, Any] | NodeSelector) -> NodeSelector:
    assert specifier.get("assignment") in [
        "unassigned",
        "assigned",
        "API boundary",
        None,
    ], "the assignment is not either one of unassigned or assigned or API boundary"
    assert specifier.get("owner") in ["DFINITY", "others", None], (
        "the owner is not either one of DFINITY or others"
    )
    assert specifier.get("group_by") in ["subnet", "datacenter", None], (
        "the group_by key is not either one of subnet or datacenter"
    )
    if specifier.get("group_by") == "subnet":
        assert specifier.get("assignment") != "API boundary", (
            "grouping by subnet is not supported for API boundary assignments"
        )
    assert specifier.get("status") in ["Healthy", "Degraded", "Down", None], (
        "the status key is not either one of Healthy, Degraded or Down"
    )
    if "nodes_per_group" in specifier:
        nodes_per_group = specifier["nodes_per_group"]
        try:
            nodes_per_group = intorfloatbounded(nodes_per_group)
        except ValueError as e:
            assert 0, f"nodes_per_group is not float, integer or percentage: {e}"
        specifier["nodes_per_group"] = nodes_per_group
    if "subnet_healthy_threshold" in specifier:
        subnet_healthy_threshold = specifier["subnet_healthy_threshold"]
        try:
            subnet_healthy_threshold = intorfloatbounded(subnet_healthy_threshold)
        except ValueError as e:
            assert 0, (
                f"subnet_healthy_threshold is not float, integer or percentage: {e}"
            )
        specifier["subnet_healthy_threshold"] = subnet_healthy_threshold
        assert specifier.get("assignment") == "assigned", (
            "subnet_healthy_threshold requires assignment: assigned"
        )
    assert specifier.get("status") in ["Healthy", "Degraded", "Down", None], (
        "the status key is not either one of Healthy, Degraded or Down"
    )
    remaining_keys = set(specifier.keys()) - set(
        [
            "assignment",
            "owner",
            "group_by",
            "status",
            "nodes_per_group",
            "datacenter",
            "subnet_healthy_threshold",
        ]
    )
    assert not remaining_keys, f"extraneous keys found in selector: {remaining_keys}"
    assert (
        "assignment" in specifier
        or "owner" in specifier
        or "status" in specifier
        or "datacenter" in specifier
    ), (
        "illegal selector -- has no owner, assignment, datacenter or status selection"
        " criteria"
    )

    return cast(NodeSelector, specifier)


def to_selectors(
    selectors: dict[str, Any] | list[dict[str, Any]] | NodeSelector,
) -> NodeSelector:
    # Compatibility with prior form of specification of the selectors.
    if isinstance(selectors, list):
        selectors = {"intersect": [to_selectors(s) for s in selectors]}

    try:
        if "join" in selectors:
            remaining_keys = set(selectors.keys()) - set(["join"])
            assert not remaining_keys, (
                f"extraneous keys found in `join` selector: {remaining_keys}"
            )
            return {"join": [to_selectors(s) for s in selectors["join"]]}

        if "intersect" in selectors:
            remaining_keys = set(selectors.keys()) - set(["intersect"])
            assert not remaining_keys, (
                f"extraneous keys found in `intersect` selector: {remaining_keys}"
            )
            return {"intersect": [to_selectors(s) for s in selectors["intersect"]]}
        if "not" in selectors:
            remaining_keys = set(selectors.keys()) - set(["not"])
            assert not remaining_keys, (
                f"extraneous keys found in `not` selector: {remaining_keys}"
            )
            return {
                "not": to_selectors(selectors["not"]),
            }

        return to_specifier(selectors)

    except Exception as e:
        assert 0, f"selector {selectors} has problems: {e}"


class HostOSRolloutBatchSpec(TypedDict):
    selectors: NodeSelector
    tolerance: NotRequired[NodeFailureTolerance]


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
                    assert 0, f"while evaluating canary stage {cn + 1} selectors: {e}"
                if "tolerance" in c:
                    try:
                        canary[cn]["tolerance"] = to_NodeFailureTolerance(
                            c["tolerance"]
                        )
                    except Exception as e:
                        assert 0, (
                            f"while evaluating canary stage {cn + 1} tolerance: {e}"
                        )
        for stage_name in ["main", "unassigned", "stragglers"]:
            if stage_name in d["stages"]:
                if stage := d["stages"].get(stage_name):
                    try:
                        selectors = to_selectors(stage["selectors"])
                        d["stages"][stage_name] = {"selectors": selectors}
                    except Exception as e:
                        assert 0, f"while evaluating {stage_name} stage: {e}"
                    if "tolerance" in stage:
                        try:
                            d["stages"][stage_name]["tolerance"] = (
                                to_NodeFailureTolerance(stage["tolerance"])
                            )
                        except Exception as e:
                            assert 0, (
                                f"while evaluating {stage_name} stage tolerance: {e}"
                            )
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
    assignment: SubnetId | Literal["API boundary"] | None
    status: NodeStatus


type NodeBatch = list[NodeInfo]

type NodeUpgradeStatus = Literal["upgraded"] | Literal["pending"] | Literal["AWOL"]

type NodeUpgradeStatuses = dict[NodeId, NodeUpgradeStatus]

type NodeAlertStatus = Literal["alerting"] | Literal["OK"] | Literal["unknown"]

type NodeAlertStatuses = dict[NodeId, NodeAlertStatus]

type HostOSStage = (
    Literal["canary"] | Literal["main"] | Literal["unassigned"] | Literal["stragglers"]
)


class ProvisionalHostOSPlanBatch(TypedDict):
    nodes: NodeBatch
    selectors: NodeSelector
    tolerance: NotRequired[NodeFailureTolerance]
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
