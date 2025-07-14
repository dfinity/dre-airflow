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

# Decoded to type SubnetRolloutPlanSpec.
DEFAULT_GUESTOS_ROLLOUT_PLANS: dict[str, str] = {
    "mainnet": """
Monday:
  9:00:      [io67a, xok3w]
  11:00:     [shefu, fuqsr, 4utr6]
Tuesday:
  7:00:      [pjljw, qdvhd, 2fq7c]
  9:00:      [snjp4, w4asl, qxesv]
  11:00:     [4zbus, ejbmu, uzr34]
  13:00:     [c4isl, mkbc3, vcpt7]
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
""".strip()
}
GUESTOS_ROLLOUT_PLAN_HELP = """\
A specification of what subnets to rollout, when, and with which versions.

Remarks:
* All times are expressed in the UTC time zone.
* Days refer to dates relative to your current work week
  if starting a rollout during a workday, or next week if
  the rollout is started during a weekend.
* A day name with " next week" added at the end means
  "add one week to this day".
* Each date/time can specify a simple list of subnets,
  or can specify a dict with two keys:
  * batch: an optional integer 1-30 with the batch number
           you want to assign to this batch.
  * subnets: a list of subnets.
* A subnet may be specified:
  * as an integer number from 0 to the maximum subnet number,
  * as a full or abbreviated subnet principal ID,
  * as a dictionary of {
       subnet: ID or principal
       git_revision: revision to deploy to this subnet
    }
    with this form being able to override the Git revision
    that will be targeted to that specific subnet.
    Example of a batch specified this way:
      Monday next week:
        7:00:
          batch: 30
          subnets:
          - subnet: tdb26
            git_revision: 0123456789012345678901234567890123456789
"""


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


# Decoded to type ApiBoundaryNodeRolloutPlanSpec.
DEFAULT_API_BOUNDARY_NODES_ROLLOUT_PLANS: dict[str, str] = {
    "mainnet": """
nodes:
  - 4fssn-4vi43-2qufr-hlrfz-hfohd-jgrwc-7l7ok-uatwb-ukau7-lwmoz-tae
  - 4p3lu-7sy3a-ph47t-wlvb6-naehi-oki2f-pkokv-hovam-s5iul-sxvwk-3ae
  - 5dpkp-lfhr2-j7mfz-gavpn-puej5-wdfzg-fw42o-zupnu-izvk3-ubzzi-6ae
  - atrq6-prjsa-w2b32-gyukj-ivjd6-kc4gq-ufi6v-yier6-puxtx-n2ipp-fqe
  - bcbz4-w2ogq-jt7xk-xd7b2-ylhei-ygp3n-pjpdy-253tu-fpn3s-f5asy-fqe
  - bfj6y-cmhcf-6fxs7-ku2u4-tucww-b2eej-2dmap-snurk-3yaks-ss7xe-rae
  - coqzx-lgyi2-hizw4-nax6t-mshs6-y4ml6-xon24-ioon4-rer4r-qtuan-cae
  - dl74z-6vpps-k6bpu-5hjsj-fb6lb-34tsv-ue5gt-bdkjn-35pt5-fdu2a-rae
  - ec62l-q44va-5lyw2-gbl4w-xcx55-c3qv4-q67vc-fu6s7-xpm2r-7tjrw-tae
  - ek6px-kxr47-noli7-nyjql-au5uc-tmj3k-777mf-lfla5-k4xx4-msu3j-dae
  - gvqb5-b2623-3twpd-mruy4-3grts-agp2t-wgmt4-hspvj-5oyl6-kje32-aqe
  - jlifv-mddo3-zfwdc-x2wmu-xklbj-dai3i-3pvjy-j4hqm-sarqr-fhkq6-zae
  - lwxeh-xayoz-wu5eb-hwraj-a3pew-yeioj-cnwat-5uow4-ugxkc-kx44o-4ae
  - mbsyf-dtlsd-ccjq4-63rmh-u2lv7-dwxuq-ym6bk-63i2l-5uyki-zccek-mqe
  - mj4qw-5oxqx-5jgpn-66re3-gzgjb-rphz5-hmwhc-ahaa5-vespc-s4maj-vqe
  - pxxcj-h2sa5-q2yql-4j7od-jwtvq-uisnw-w2ox6-xtfqi-noxva-nlgxx-wqe
  - q6hk7-hplmv-xsedz-5bv7n-vfkws-rjyel-ijhge-hx2zg-nk7wb-tzisp-xqe
  - ubipk-gibrt-gr23n-u4mrg-iwgaa-4jz42-y6gt6-3htxw-3mq2m-dwhv2-pqe
  - upg5h-ggk5u-6qxp7-ksz3r-osynn-z2wou-65klx-cuala-sd6y3-3lorr-dae
  - yqbqe-whgvn-teyic-zvtln-rcolf-yztin-ecal6-smlwy-6imph-6isdn-qqe
start_day: Wednesday
resume_at: 9:00
suspend_at: 17:00
minimum_minutes_per_batch: 15
""".strip()
}
API_BOUNDARY_NODES_ROLLOUT_PLAN_HELP = """\
Represents the shape of the rollout plan for boundary nodes input into Airflow
by the operator.

All keys are required except for start_day.

Remarks:

* The nodes key contains a list of all boundary nodes to be
  rolled out to.  These will be batched (in the given order) into
  batches of one or more, to fit a total maximum of 20 batches.
  The largest batches will occur at the end.
* The minimum_minutes_per_batch key indicates how fast we can go.
  The default 60 minutes ensures batches are spaced a minimum of
  60 minutes apart.
* The start_day key indicates the weekday (in English) when the
  first batch of the rollout should start being rolled out.
  If left unspecified, it corresponds to today.
* Batches are rolled out between the times specified in the
  resume_at and the suspend_at keys (in HH:MM format).  The time
  window between resume_at and suspend_at must be large enough
  to fit the minimum_minutes_per_batch value.
* All times are UTC.
"""


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

    See below for help on the spec.
    """

    stages: HostOSRolloutStages
    allowed_days: NotRequired[list[DaysOfWeek]]
    start_day: NotRequired[DaysOfWeek]
    resume_at: str
    suspend_at: str
    minimum_minutes_per_batch: int


# Decoded to type HostOSRolloutPlanSpec.
DEFAULT_HOSTOS_ROLLOUT_PLANS: dict[str, str] = {
    "mainnet": """
stages:
  canary:
  - selectors:
    - assignment: unassigned
      owner: DFINITY
      status: Healthy
      nodes_per_group: 1
  - selectors:
    - assignment: unassigned
      owner: DFINITY
      status: Healthy
      nodes_per_group: 5
  - selectors:
    - assignment: assigned
      owner: DFINITY
      status: Healthy
      nodes_per_group: 10%
  - selectors:
    - assignment: assigned
      owner: others
      group_by: subnet
      status: Healthy
      nodes_per_group: 1
  main:
    selectors:
    - assignment: assigned
      group_by: subnet
      status: Healthy
      nodes_per_group: 1
  unassigned:
    selectors:
    - assignment: unassigned
      status: Healthy
      nodes_per_group: 100
  stragglers:
    selectors: []
allowed_days:
- Monday
- Tuesday
- Wednesday
- Thursday
resume_at: 7:00
suspend_at: 15:00
minimum_minutes_per_batch: 120
""".strip()
}
HOSTOS_ROLLOUT_PLAN_HELP = """\
Represents the plan that the HostOS rollout will follow.

A HostOS rollout proceeds in stages (none mandatory) that each proceed
batch by batch:

1. `canary` stages (up to `CANARY_BATCH_COUNT` batches which is 5)
2. `main` stages (up to `MAIN_BATCH_COUNT` batches which is 50)
3. `unassigned` stages (up to `UNASSIGNED_BATCH_COUNT` batches which is 15)
4. `stragglers` stage (only one batch)

Which nodes to roll out to is decided by a list of selectors that select
which nodes are to be rolled out in each batch:

* one list of selectors per `canary` batch,
* a single list of selectors common to all `main` batches,
* a single list of selectors common to all `unassigned` batches,
* a single list of selectors for the single `stragglers` batch

A selector is a dictionary that specifies `assignment` (unassigned or
assigned), `owner` (DFINITY or others), status (Degraded, Healthy or
Down).  From those selection keys, a list of nodes is created by the batch,
and then optionally grouped by a property (either datacenter or subnet,
specified in key `group_by`).  After the (optional) grouping, a number of
nodes from each group is selected (up to an absolute number if the key
`nodes_per_group` is an integer, or a 0-100 percentage if the key is a
number postfixed by %).  Then the groups (or single group if no `group_by`
was specified) are collated into a single list, and those are the nodes that
the batch will target.  Here is an example of a selector that would select
1 unassigned node per datacenter that is owned by DFINITY:

```
- assignment: unassigned
    owner: DFINITY
    group_by: datacenter
    nodes_per_group: 1
```

The application of selectors for each batch happens as follows:

* The batch starts with all nodes not yet rolled out to as candidates.
* Each selector in the batch's list of selectors is applied iteratively,
    reducing the list of nodes that the batch will roll out to only the nodes
    matching the selector as well as all prior selectors.

A list is used rather than a single selector, because this allows for combining
multiple selectors to achieve a selection of nodes that would otherwise be
impossible to obtain with a single selector.  Note that an empty list of
selectors is equivalent to "all nodes".  The rollout has a fuse built-in that
prevents rolling out to more than 150 nodes at once, so if this error takes
place, the rollout will abort.  If an empty list of nodes is the result of
all selectors applied, the batch is simply skipped and the rollout moves to
the next batch (or the first batch of the next stage, if need be).

If a stage key is not specified, the stage is skipped altogether.

Putting it all together, here is an abridged example of a rollout that would
only roll out three `canary` batches (to a single node each), and a series
of unassigned batches, with no `main` or `stragglers` stage:

```
stages:
  canary:
    - selectors: # for batch 1
      - assignment: unassigned
        nodes_per_group: 1
    - selectors: # for batch 2
      - assignment: unassigned
        nodes_per_group: 1
    - selectors: # for batch 3
      - assignment: unassigned
        nodes_per_group: 1
  unassigned:
    selectors: # for all batches up to the 15th
    - assignment: unassigned
      nodes_per_group: 100
...
```

There are a few other configuration options -- non-stage configuration keys are
required except for `start_day` and `allowed_days`.  Some remarks:

* The `minimum_minutes_per_batch` key indicates how fast we can go per batch.
* The `start_day` key indicates the weekday (in English) when the
  first batch of the rollout should start being rolled out.
  If left unspecified, it corresponds to today.
* Batches are rolled out between the times specified in the
  `resume_at` and the `suspend_at` keys (in HH:MM format).  The time
  window between `resume_at` and `suspend_at` must be large enough
  to fit the `minimum_minutes_per_batch` value.
* All times are UTC.
* If not specified, `allowed_days` will default to weekdays (including Friday!).
"""


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
    stragglers: ComputedBatch | None


class ProvisionalHostOSPlanBatch(TypedDict):
    nodes: NodeBatch
    selectors: NodeSelectors
    start_at: datetime.datetime


class ProvisionalHostOSPlan(TypedDict):
    canary: list[ProvisionalHostOSPlanBatch]
    main: list[ProvisionalHostOSPlanBatch]
    unassigned: list[ProvisionalHostOSPlanBatch]
    stragglers: ProvisionalHostOSPlanBatch | None
    minimum_minutes_per_batch: int
    allowed_days: NotRequired[list[DaysOfWeek]]
    resume_at: str
    suspend_at: str
    start_day: NotRequired[DaysOfWeek]


class ProposalInfo(TypedDict):
    proposal_id: int
    proposal_url: str
    needs_vote: bool
