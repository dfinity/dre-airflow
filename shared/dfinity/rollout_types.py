import datetime
from typing import NotRequired, TypedDict, cast

import yaml

GitCommit = str
FeatureName = str
SubnetId = str


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


SubnetRolloutPlanSpec_doc: str = """\
Represents the rollout plan for subnet rollouts.

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

    nodes: list[str]
    start_day: NotRequired[str]
    resume_at: str
    suspend_at: str
    minimum_minutes_per_batch: int


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


class ProposalInfo(TypedDict):
    proposal_id: int
    proposal_url: str
    needs_vote: bool
