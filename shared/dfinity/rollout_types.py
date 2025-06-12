import datetime
from typing import TypedDict

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
