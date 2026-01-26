import datetime
import json
import os
import sys
import textwrap
from typing import Any, cast

import pytest
from dfinity import dre
from dfinity.ic_types import IC_NETWORKS
from dfinity.rollout_types import yaml_to_HostOSRolloutPlanSpec
from operators.hostos_rollout import (
    DagParams,
    compute_provisional_plan,
    is_apibn,
    schedule,
)


@pytest.fixture
def registry() -> dre.RegistrySnapshot:
    with open(os.path.join(os.path.dirname(__file__), "registry.json"), "r") as f:
        return cast(dre.RegistrySnapshot, json.load(f))


def test_simple_plan(registry: dre.RegistrySnapshot) -> None:
    """Tests that a valid rollout plan spec works."""
    spec = textwrap.dedent("""\
        stages:
          canary:
          - selectors:
            - assignment: unassigned
              owner: DFINITY
              nodes_per_group: 1
              status: Healthy
        resume_at: 7:00
        suspend_at: 15:00
        minimum_minutes_per_batch: 30
        """)
    now = datetime.datetime(2025, 7, 11, 11, 30)

    res = compute_provisional_plan(
        "000000000",
        5,
        1,
        15,
        1,
        yaml_to_HostOSRolloutPlanSpec(spec),
        registry,
        now,
    )
    assert not res["main"]
    assert not res["stragglers"]
    assert not res["unassigned"]
    canary = res["canary"]
    assert canary[0]["nodes"][0]["node_provider_id"].startswith("bvc")
    assert canary[0]["start_at"] == now


def test_default_plan(registry: dre.RegistrySnapshot) -> None:
    """Tests that the default rollout plan spec produces expected results."""
    now = datetime.datetime(2025, 7, 6, 11, 30)

    sys.path.append(os.path.dirname(os.path.dirname("{__file__}")))
    try:
        from dags.defaults import DEFAULT_HOSTOS_ROLLOUT_PLANS
    finally:
        sys.path.pop()

    spec = yaml_to_HostOSRolloutPlanSpec(DEFAULT_HOSTOS_ROLLOUT_PLANS["mainnet"])
    res = compute_provisional_plan(
        "000000000",
        5,
        40,
        15,
        1,
        spec,
        registry,
        now,
    )
    # for b in ["canary", "main", "unassigned", "stragglers"]:
    #    if b == "stragglers":
    #        print(b, "has", len(res[b]["nodes"]), "nodes")
    #    else:
    #        for x in res[b]:
    #            print(b, "has", len(x["nodes"]), "nodes")
    assert res["main"]
    assert res["stragglers"]
    assert res["unassigned"]
    assert res["canary"]
    canary = res["canary"]
    assert canary[0]["start_at"] == datetime.datetime(2025, 7, 7, 7, 0)
    assert len(canary) == 4
    assert len(res["main"]) == 39
    assert any(
        is_apibn(n, set(s["principal"] for s in registry["api_bns"]))
        for n in res["main"][3]["nodes"]
    )
    assert len(res["main"][-1]["nodes"]) == 1
    assert len(res["unassigned"]) == 7
    assert len(res["unassigned"][-1]["nodes"]) == 96
    assert len(res["stragglers"][0]["nodes"]) == 14
    batches = (
        [r for r in res["canary"]]
        + [r for r in res["main"]]
        + [r for r in res["unassigned"]]
        + [r for r in res["stragglers"]]
    )
    assert batches[-1]["start_at"] == datetime.datetime(2025, 7, 16, 9, 0)


def test_schedule_bombs_with_too_many_nodes(
    mocker: Any, registry: dre.RegistrySnapshot
) -> None:
    "Schedule should bomb if one stage tries to upgrade too many nodes."
    """Tests that the default rollout plan spec works."""
    spec = textwrap.dedent("""\
        stages:
          canary:
          - selectors: [] # should cause ALL nodes to be upgraded
                          # in this stage
        resume_at: 7:00
        suspend_at: 15:00
        minimum_minutes_per_batch: 30
        """)
    params: DagParams = {"simulate": True, "plan": spec, "git_revision": "0"}
    mocker.patch("dfinity.dre.DRE.get_registry", return_value=registry)
    with pytest.raises(AssertionError):
        schedule(IC_NETWORKS["mainnet"], params)


def test_only_apibns(mocker: Any, registry: dre.RegistrySnapshot) -> None:
    "Schedule should bomb if one stage tries to upgrade too many nodes."
    """Tests that the default rollout plan spec works."""
    spec = textwrap.dedent("""\
        stages:
          main:
            selectors:
              assignment: API boundary
        resume_at: 7:00
        suspend_at: 15:00
        minimum_minutes_per_batch: 30
        """)
    params: DagParams = {"simulate": True, "plan": spec, "git_revision": "0"}
    mocker.patch("dfinity.dre.DRE.get_registry", return_value=registry)
    sched = schedule(IC_NETWORKS["mainnet"], params)
    assert len(sched["main"][0]["nodes"]) == 20
    assert all(
        is_apibn(n, set(s["principal"] for s in registry["api_bns"]))
        for n in sched["main"][0]["nodes"]
    )


def test_join_apibn_and_regular_assigned(
    mocker: Any, registry: dre.RegistrySnapshot
) -> None:
    "Schedule should bomb if one stage tries to upgrade too many nodes."
    """Tests that the default rollout plan spec works."""
    spec = textwrap.dedent("""\
        stages:
          canary:
          - selectors:
              join:
              - assignment: API boundary
                nodes_per_group: 1
              - assignment: unassigned
                nodes_per_group: 1
        resume_at: 7:00
        suspend_at: 15:00
        minimum_minutes_per_batch: 30
        """)
    params: DagParams = {"simulate": True, "plan": spec, "git_revision": "0"}
    mocker.patch("dfinity.dre.DRE.get_registry", return_value=registry)
    sched = schedule(IC_NETWORKS["mainnet"], params)
    assert len(sched["canary"][0]["nodes"]) == 2
    assert sched["canary"][0]["nodes"][0]["assignment"] == "API boundary"
    assert sched["canary"][0]["nodes"][1]["assignment"] != "API boundary"


def test_nodes_dont_repeat_themselves(
    mocker: Any, registry: dre.RegistrySnapshot
) -> None:
    "Schedule should bomb if one stage tries to upgrade too many nodes."
    """Tests that the default rollout plan spec works."""
    spec = textwrap.dedent("""\
        stages:
          canary:
          - selectors:
              join:
              - assignment: API boundary
                nodes_per_group: 1
              - assignment: API boundary
                nodes_per_group: 1
        resume_at: 7:00
        suspend_at: 15:00
        minimum_minutes_per_batch: 30
        """)
    params: DagParams = {"simulate": True, "plan": spec, "git_revision": "0"}
    mocker.patch("dfinity.dre.DRE.get_registry", return_value=registry)
    sched = schedule(IC_NETWORKS["mainnet"], params)
    assert len(sched["canary"][0]["nodes"]) == 2
    assert (
        sched["canary"][0]["nodes"][0]["node_id"]
        != sched["canary"][0]["nodes"][1]["node_id"]
    )


def test_exclude_hk_nodes(mocker: Any, registry: dre.RegistrySnapshot) -> None:
    "Schedule should bomb if one stage tries to upgrade too many nodes."
    """Tests that the default rollout plan spec works."""
    spec_without_exclusion = textwrap.dedent("""\
        stages:
          canary:
          - selectors:
                assignment: unassigned
                nodes_per_group: 120
        resume_at: 7:00
        suspend_at: 15:00
        minimum_minutes_per_batch: 30
        """)
    spec_with_exclusion = textwrap.dedent("""\
        stages:
          canary:
          - selectors:
              intersect:
              - assignment: unassigned
                nodes_per_group: 120
              - not:
                  datacenter: hk4
        resume_at: 7:00
        suspend_at: 15:00
        minimum_minutes_per_batch: 30
        """)
    mocker.patch("dfinity.dre.DRE.get_registry", return_value=registry)
    sched_without_exclusion = schedule(
        IC_NETWORKS["mainnet"],
        {"simulate": True, "plan": spec_without_exclusion, "git_revision": "0"},
    )
    sched_with_exclusion = schedule(
        IC_NETWORKS["mainnet"],
        {"simulate": True, "plan": spec_with_exclusion, "git_revision": "0"},
    )
    assert any(
        n["dc_id"] == "hk4" for n in sched_without_exclusion["canary"][0]["nodes"]
    )
    assert len(sched_with_exclusion["canary"][0]["nodes"])
    assert not any(
        n["dc_id"] == "hk4" for n in sched_with_exclusion["canary"][0]["nodes"]
    )


def test_subnet_healthy_threshold_filters_unhealthy_subnets(
    mocker: Any, registry: dre.RegistrySnapshot
) -> None:
    """Tests that subnet_healthy_threshold filters out subnets with insufficient healthy nodes.

    The mock registry contains 42 subnets with 635 total healthy assigned nodes:
    - 41 subnets with 100% healthy nodes (13 nodes each, or 34-40 for larger ones)
    - 1 subnet (mkbc3-...) with 12/13 healthy nodes (92.31%)

    When a subnet doesn't qualify due to threshold, all its healthy nodes are excluded.
    """
    # Without threshold: should get all 635 healthy nodes from all 42 subnets
    spec_without_threshold = textwrap.dedent("""\
        stages:
          main:
            selectors:
              assignment: assigned
              group_by: subnet
              nodes_per_group: 1
              status: Healthy
        resume_at: 7:00
        suspend_at: 15:00
        minimum_minutes_per_batch: 30
        """)
    # With high threshold (100): should only get nodes from subnets with > 100 healthy nodes
    # No subnet has more than 40 nodes, so this should result in 0 nodes
    spec_with_high_threshold = textwrap.dedent("""\
        stages:
          main:
            selectors:
              assignment: assigned
              group_by: subnet
              nodes_per_group: 1
              status: Healthy
              subnet_healthy_threshold: 100
        resume_at: 7:00
        suspend_at: 15:00
        minimum_minutes_per_batch: 30
        """)
    # With threshold 5: all 42 subnets have > 5 healthy nodes -> all 635 nodes
    spec_with_threshold_5 = textwrap.dedent("""\
        stages:
          main:
            selectors:
              assignment: assigned
              group_by: subnet
              nodes_per_group: 1
              status: Healthy
              subnet_healthy_threshold: 5
        resume_at: 7:00
        suspend_at: 15:00
        minimum_minutes_per_batch: 30
        """)
    # With threshold 12: mkbc3 has exactly 12 healthy (not > 12), so excluded
    # 635 - 12 = 623 nodes scheduled
    spec_with_threshold_12 = textwrap.dedent("""\
        stages:
          main:
            selectors:
              assignment: assigned
              group_by: subnet
              nodes_per_group: 1
              status: Healthy
              subnet_healthy_threshold: 12
        resume_at: 7:00
        suspend_at: 15:00
        minimum_minutes_per_batch: 30
        """)
    mocker.patch("dfinity.dre.DRE.get_registry", return_value=registry)

    sched_without = schedule(
        IC_NETWORKS["mainnet"],
        {"simulate": True, "plan": spec_without_threshold, "git_revision": "0"},
    )
    sched_high = schedule(
        IC_NETWORKS["mainnet"],
        {"simulate": True, "plan": spec_with_high_threshold, "git_revision": "0"},
    )
    sched_5 = schedule(
        IC_NETWORKS["mainnet"],
        {"simulate": True, "plan": spec_with_threshold_5, "git_revision": "0"},
    )
    sched_12 = schedule(
        IC_NETWORKS["mainnet"],
        {"simulate": True, "plan": spec_with_threshold_12, "git_revision": "0"},
    )

    # Helper to count total nodes across all batches
    def count_nodes(batches: list) -> int:
        return sum(len(batch["nodes"]) for batch in batches)

    # Without threshold: all 42 subnets qualify -> 635 healthy nodes scheduled
    assert count_nodes(sched_without["main"]) == 635

    # With threshold 100: no subnet qualifies -> 0 nodes scheduled
    assert len(sched_high["main"]) == 0

    # With threshold 5: all 42 subnets qualify -> 635 nodes scheduled
    assert count_nodes(sched_5["main"]) == 635

    # With threshold 12: 41 subnets qualify (mkbc3's 12 nodes excluded) -> 623 nodes
    assert count_nodes(sched_12["main"]) == 623


def test_subnet_healthy_threshold_percentage(
    mocker: Any, registry: dre.RegistrySnapshot
) -> None:
    """Tests that subnet_healthy_threshold works with percentage values.

    The mock registry contains 42 subnets with 635 total healthy assigned nodes:
    - 41 subnets with 100% healthy nodes
    - 1 subnet (mkbc3-...) with 12/13 healthy nodes (92.31%)

    When a subnet doesn't qualify due to threshold, all its healthy nodes are excluded.
    """
    # With 99% threshold: only subnets with > 99% healthy nodes qualify
    # 41 subnets are 100% healthy, mkbc3 is 92.31% -> 623 nodes (635 - 12)
    spec_99_percent = textwrap.dedent("""\
        stages:
          main:
            selectors:
              assignment: assigned
              group_by: subnet
              nodes_per_group: 1
              status: Healthy
              subnet_healthy_threshold: 99%
        resume_at: 7:00
        suspend_at: 15:00
        minimum_minutes_per_batch: 30
        """)
    # With 92% threshold: subnets with > 92% healthy qualify
    # mkbc3 is 92.31% which is > 92%, so all 42 subnets qualify -> 635 nodes
    spec_92_percent = textwrap.dedent("""\
        stages:
          main:
            selectors:
              assignment: assigned
              group_by: subnet
              nodes_per_group: 1
              status: Healthy
              subnet_healthy_threshold: 92%
        resume_at: 7:00
        suspend_at: 15:00
        minimum_minutes_per_batch: 30
        """)
    # With 50% threshold: all subnets have > 50% healthy -> all 635 nodes
    spec_50_percent = textwrap.dedent("""\
        stages:
          main:
            selectors:
              assignment: assigned
              group_by: subnet
              nodes_per_group: 1
              status: Healthy
              subnet_healthy_threshold: 50%
        resume_at: 7:00
        suspend_at: 15:00
        minimum_minutes_per_batch: 30
        """)
    mocker.patch("dfinity.dre.DRE.get_registry", return_value=registry)

    sched_99 = schedule(
        IC_NETWORKS["mainnet"],
        {"simulate": True, "plan": spec_99_percent, "git_revision": "0"},
    )
    sched_92 = schedule(
        IC_NETWORKS["mainnet"],
        {"simulate": True, "plan": spec_92_percent, "git_revision": "0"},
    )
    sched_50 = schedule(
        IC_NETWORKS["mainnet"],
        {"simulate": True, "plan": spec_50_percent, "git_revision": "0"},
    )

    # Helper to count total nodes across all batches
    def count_nodes(batches: list) -> int:
        return sum(len(batch["nodes"]) for batch in batches)

    # With 99% threshold: 41 subnets qualify (mkbc3's 12 nodes excluded) -> 623 nodes
    assert count_nodes(sched_99["main"]) == 623

    # With 92% threshold: all 42 subnets qualify (mkbc3 at 92.31% > 92%) -> 635 nodes
    assert count_nodes(sched_92["main"]) == 635

    # With 50% threshold: all 42 subnets qualify -> 635 nodes
    assert count_nodes(sched_50["main"]) == 635
