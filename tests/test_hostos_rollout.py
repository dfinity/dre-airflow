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
    assert len(canary) == 5
    assert len(res["main"]) == 39
    assert any(
        is_apibn(n, set(s["principal"] for s in registry["api_bns"]))
        for n in res["main"][3]["nodes"]
    )
    assert len(res["main"][-1]["nodes"]) == 1
    assert len(res["unassigned"]) == 7
    assert len(res["unassigned"][-1]["nodes"]) == 93
    assert len(res["stragglers"][0]["nodes"]) == 14
    batches = (
        [r for r in res["canary"]]
        + [r for r in res["main"]]
        + [r for r in res["unassigned"]]
        + [r for r in res["stragglers"]]
    )
    assert batches[-1]["start_at"] == datetime.datetime(2025, 7, 23, 8, 30)


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
