import datetime
import json
import os
import textwrap
from typing import Any

import pytest
from dfinity import dre
from dfinity.hostos_rollout import compute_provisional_plan, schedule
from dfinity.ic_os_rollout import DEFAULT_HOSTOS_ROLLOUT_PLANS
from dfinity.ic_types import IC_NETWORKS
from dfinity.rollout_types import yaml_to_HostOSRolloutPlanSpec


@pytest.fixture
def registry() -> dre.RegistrySnapshot:
    with open(os.path.join(os.path.dirname(__file__), "registry.json"), "r") as f:
        return json.load(f)


def test_simple_plan(registry: dre.RegistrySnapshot) -> None:
    """Tests that a valid rollout plan spec works."""
    spec = textwrap.dedent("""\
        stages:
          canary:
          - 
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

    spec = yaml_to_HostOSRolloutPlanSpec(DEFAULT_HOSTOS_ROLLOUT_PLANS["mainnet"])
    res = compute_provisional_plan(
        "000000000",
        5,
        40,
        15,
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
    assert len(res["main"][-1]["nodes"]) == 1
    assert len(res["unassigned"]) == 8
    assert len(res["unassigned"][-1]["nodes"]) == 83
    assert len(res["stragglers"]["nodes"]) == 14
    batches = (
        [r for r in res["canary"]]
        + [r for r in res["main"]]
        + [res["unassigned"], res["stragglers"]]
    )
    assert batches[-1]["start_at"] == datetime.datetime(2025, 7, 28, 13, 0)


def test_schedule_bombs_with_too_many_nodes(
    mocker: Any, registry: dre.RegistrySnapshot
) -> None:
    "Schedule should bomb if one stage tries to upgrade too many nodes."
    """Tests that the default rollout plan spec works."""
    spec = textwrap.dedent("""\
        stages:
          canary:
          - [] # should cause ALL nodes to be upgraded
               # in this stage
        resume_at: 7:00
        suspend_at: 15:00
        minimum_minutes_per_batch: 30
        """)
    params = {"simulate": True, "plan": spec, "git_revision": "0"}
    mocker.patch("dfinity.dre.DRE.get_registry", return_value=registry)
    with pytest.raises(AssertionError):
        schedule(IC_NETWORKS["mainnet"], params)
