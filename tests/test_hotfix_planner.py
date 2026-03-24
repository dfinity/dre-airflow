import unittest

from dfinity.ic_os_rollout import (
    NNS_SUBNET_PREFIX,
    SubnetRolloutPlanWithRevision,
    extract_subnets_from_plan,
    hotfix_planner,
)


class TestExtractSubnetsFromPlan(unittest.TestCase):
    def test_monday_subnets_are_canary(self) -> None:
        plan = """
Monday:
  9:00:  [aaa, bbb]
  11:00: [ccc]
Tuesday:
  7:00:  [ddd, eee, fff]
"""
        canary, batch = extract_subnets_from_plan(plan)
        assert canary == ["aaa", "bbb", "ccc"]
        assert batch == ["ddd", "eee", "fff"]

    def test_monday_next_week_is_not_canary(self) -> None:
        plan = """
Monday:
  9:00: [aaa]
Tuesday:
  7:00: [bbb]
Monday next week:
  7:00:
    subnets: [nns]
    batch: 30
"""
        canary, batch = extract_subnets_from_plan(plan)
        assert canary == ["aaa"]
        assert batch == ["bbb", "nns"]

    def test_dict_subnet_spec(self) -> None:
        plan = """
Monday:
  9:00:
    subnets:
    - subnet: aaa
      git_revision: 0123456789012345678901234567890123456789
Tuesday:
  7:00: [bbb]
"""
        canary, batch = extract_subnets_from_plan(plan)
        assert canary == ["aaa"]
        assert batch == ["bbb"]

    def test_empty_plan(self) -> None:
        plan = """
Monday:
  9:00: []
"""
        canary, batch = extract_subnets_from_plan(plan)
        assert canary == []
        assert batch == []

    def test_no_monday(self) -> None:
        plan = """
Tuesday:
  9:00: [aaa, bbb]
"""
        canary, batch = extract_subnets_from_plan(plan)
        assert canary == []
        assert batch == ["aaa", "bbb"]


class TestHotfixPlanner(unittest.TestCase):
    NNS = f"{NNS_SUBNET_PREFIX}-full-principal-id"

    def fake_subnet_list(self) -> list[str]:
        return [
            "aaa-full-principal-id",
            "bbb-full-principal-id",
            "ccc-full-principal-id",
            "ddd-full-principal-id",
            "eee-full-principal-id",
            "fff-full-principal-id",
            "ggg-full-principal-id",
            "hhh-full-principal-id",
            "iii-full-principal-id",
            self.NNS,
        ]

    def subnet_ids_per_batch(self, plan: SubnetRolloutPlanWithRevision) -> list[list[str]]:
        """Extract subnet IDs grouped by batch in order."""
        result = []
        for nstr in sorted(plan.keys(), key=int):
            _, members = plan[nstr]
            result.append([m.subnet_id for m in members])
        return result

    def test_canary_are_single_batches(self) -> None:
        plan = hotfix_planner(
            canary_abbreviations=["aaa", "bbb"],
            batch_abbreviations=["ccc", "ddd", "eee"],
            subnet_list_source=self.fake_subnet_list,
            git_revision="a" * 40,
        )
        batches = self.subnet_ids_per_batch(plan)
        # 2 canary (one each) + 1 batch of 3
        assert len(batches) == 3
        assert len(batches[0]) == 1  # canary 1
        assert len(batches[1]) == 1  # canary 2
        assert len(batches[2]) == 3  # batch of 3

    def test_nns_is_always_last(self) -> None:
        plan = hotfix_planner(
            canary_abbreviations=["aaa"],
            batch_abbreviations=["bbb", "ccc", NNS_SUBNET_PREFIX],
            subnet_list_source=self.fake_subnet_list,
            git_revision="a" * 40,
        )
        batches = self.subnet_ids_per_batch(plan)
        last_batch = batches[-1]
        assert len(last_batch) == 1
        assert last_batch[0].startswith(NNS_SUBNET_PREFIX)

    def test_nns_not_grouped_with_others(self) -> None:
        plan = hotfix_planner(
            canary_abbreviations=["aaa"],
            batch_abbreviations=["bbb", "ccc", NNS_SUBNET_PREFIX],
            subnet_list_source=self.fake_subnet_list,
            git_revision="a" * 40,
        )
        batches = self.subnet_ids_per_batch(plan)
        # canary(aaa) + batch(bbb,ccc) + nns
        assert len(batches) == 3
        assert batches[0] == ["aaa-full-principal-id"]
        assert batches[1] == ["bbb-full-principal-id", "ccc-full-principal-id"]
        assert batches[2] == [self.NNS]

    def test_batches_of_three(self) -> None:
        plan = hotfix_planner(
            canary_abbreviations=["aaa"],
            batch_abbreviations=[
                "bbb", "ccc", "ddd", "eee", "fff", "ggg", "hhh",
            ],
            subnet_list_source=self.fake_subnet_list,
            git_revision="a" * 40,
            subnets_per_batch=3,
        )
        batches = self.subnet_ids_per_batch(plan)
        # canary(1) + 3 batches(3,3,1)
        assert len(batches) == 4
        assert len(batches[0]) == 1  # canary
        assert len(batches[1]) == 3
        assert len(batches[2]) == 3
        assert len(batches[3]) == 1  # remainder

    def test_git_revision_applied_to_all(self) -> None:
        rev = "b" * 40
        plan = hotfix_planner(
            canary_abbreviations=["aaa"],
            batch_abbreviations=["bbb"],
            subnet_list_source=self.fake_subnet_list,
            git_revision=rev,
        )
        for _, (_, members) in plan.items():
            for m in members:
                assert m.git_revision == rev

    def test_unknown_abbreviation_raises(self) -> None:
        self.assertRaises(
            ValueError,
            lambda: hotfix_planner(
                canary_abbreviations=["zzz"],
                batch_abbreviations=[],
                subnet_list_source=self.fake_subnet_list,
                git_revision="a" * 40,
            ),
        )

    def test_ambiguous_abbreviation_raises(self) -> None:
        def ambiguous_list() -> list[str]:
            return ["aaa-001", "aaa-002"]

        self.assertRaises(
            ValueError,
            lambda: hotfix_planner(
                canary_abbreviations=["aaa"],
                batch_abbreviations=[],
                subnet_list_source=ambiguous_list,
                git_revision="a" * 40,
            ),
        )
