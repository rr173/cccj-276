from __future__ import annotations

import sys
import unittest
from fractions import Fraction
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lineage_ledger import ConflictRejected, Ledger


def mixed_ledger() -> Ledger:
    """A=(3) and B=(5) are merged into MIX (A=1, B=2); C is split off (1)."""
    ledger = Ledger.open(":memory:")
    ledger.submit_event(
        {
            "event_id": "O-A",
            "seq": 1,
            "rev": 1,
            "type": "original",
            "sample_id": "A",
            "base_amount": 3,
        }
    )
    ledger.submit_event(
        {
            "event_id": "O-B",
            "seq": 2,
            "rev": 1,
            "type": "original",
            "sample_id": "B",
            "base_amount": 5,
        }
    )
    ledger.submit_event(
        {
            "event_id": "M-1",
            "seq": 3,
            "rev": 1,
            "type": "merge",
            "sources": [
                {"sample_id": "A", "amount": 1},
                {"sample_id": "B", "amount": 2},
            ],
            "output": "MIX",
        }
    )
    ledger.submit_event(
        {
            "event_id": "S-1",
            "seq": 4,
            "rev": 1,
            "type": "split",
            "source": "MIX",
            "outputs": [{"sample_id": "C", "amount": 1}],
        }
    )
    return ledger


def contributions(report: dict) -> dict[str, str]:
    return {x["original_id"]: x["amount"] for x in report["precise_contributions"]}


class QuarantineTests(unittest.TestCase):
    def test_quarantine_keeps_inventory_but_blocks_whole_mixture(self) -> None:
        ledger = mixed_ledger()
        before = ledger.sample("C")
        self.assertEqual("1", before["available_quantity"])
        self.assertTrue(before["releasable"])
        self.assertEqual("1", before["releasable_quantity"])

        ledger.quarantine_source("Q-A", 5, "A", reason="source contaminated")

        after = ledger.sample("C")
        # Physical inventory and historical composition are untouched.
        self.assertEqual("available", after["status"])
        self.assertEqual("1", after["available_quantity"])
        self.assertEqual({"A": "1/3", "B": "2/3"}, contributions(after))
        # The homogeneously mixed whole is not releasable, only the source named.
        self.assertFalse(after["releasable"])
        self.assertEqual("0", after["releasable_quantity"])
        self.assertEqual("quarantined_source", after["release_block_reason"])
        self.assertEqual(["A"], [b["original_id"] for b in after["blocked_sources"]])
        self.assertEqual("1/3", after["blocked_sources"][0]["amount"])
        self.assertEqual("Q-A", after["blocked_sources"][0]["quarantine_event_id"])

        # The pure B lineage is unaffected.
        independent = ledger.sample("B")
        self.assertTrue(independent["releasable"])
        self.assertEqual("3", independent["releasable_quantity"])
        # A itself contains its own quarantined source.
        self.assertFalse(ledger.sample("A")["releasable"])
        ledger.close()

    def test_as_of_only_applies_records_effective_at_that_seq(self) -> None:
        ledger = mixed_ledger()
        ledger.quarantine_source("Q-A", 5, "A")
        self.assertTrue(ledger.sample("MIX", at_seq=4)["releasable"])
        self.assertEqual("2", ledger.sample("MIX", at_seq=4)["releasable_quantity"])
        self.assertFalse(ledger.sample("MIX", at_seq=5)["releasable"])
        self.assertEqual("0", ledger.sample("MIX", at_seq=5)["releasable_quantity"])
        self.assertEqual(["A"], ledger.sample("MIX", at_seq=5)["quarantined_source_ids"])
        ledger.close()

    def test_release_restores_releasability_of_remaining_inventory(self) -> None:
        ledger = mixed_ledger()
        ledger.quarantine_source("Q-A", 5, "A")
        ledger.submit_event(
            {
                "event_id": "D-C",
                "seq": 6,
                "rev": 1,
                "type": "discard",
                "sample": "C",
                "amount": "1/2",
            }
        )
        half = ledger.sample("C")
        self.assertEqual("1/2", half["available_quantity"])
        self.assertFalse(half["releasable"])

        ledger.release_source("R-A", 7, "A", reason="false positive")
        restored = ledger.sample("C")
        self.assertEqual("1/2", restored["available_quantity"])
        self.assertTrue(restored["releasable"])
        self.assertEqual("1/2", restored["releasable_quantity"])
        self.assertEqual([], restored["blocked_sources"])
        ledger.close()

    def test_last_relevant_quarantine_released_but_unrelated_one_remains(self) -> None:
        ledger = mixed_ledger()
        ledger.quarantine_source("Q-A", 5, "C-ONLY-A")
        # quarantine applies to source ids, not sample ids; seed a real source.
        ledger.release_source("R-C-ONLY-A", 6, "C-ONLY-A")
        ledger.quarantine_source("Q-B", 7, "B")
        ledger.quarantine_source("Q-A2", 8, "A")
        ledger.release_source("R-A2", 9, "A")
        report = ledger.sample("MIX")
        self.assertFalse(report["releasable"])
        self.assertEqual(["B"], report["quarantined_source_ids"])
        ledger.release_source("R-B", 10, "B")
        self.assertTrue(ledger.sample("MIX")["releasable"])
        ledger.close()

    def test_revision_corrects_source_and_retracts_judgement(self) -> None:
        ledger = mixed_ledger()
        ledger.quarantine_source("Q-SRC", 5, "A", reason="preliminary")
        self.assertFalse(ledger.sample("MIX")["releasable"])

        corrected = ledger.submit_event(
            {
                "event_id": "Q-SRC",
                "seq": 5,
                "rev": 2,
                "type": "quarantine",
                "source_id": "B",
                "reason": "corrected to B",
            }
        )
        self.assertEqual("revision_accepted", corrected["status"])
        report = ledger.sample("MIX")
        self.assertFalse(report["releasable"])
        self.assertEqual(["B"], report["quarantined_source_ids"])
        # A's own inventory is now releasable again; the judgement follows the
        # corrected lineage, not the superseded revision.
        self.assertTrue(ledger.sample("A")["releasable"])
        ledger.close()

    def test_duplicate_retry_is_noop_and_same_rev_conflict_rejected(self) -> None:
        ledger = mixed_ledger()
        event = {
            "event_id": "Q-DUP",
            "seq": 5,
            "rev": 1,
            "type": "quarantine",
            "source_id": "A",
            "reason": "r",
        }
        first = ledger.submit_event(event)
        duplicate = ledger.submit_event(dict(event))
        self.assertEqual("accepted", first["status"])
        self.assertEqual("duplicate_noop", duplicate["status"])
        self.assertEqual([], duplicate["restated_citations"])
        self.assertEqual(1, len(ledger.source_actions(source_id="A")))

        with self.assertRaises(ConflictRejected):
            ledger.submit_event({**event, "source_id": "B"})
        ledger.close()

    def test_source_revision_restates_only_citations_at_or_after_seq(self) -> None:
        ledger = mixed_ledger()
        ledger.add_citation("DET-BEFORE", "MIX", at_seq=3)
        ledger.add_citation("DET-AT", "MIX", at_seq=5)
        result = ledger.quarantine_source("Q-LATE", 5, "A", reason="late finding")
        self.assertEqual(
            ["DET-AT"],
            [r["citation_id"] for r in result["restated_citations"]],
        )
        self.assertEqual("traceable", ledger.get_citation("DET-BEFORE")["status"])
        self.assertEqual("pending", ledger.get_citation("DET-AT")["status"])
        self.assertEqual(
            "quarantined_source_at_reference_time",
            ledger.get_citation("DET-AT")["reason_code"],
        )
        # Pre-quarantine interpretation text is immutable.
        self.assertEqual(1, ledger.get_citation("DET-BEFORE")["version"])
        ledger.close()

    def test_blocked_source_record_has_no_effect(self) -> None:
        ledger = mixed_ledger()
        result = ledger.quarantine_source(
            "Q-BLOCKED",
            5,
            "A",
            dependencies=[{"event_id": "NEVER-ARRIVES", "seq": 4}],
        )
        self.assertEqual("blocked", result["derivation"]["status"])
        self.assertTrue(ledger.sample("MIX")["releasable"])
        self.assertEqual([], ledger.source_actions())
        ledger.close()

    def test_quarantine_does_not_rewrite_pre_quarantine_citation(self) -> None:
        ledger = mixed_ledger()
        ledger.add_citation("DET-OLD", "MIX", at_seq=3)
        self.assertEqual("traceable", ledger.get_citation("DET-OLD")["status"])
        ledger.quarantine_source("Q-A", 5, "A")
        old = ledger.get_citation("DET-OLD", version=1)
        self.assertEqual("traceable", old["status"])
        self.assertEqual("3", old["report"]["sample_report"]["available_quantity"])
        self.assertTrue(old["report"]["sample_report"]["releasable"])
        ledger.close()

    def test_full_drain_of_mixed_sample_keeps_historical_inputs(self) -> None:
        ledger = mixed_ledger()
        ledger.quarantine_source("Q-A", 5, "A")
        ledger.submit_event(
            {
                "event_id": "DRAIN",
                "seq": 6,
                "rev": 1,
                "type": "discard",
                "sample": "C",
                "amount": 1,
            }
        )
        report = ledger.sample("C")
        self.assertEqual("exhausted", report["status"])
        self.assertEqual("0", report["available_quantity"])
        self.assertFalse(report["releasable"])
        self.assertEqual("no_available_quantity", report["release_block_reason"])
        self.assertEqual(
            {"A": "1/3", "B": "2/3"},
            {x["original_id"]: x["entered_amount"] for x in report["historical_input_contributions"]},
        )
        ledger.close()


if __name__ == "__main__":
    unittest.main()
