from __future__ import annotations

import tempfile
import unittest
from fractions import Fraction
from pathlib import Path
from unittest.mock import patch
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lineage_ledger import ConflictRejected, Ledger, ValidationRejected


def merge_split_ledger(path: str = ":memory:") -> Ledger:
    ledger = Ledger.open(path)
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


class ExactLineageTests(unittest.TestCase):
    def test_merge_then_split_uses_exact_ratios(self) -> None:
        ledger = merge_split_ledger()
        report = ledger.sample("C")
        self.assertEqual("available", report["status"])
        self.assertEqual("1", report["available_quantity"])
        amounts = {
            x["original_id"]: Fraction(int(x["numerator"]), int(x["denominator"]))
            for x in report["precise_contributions"]
        }
        self.assertEqual({ "A": Fraction(1, 3), "B": Fraction(2, 3)}, amounts)

        mix = ledger.sample("MIX")
        mix_amounts = {
            x["original_id"]: Fraction(int(x["numerator"]), int(x["denominator"]))
            for x in mix["precise_contributions"]
        }
        # MIX started at A=1, B=2, then lost exactly one third pro rata.
        self.assertEqual("2", mix["available_quantity"])
        self.assertEqual({"A": Fraction(2, 3), "B": Fraction(4, 3)}, mix_amounts)
        ledger.close()

    def test_unit_conversion_is_exact_rational(self) -> None:
        ledger = Ledger.open(":memory:")
        ledger.register_unit("BOX", Fraction(6, 1), "one box is six base units")
        ledger.submit_event(
            {
                "event_id": "O-U",
                "seq": 1,
                "rev": 1,
                "type": "original",
                "sample_id": "U",
                "base_amount": 10,
            }
        )
        ledger.submit_event(
            {
                "event_id": "T-U",
                "seq": 2,
                "rev": 1,
                "type": "transfer",
                "source": "U",
                "destination": "V",
                "amount": "1/2",
                "unit": "BOX",  # 1/2 * 6 = 3 base units
            }
        )
        self.assertEqual("3", ledger.sample("V")["available_quantity"])
        ledger.close()

    def test_later_exhaustion_preserves_earlier_reference(self) -> None:
        ledger = merge_split_ledger()
        ledger.add_citation("DET-BEFORE-DRAIN", "C", 4)
        before = ledger.get_citation("DET-BEFORE-DRAIN")
        self.assertEqual("traceable", before["status"])

        ledger.submit_event(
            {
                "event_id": "D-C",
                "seq": 5,
                "rev": 1,
                "type": "discard",
                "sample": "C",
                "amount": 1,
            }
        )
        # The old business-time interpretation is unchanged.
        old = ledger.get_citation("DET-BEFORE-DRAIN", version=1)
        self.assertEqual("traceable", old["status"])
        self.assertIn("1/3", old["explanation_text"])

        # A reference made after exhaustion is pending.
        ledger.add_citation("DET-AFTER-DRAIN", "C", 5)
        after = ledger.get_citation("DET-AFTER-DRAIN")
        self.assertEqual("pending", after["status"])
        self.assertEqual(
            "no_quantity_at_reference_time", after["reason_code"]
        )

        current = ledger.sample("C")
        self.assertEqual("exhausted", current["status"])
        self.assertEqual("0", current["available_quantity"])
        historical = {
            x["original_id"]: x["entered_amount"]
            for x in current["historical_input_contributions"]
        }
        self.assertEqual({"A": "1/3", "B": "2/3"}, historical)
        ledger.close()

    def test_late_predecessor_activates_waiting_lineage(self) -> None:
        ledger = Ledger.open(":memory:")
        waiting = ledger.submit_event(
            {
                "event_id": "LATE-USE",
                "seq": 20,
                "rev": 1,
                "type": "split",
                "source": "LATE-ROOT",
                "outputs": [{"sample_id": "LATE-CHILD", "amount": 4}],
            }
        )
        self.assertEqual("blocked", waiting["derivation"]["status"])
        self.assertEqual(
            "predecessor_not_formed", waiting["derivation"]["reason_code"]
        )
        ledger.add_citation("DET-LATE", "LATE-CHILD", 20)
        self.assertEqual("pending", ledger.get_citation("DET-LATE")["status"])

        arrived = ledger.submit_event(
            {
                "event_id": "LATE-ROOT-E",
                "seq": 10,
                "rev": 1,
                "type": "original",
                "sample_id": "LATE-ROOT",
                "base_amount": 10,
            }
        )
        self.assertEqual(
            ["LATE-ROOT-E", "LATE-USE"], arrived["affected_event_ids"]
        )
        self.assertEqual(
            [{"citation_id": "DET-LATE", "version": 2, "status": "traceable"}],
            arrived["restated_citations"],
        )
        versions = ledger.list_citation_versions("DET-LATE")
        self.assertEqual(
            [("pending", 1), ("traceable", 2)],
            [(v["status"], v["version"]) for v in versions],
        )
        self.assertEqual("6", ledger.sample("LATE-ROOT")["available_quantity"])
        self.assertEqual("4", ledger.sample("LATE-CHILD")["available_quantity"])
        ledger.close()

    def test_revision_causes_downstream_overquantity_and_restates(self) -> None:
        ledger = Ledger.open(":memory:")
        ledger.submit_event(
            {
                "event_id": "R-ROOT",
                "seq": 100,
                "rev": 1,
                "type": "original",
                "sample_id": "R-A",
                "base_amount": 10,
            }
        )
        ledger.submit_event(
            {
                "event_id": "R-SPLIT",
                "seq": 110,
                "rev": 1,
                "type": "split",
                "source": "R-A",
                "outputs": [{"sample_id": "R-C", "amount": 6}],
            }
        )
        ledger.add_citation("DET-R", "R-C", 110)
        self.assertEqual("traceable", ledger.get_citation("DET-R")["status"])

        revised = ledger.submit_event(
            {
                "event_id": "R-ROOT",
                "seq": 100,
                "rev": 2,
                "type": "original",
                "sample_id": "R-A",
                "base_amount": 5,
            }
        )
        self.assertEqual(["R-ROOT", "R-SPLIT"], revised["affected_event_ids"])
        split = ledger._derivation("R-SPLIT")
        self.assertEqual("blocked", split["status"])
        self.assertEqual("insufficient_quantity", split["reason_code"])
        self.assertEqual(
            {"available": "5", "requested": "6"},
            split["reason"]["problems"][0]
            if "problems" in split["reason"]
            else split["reason"],
        )

        citation_versions = ledger.list_citation_versions("DET-R")
        self.assertEqual(
            [("traceable", 1), ("pending", 2)],
            [(v["status"], v["version"]) for v in citation_versions],
        )
        old = ledger.get_citation("DET-R", version=1)
        new = ledger.get_citation("DET-R", version=2)
        self.assertEqual("traceable", old["status"])
        self.assertEqual("traceable", old["report"]["status"])
        self.assertEqual("pending", new["status"])
        ledger.close()

    def test_independent_lineage_is_not_recomputed_or_restated(self) -> None:
        ledger = Ledger.open(":memory:")
        ledger.submit_event(
            {
                "event_id": "I-ROOT",
                "seq": 200,
                "rev": 1,
                "type": "original",
                "sample_id": "I",
                "base_amount": 7,
            }
        )
        ledger.add_citation("DET-I", "I", 200)

        result = ledger.submit_event(
            {
                "event_id": "OTHER-ROOT",
                "seq": 201,
                "rev": 1,
                "type": "original",
                "sample_id": "OTHER",
                "base_amount": 1,
            }
        )
        self.assertEqual(["OTHER-ROOT"], result["affected_event_ids"])
        self.assertEqual([], result["restated_citations"])
        self.assertEqual(1, ledger.get_citation("DET-I")["version"])
        self.assertEqual("7", ledger.sample("I")["available_quantity"])
        ledger.close()

    def test_idempotent_retry_and_conflicting_same_revision(self) -> None:
        ledger = Ledger.open(":memory:")
        event = {
            "event_id": "E-IDEM",
            "seq": 300,
            "rev": 1,
            "type": "original",
            "sample_id": "IDEM",
            "base_amount": 4,
        }
        first = ledger.submit_event(event)
        second = ledger.submit_event(dict(event))
        self.assertEqual("accepted", first["status"])
        self.assertEqual("duplicate_noop", second["status"])
        self.assertEqual(first["content_hash"], second["content_hash"])

        with self.assertRaises(ConflictRejected):
            ledger.submit_event({**event, "base_amount": 5})
        ledger.close()

    def test_dependency_must_point_to_smaller_sequence(self) -> None:
        ledger = Ledger.open(":memory:")
        with self.assertRaises(ValidationRejected):
            ledger.submit_event(
                {
                    "event_id": "E-DEP-BAD",
                    "seq": 10,
                    "rev": 1,
                    "type": "discard",
                    "sample": "X",
                    "amount": 1,
                    "dependencies": [
                        {"event_id": "FUTURE", "seq": 10}
                    ],
                }
            )
        ledger.close()

    def test_derived_output_cannot_be_reregistered_as_original(self) -> None:
        ledger = Ledger.open(":memory:")
        ledger.submit_event(
            {
                "event_id": "D-ROOT",
                "seq": 1,
                "rev": 1,
                "type": "original",
                "sample_id": "D-A",
                "base_amount": 2,
            }
        )
        ledger.submit_event(
            {
                "event_id": "D-SPLIT",
                "seq": 2,
                "rev": 1,
                "type": "split",
                "source": "D-A",
                "outputs": [{"sample_id": "D-CHILD", "amount": 1}],
            }
        )
        result = ledger.submit_event(
            {
                "event_id": "D-FAKE-ORIGIN",
                "seq": 3,
                "rev": 1,
                "type": "original",
                "sample_id": "D-CHILD",
                "base_amount": 1,
            }
        )
        self.assertEqual("blocked", result["derivation"]["status"])
        self.assertEqual(
            "derived_sample_cannot_be_original",
            result["derivation"]["reason_code"],
        )
        self.assertFalse(ledger.sample("D-CHILD")["status"] == "available" and
                         ledger.sample("D-CHILD")["evidence"][0]["event_id"] == "D-FAKE-ORIGIN")
        ledger.close()

    def test_insufficient_event_blocks_later_same_sample_but_not_independent_lineage(self) -> None:
        ledger = Ledger.open(":memory:")
        ledger.submit_event(
            {
                "event_id": "Q-ROOT",
                "seq": 1,
                "rev": 1,
                "type": "original",
                "sample_id": "Q",
                "base_amount": 10,
            }
        )
        bad_discard = ledger.submit_event(
            {
                "event_id": "Q-DISCARD",
                "seq": 2,
                "rev": 1,
                "type": "discard",
                "sample": "Q",
                "amount": 20,
            }
        )
        later_split = ledger.submit_event(
            {
                "event_id": "Q-SPLIT",
                "seq": 3,
                "rev": 1,
                "type": "split",
                "source": "Q",
                "outputs": [{"sample_id": "Q-CHILD", "amount": 5}],
            }
        )
        independent = ledger.submit_event(
            {
                "event_id": "P-ROOT",
                "seq": 4,
                "rev": 1,
                "type": "original",
                "sample_id": "P",
                "base_amount": 3,
            }
        )

        self.assertEqual("blocked", bad_discard["derivation"]["status"])
        self.assertEqual("insufficient_quantity", bad_discard["derivation"]["reason_code"])
        self.assertEqual("blocked", later_split["derivation"]["status"])
        self.assertEqual("blocked_predecessor", later_split["derivation"]["reason_code"])
        self.assertEqual("valid", independent["derivation"]["status"])
        self.assertEqual("0", ledger.sample("Q-CHILD")["available_quantity"])
        self.assertEqual("3", ledger.sample("P")["available_quantity"])
        ledger.close()

    def test_transfer_preserves_exact_mixture(self) -> None:
        ledger = merge_split_ledger()
        ledger.submit_event(
            {
                "event_id": "T-1",
                "seq": 6,
                "rev": 1,
                "type": "transfer",
                "source": "MIX",
                "destination": "TUBE",
                "amount": 1,
            }
        )
        tube = ledger.sample("TUBE")
        self.assertEqual("1", tube["available_quantity"])
        amounts = {
            x["original_id"]: Fraction(int(x["numerator"]), int(x["denominator"]))
            for x in tube["precise_contributions"]
        }
        # MIX before transfer has A=2/3, B=4/3 in quantity 2; moving half.
        self.assertEqual({"A": Fraction(1, 3), "B": Fraction(2, 3)}, amounts)
        ledger.close()

    def test_event_after_citation_time_does_not_restate_that_citation(self) -> None:
        ledger = Ledger.open(":memory:")
        ledger.submit_event(
            {
                "event_id": "T-ROOT",
                "seq": 1,
                "rev": 1,
                "type": "original",
                "sample_id": "T",
                "base_amount": 5,
            }
        )
        ledger.add_citation("DET-T-EARLY", "T", at_seq=1)
        result = ledger.submit_event(
            {
                "event_id": "T-DISCARD",
                "seq": 2,
                "rev": 1,
                "type": "discard",
                "sample": "T",
                "amount": 5,
            }
        )
        self.assertEqual([], result["restated_citations"])
        self.assertEqual(1, ledger.get_citation("DET-T-EARLY")["version"])
        self.assertEqual("traceable", ledger.get_citation("DET-T-EARLY")["status"])
        ledger.close()

    def test_commit_is_atomic_when_derivation_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "atomic.sqlite3")
            ledger = Ledger.open(path)
            with patch(
                "lineage_ledger.ledger.recompute_closure",
                side_effect=RuntimeError("simulated crash"),
            ):
                with self.assertRaises(RuntimeError):
                    ledger.submit_event(
                        {
                            "event_id": "ATOMIC",
                            "seq": 1,
                            "rev": 1,
                            "type": "original",
                            "sample_id": "ATOMIC",
                            "base_amount": 1,
                        }
                    )
            ledger.close()

            reopened = Ledger.open(path)
            self.assertEqual([], reopened.list_events())
            with self.assertRaises(KeyError):
                reopened.get_event("ATOMIC")
            reopened.close()

    def test_persistent_file_reopens_with_committed_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "lineage.sqlite3")
            ledger = merge_split_ledger(path)
            ledger.add_citation("DET-P", "C", 4)
            ledger.close()

            reopened = Ledger.open(path)
            self.assertEqual("1", reopened.sample("C")["available_quantity"])
            citation = reopened.get_citation("DET-P")
            self.assertEqual("traceable", citation["status"])
            self.assertEqual(1, citation["version"])
            reopened.close()


if __name__ == "__main__":
    unittest.main()
