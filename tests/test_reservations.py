from __future__ import annotations

import os
import sys
import tempfile
import threading
import unittest
from fractions import Fraction
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lineage_ledger import ConflictRejected, Ledger, ValidationRejected
import lineage_ledger.ledger as ledger_module


def simple_ledger() -> Ledger:
    ledger = Ledger.open(":memory:")
    ledger.submit_event(
        {
            "event_id": "O-A",
            "seq": 1,
            "rev": 1,
            "type": "original",
            "sample_id": "A",
            "base_amount": 10,
        }
    )
    return ledger


def mixed_ledger() -> Ledger:
    ledger = simple_ledger()
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
                {"sample_id": "A", "amount": 4},
                {"sample_id": "B", "amount": 2},
            ],
            "output": "MIX",
        }
    )
    return ledger


class ReservationAcceptanceTests(unittest.TestCase):
    def test_reservation_does_not_change_inventory_or_lineage(self) -> None:
        ledger = mixed_ledger()
        before = ledger.sample("A")
        ledger.reserve_sample("REQ-1", "A", 4, note="plan 1")
        after = ledger.sample("A")
        # No effect was written: physical stock and composition are identical.
        self.assertEqual(before["available_quantity"], after["available_quantity"])
        self.assertEqual(
            before["precise_contributions"], after["precise_contributions"]
        )
        self.assertEqual(before["releasable_quantity"], after["releasable_quantity"])
        ledger.close()

    def test_reserve_exact_amount_and_query_remaining_bookable(self) -> None:
        ledger = simple_ledger()
        result = ledger.reserve_sample("REQ-1", "A", 4)
        self.assertEqual("accepted", result["request_outcome"])
        self.assertEqual("4", result["remaining_quantity"])
        self.assertEqual("active", result["status"])
        self.assertTrue(result["coverage"]["fully_covered"])

        capacity = ledger.reservable_quantity("A")
        self.assertEqual("10", capacity["releasable_quantity"])
        self.assertEqual("4", capacity["reserved_quantity"])
        self.assertEqual("6", capacity["bookable_quantity"])
        self.assertTrue(capacity["fully_covered"])
        self.assertEqual("0", capacity["uncovered_total"])
        ledger.close()

    def test_concurrent_reservations_cannot_exceed_releasable_stock(self) -> None:
        ledger = simple_ledger()
        ledger.reserve_sample("REQ-1", "A", 6)
        ledger.reserve_sample("REQ-2", "A", 3)
        # 9 of 10 booked; a request for 2 must be refused entirely.
        with self.assertRaises(ConflictRejected):
            ledger.reserve_sample("REQ-3", "A", 2)
        # The refused request leaves no trace and consumes nothing.
        self.assertEqual(
            ["REQ-1", "REQ-2"],
            [r["request_id"] for r in ledger.list_reservations()],
        )
        self.assertEqual("1", ledger.reservable_quantity("A")["bookable_quantity"])
        ledger.close()

    def test_all_or_nothing_rejection(self) -> None:
        ledger = simple_ledger()
        ledger.reserve_sample("REQ-1", "A", 8)
        with self.assertRaises(ConflictRejected):
            ledger.reserve_sample("REQ-2", "A", 3)
        # Nothing was partially booked.
        self.assertEqual("2", ledger.reservable_quantity("A")["bookable_quantity"])
        ledger.close()

    def test_exact_fraction_amounts_are_supported(self) -> None:
        ledger = simple_ledger()
        ledger.reserve_sample("REQ-1", "A", "1/3")
        ledger.reserve_sample("REQ-2", "A", {"num": 1, "den": 3})
        capacity = ledger.reservable_quantity("A")
        self.assertEqual("2/3", capacity["reserved_quantity"])
        self.assertEqual("28/3", capacity["bookable_quantity"])
        with self.assertRaises(ValidationRejected):
            ledger.reserve_sample("REQ-BAD", "A", 0)
        with self.assertRaises(ValidationRejected):
            ledger.reserve_sample("REQ-BAD2", "A", -1)
        ledger.close()

    def test_retry_same_request_id_is_idempotent_no_double_hold(self) -> None:
        ledger = simple_ledger()
        first = ledger.reserve_sample("REQ-1", "A", 4, note="x")
        second = ledger.reserve_sample("REQ-1", "A", 4, note="x")
        self.assertEqual("accepted", first["request_outcome"])
        self.assertEqual("duplicate_noop", second["request_outcome"])
        self.assertEqual("4", ledger.reservable_quantity("A")["reserved_quantity"])
        ledger.close()

    def test_same_request_id_different_content_is_rejected(self) -> None:
        ledger = simple_ledger()
        ledger.reserve_sample("REQ-1", "A", 4)
        with self.assertRaises(ConflictRejected):
            ledger.reserve_sample("REQ-1", "A", 5)
        with self.assertRaises(ConflictRejected):
            ledger.reserve_sample("REQ-1", "B", 4)
        # Original reservation is intact.
        self.assertEqual("4", ledger.get_reservation("REQ-1")["remaining_quantity"])
        ledger.close()

    def test_cannot_reserve_non_releasable_or_unformed_sample(self) -> None:
        ledger = mixed_ledger()
        with self.assertRaises(ConflictRejected):
            ledger.reserve_sample("REQ-X", "GHOST", 1)
        ledger.quarantine_source("Q-A", 10, "A")
        with self.assertRaises(ConflictRejected):
            ledger.reserve_sample("REQ-Q", "A", 1)
        with self.assertRaises(ConflictRejected):
            ledger.reserve_sample("REQ-MIX", "MIX", 1)
        # The pure-B lineage remains bookable.
        result = ledger.reserve_sample("REQ-B", "B", 2)
        self.assertEqual("accepted", result["request_outcome"])
        ledger.close()


class CancelAndFulfillTests(unittest.TestCase):
    def test_partial_and_full_cancel_releases_capacity(self) -> None:
        ledger = simple_ledger()
        ledger.reserve_sample("REQ-1", "A", 4)
        result = ledger.cancel_reservation(
            "REQ-1", amount="1/2", operation_id="C-1"
        )
        self.assertEqual("accepted", result["status"])
        self.assertEqual("7/2", result["reservation"]["remaining_quantity"])
        self.assertEqual("active", result["reservation"]["status"])
        self.assertEqual("13/2", ledger.reservable_quantity("A")["bookable_quantity"])

        full = ledger.cancel_reservation("REQ-1", operation_id="C-2")
        self.assertEqual("cancelled", full["reservation"]["status"])
        self.assertEqual("0", full["reservation"]["remaining_quantity"])
        self.assertEqual("10", ledger.reservable_quantity("A")["bookable_quantity"])
        ledger.close()

    def test_cancel_is_bounded_by_remaining_amount(self) -> None:
        ledger = simple_ledger()
        ledger.reserve_sample("REQ-1", "A", 2)
        with self.assertRaises(ConflictRejected):
            ledger.cancel_reservation("REQ-1", amount=3, operation_id="C-1")
        ledger.cancel_reservation("REQ-1", operation_id="C-2")
        with self.assertRaises(ConflictRejected):
            ledger.cancel_reservation("REQ-1", operation_id="C-3")
        ledger.close()

    def test_cancel_idempotent_and_content_conflict(self) -> None:
        ledger = simple_ledger()
        ledger.reserve_sample("REQ-1", "A", 4)
        first = ledger.cancel_reservation(
            "REQ-1", amount=1, operation_id="C-1", reason="r"
        )
        second = ledger.cancel_reservation(
            "REQ-1", amount=1, operation_id="C-1", reason="r"
        )
        self.assertEqual("accepted", first["status"])
        self.assertEqual("duplicate_noop", second["status"])
        self.assertEqual("3", ledger.get_reservation("REQ-1")["remaining_quantity"])
        with self.assertRaises(ConflictRejected):
            ledger.cancel_reservation("REQ-1", amount=2, operation_id="C-1")
        ledger.close()

    def test_fulfill_debits_reservation_and_physical_stock_together(self) -> None:
        ledger = simple_ledger()
        ledger.reserve_sample("REQ-1", "A", 4)
        result = ledger.fulfill_reservation(
            "REQ-1", 5, "1/2", operation_id="F-1"
        )
        self.assertEqual("accepted", result["status"])
        self.assertEqual("FULFILL-F-1", result["withdrawal_event"]["event_id"])
        self.assertEqual("valid", result["withdrawal_event"]["derivation"]["status"])
        # Physical stock drops through a normal discard event.
        self.assertEqual("19/2", ledger.sample("A")["available_quantity"])
        # Reservation remaining drops by the same amount; original amount kept.
        reservation = ledger.get_reservation("REQ-1")
        self.assertEqual("4", reservation["amount_quantity"])
        self.assertEqual("7/2", reservation["remaining_quantity"])
        self.assertEqual("active", reservation["status"])

        rest = ledger.fulfill_reservation("REQ-1", 6, "7/2", operation_id="F-2")
        self.assertEqual("fulfilled", rest["reservation"]["status"])
        self.assertEqual("0", rest["reservation"]["remaining_quantity"])
        self.assertEqual("6", ledger.sample("A")["available_quantity"])
        ledger.close()

    def test_fulfill_is_bounded_by_reservation_remaining(self) -> None:
        ledger = simple_ledger()
        ledger.reserve_sample("REQ-1", "A", 2)
        with self.assertRaises(ConflictRejected):
            ledger.fulfill_reservation("REQ-1", 5, 3, operation_id="F-1")
        ledger.fulfill_reservation("REQ-1", 5, 2, operation_id="F-1")
        with self.assertRaises(ConflictRejected):
            ledger.fulfill_reservation("REQ-1", 6, 1, operation_id="F-2")
        # No stock left the books beyond the fulfilled amount.
        self.assertEqual("8", ledger.sample("A")["available_quantity"])
        ledger.close()

    def test_fulfill_rejects_when_not_releasable(self) -> None:
        ledger = mixed_ledger()
        ledger.reserve_sample("REQ-MIX", "MIX", 3)
        ledger.quarantine_source("Q-A", 10, "A")
        with self.assertRaises(ConflictRejected):
            ledger.fulfill_reservation("REQ-MIX", 11, 1, operation_id="F-1")
        # Nothing was withdrawn and the reservation is untouched.
        self.assertEqual("6", ledger.sample("MIX")["available_quantity"])
        self.assertEqual(
            "3", ledger.get_reservation("REQ-MIX")["remaining_quantity"]
        )
        ledger.close()

    def test_fulfill_idempotent_and_content_conflict(self) -> None:
        ledger = simple_ledger()
        ledger.reserve_sample("REQ-1", "A", 4)
        kwargs = dict(seq=5, amount=1, operation_id="F-1")
        first = ledger.fulfill_reservation("REQ-1", **kwargs)
        second = ledger.fulfill_reservation("REQ-1", **kwargs)
        self.assertEqual("accepted", first["status"])
        self.assertEqual("duplicate_noop", second["status"])
        self.assertEqual("9", ledger.sample("A")["available_quantity"])
        self.assertEqual("3", ledger.get_reservation("REQ-1")["remaining_quantity"])
        with self.assertRaises(ConflictRejected):
            ledger.fulfill_reservation("REQ-1", 5, 2, operation_id="F-1")
        with self.assertRaises(ConflictRejected):
            ledger.fulfill_reservation("REQ-1", 6, 1, operation_id="F-1")
        ledger.close()

    def test_fulfill_is_atomic_with_its_withdrawal_event(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "atomic.sqlite3")
            ledger = Ledger.open(path)
            ledger.submit_event(
                {
                    "event_id": "O-A",
                    "seq": 1,
                    "rev": 1,
                    "type": "original",
                    "sample_id": "A",
                    "base_amount": 10,
                }
            )
            ledger.reserve_sample("REQ-1", "A", 4)
            with patch(
                "lineage_ledger.ledger.recompute_closure",
                side_effect=RuntimeError("simulated crash"),
            ):
                with self.assertRaises(RuntimeError):
                    ledger.fulfill_reservation("REQ-1", 2, 2, operation_id="F-1")
            ledger.close()

            reopened = Ledger.open(path)
            self.assertEqual(["O-A"], [e["event_id"] for e in reopened.list_events()])
            self.assertEqual([], reopened.reservation_operations())
            self.assertEqual(
                "4", reopened.get_reservation("REQ-1")["remaining_quantity"]
            )
            self.assertEqual("10", reopened.sample("A")["available_quantity"])
            reopened.close()


class ShortfallReportingTests(unittest.TestCase):
    def test_later_actual_withdrawal_keeps_reservations_and_reports_gap(self) -> None:
        ledger = simple_ledger()
        ledger.reserve_sample("R1", "A", 6)
        ledger.reserve_sample("R2", "A", 3)
        # External actual use outside the reservation workflow.
        ledger.submit_event(
            {
                "event_id": "D-1",
                "seq": 5,
                "rev": 1,
                "type": "discard",
                "sample": "A",
                "amount": 4,
            }
        )
        report = ledger.reservation_coverage_report("A")[0]
        self.assertFalse(report["fully_covered"])
        self.assertEqual("3", report["uncovered_total"])
        # FIFO: oldest reservation stays fully covered, newest loses 3.
        by_id = {r["request_id"]: r for r in report["reservations"]}
        self.assertEqual("0", by_id["R1"]["uncovered_quantity"])
        self.assertEqual("3", by_id["R2"]["uncovered_quantity"])
        self.assertEqual(["R2"], [r["request_id"] for r in report["affected_reservations"]])
        # Original reservation amounts are preserved, not rewritten.
        self.assertEqual("6", ledger.get_reservation("R1")["remaining_quantity"])
        self.assertEqual("3", ledger.get_reservation("R2")["remaining_quantity"])
        self.assertEqual("-3", report["bookable_quantity"])
        ledger.close()

    def test_quarantine_shortfall_is_reported_with_reason(self) -> None:
        ledger = mixed_ledger()
        ledger.reserve_sample("R-MIX", "MIX", 3)
        ledger.quarantine_source("Q-A", 10, "A")
        report = ledger.reservation_coverage_report("MIX")[0]
        self.assertEqual("0", report["releasable_quantity"])
        self.assertEqual("3", report["uncovered_total"])
        self.assertEqual(
            ["quarantined_source"], report["shortfall_reasons"]
        )
        self.assertEqual(
            ["R-MIX"],
            [r["request_id"] for r in report["affected_reservations"]],
        )
        # Releasing the source restores coverage without touching reservations.
        ledger.release_source("REL-A", 11, "A")
        restored = ledger.reservation_coverage_report("MIX")[0]
        self.assertTrue(restored["fully_covered"])
        self.assertEqual("0", restored["uncovered_total"])
        self.assertEqual(
            "3", ledger.get_reservation("R-MIX")["remaining_quantity"]
        )
        ledger.close()

    def test_coverage_report_covers_all_samples_with_open_reservations(self) -> None:
        ledger = mixed_ledger()
        ledger.reserve_sample("R-A", "A", 2)
        ledger.reserve_sample("R-B", "B", 1)
        rows = ledger.reservation_coverage_report()
        self.assertEqual({"A", "B"}, {r["sample_id"] for r in rows})
        self.assertTrue(all(r["fully_covered"] for r in rows))
        ledger.close()

    def test_cancelled_and_fulfilled_reservations_leave_coverage(self) -> None:
        ledger = simple_ledger()
        ledger.reserve_sample("R1", "A", 3)
        ledger.cancel_reservation("R1", operation_id="C-1")
        report = ledger.reservation_coverage_report()
        self.assertEqual([], report)
        ledger.reserve_sample("R2", "A", 2)
        ledger.fulfill_reservation("R2", 5, 2, operation_id="F-1")
        self.assertEqual([], ledger.reservation_coverage_report())
        ledger.close()


class PersistenceAndConventionTests(unittest.TestCase):
    def test_concurrent_reservations_never_overshoot_releasable_stock(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "concurrent.sqlite3")
            setup = Ledger.open(path)
            setup.submit_event(
                {
                    "event_id": "O-A",
                    "seq": 1,
                    "rev": 1,
                    "type": "original",
                    "sample_id": "A",
                    "base_amount": 10,
                }
            )
            setup.close()

            outcomes: list[tuple[str, int]] = []
            lock = threading.Lock()

            def worker(index: int) -> None:
                ledger = Ledger.open(path)
                try:
                    ledger.reserve_sample(f"R{index}", "A", 3)
                    outcome = "accepted"
                except ConflictRejected:
                    outcome = "rejected"
                finally:
                    ledger.close()
                with lock:
                    outcomes.append((outcome, index))

            threads = [threading.Thread(target=worker, args=(i,)) for i in range(6)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

            accepted = [i for outcome, i in outcomes if outcome == "accepted"]
            rejected = [i for outcome, i in outcomes if outcome == "rejected"]
            self.assertEqual(3, len(accepted))
            self.assertEqual(3, len(rejected))

            check = Ledger.open(path)
            capacity = check.reservable_quantity("A")
            self.assertEqual("9", capacity["reserved_quantity"])
            self.assertEqual("1", capacity["bookable_quantity"])
            # Reservation contention never touches physical inventory.
            self.assertEqual("10", check.sample("A")["available_quantity"])
            check.close()

    def test_state_persists_across_reopen(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "ledger.sqlite3")
            ledger = Ledger.open(path)
            ledger.submit_event(
                {
                    "event_id": "O-A",
                    "seq": 1,
                    "rev": 1,
                    "type": "original",
                    "sample_id": "A",
                    "base_amount": 10,
                }
            )
            ledger.reserve_sample("R1", "A", "1/3", metadata={"plan": "p1"})
            ledger.fulfill_reservation("R1", 2, "1/6", operation_id="F-1")
            ledger.close()

            reopened = Ledger.open(path)
            reservation = reopened.get_reservation("R1")
            self.assertEqual("1/3", reservation["amount_quantity"])
            self.assertEqual("1/6", reservation["remaining_quantity"])
            self.assertEqual({"plan": "p1"}, reservation["metadata"])
            self.assertEqual(1, len(reservation["operations"]))
            self.assertEqual(
                "59/6", reopened.sample("A")["available_quantity"]
            )
            reopened.close()

    def test_existing_database_file_is_compatible(self) -> None:
        # The pre-feature generated database must open with the new schema.
        fixture = ROOT / "generated_example.sqlite3"
        if not fixture.exists():
            self.skipTest("generated fixture not present")
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "copy.sqlite3")
            import shutil

            shutil.copy(fixture, path)
            ledger = Ledger.open(path)
            events_before = ledger.list_events()
            self.assertTrue(events_before)
            # New tables exist and old queries still work.
            self.assertEqual([], ledger.list_reservations())
            ledger.close()

    def test_validation_error_types(self) -> None:
        ledger = simple_ledger()
        with self.assertRaises(ValidationRejected):
            ledger.reserve_sample("", "A", 1)
        with self.assertRaises(ValidationRejected):
            ledger.reserve_sample("R", "A", 1.5)  # raw float forbidden
        with self.assertRaises(ValidationRejected):
            ledger.fulfill_reservation("R", -1, 1, operation_id="F")
        with self.assertRaises(KeyError):
            ledger.get_reservation("MISSING")
        with self.assertRaises(KeyError):
            ledger.cancel_reservation("MISSING", operation_id="C")
        with self.assertRaises(KeyError):
            ledger.fulfill_reservation("MISSING", 1, 1, operation_id="F")
        ledger.close()

    def test_listing_and_operations_history(self) -> None:
        ledger = simple_ledger()
        ledger.reserve_sample("R1", "A", 4)
        ledger.reserve_sample("R2", "A", 2)
        ledger.fulfill_reservation("R1", 5, 1, operation_id="F-1")
        ledger.cancel_reservation("R2", operation_id="C-1")
        active = ledger.list_reservations(status="active")
        self.assertEqual(["R1"], [r["request_id"] for r in active])
        cancelled = ledger.list_reservations(status="cancelled")
        self.assertEqual(["R2"], [r["request_id"] for r in cancelled])
        sample_filter = ledger.list_reservations(sample_id="A")
        self.assertEqual(2, len(sample_filter))
        ops = ledger.reservation_operations("R1")
        self.assertEqual(["F-1"], [o["operation_id"] for o in ops])
        self.assertEqual("fulfill", ops[0]["op_type"])
        ledger.close()


if __name__ == "__main__":
    unittest.main()
