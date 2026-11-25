from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lineage_ledger import Ledger, ValidationRejected
from lineage_ledger.audit import sample_diff_between_writes
from lineage_ledger.cli import main as cli_main
from lineage_ledger.db import connect_readonly

MAIN_TABLES = (
    "unit_conversion",
    "event_head",
    "event_version",
    "event_sample_ref",
    "event_dependency",
    "sample_identity_claim",
    "event_derivation",
    "effect",
    "effect_source",
    "event_upstream",
    "quarantine_record",
    "citation",
    "citation_explanation",
    "ledger_write",
    "reservation",
    "reservation_operation",
)


def basic_ledger() -> tuple[Ledger, dict[str, int]]:
    """A(3) + B(5) merged into MIX, then split into C(1)."""
    ledger = Ledger.open(":memory:")
    writes = {}
    writes["O-A"] = ledger.submit_event(
        {
            "event_id": "O-A",
            "seq": 1,
            "rev": 1,
            "type": "original",
            "sample_id": "A",
            "base_amount": 3,
        }
    )["write_id"]
    writes["O-B"] = ledger.submit_event(
        {
            "event_id": "O-B",
            "seq": 2,
            "rev": 1,
            "type": "original",
            "sample_id": "B",
            "base_amount": 5,
        }
    )["write_id"]
    writes["M-1"] = ledger.submit_event(
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
    )["write_id"]
    writes["S-1"] = ledger.submit_event(
        {
            "event_id": "S-1",
            "seq": 4,
            "rev": 1,
            "type": "split",
            "source": "MIX",
            "outputs": [{"sample_id": "C", "amount": 1}],
        }
    )["write_id"]
    return ledger, writes


def segment_spans(diff: dict) -> list[tuple[int, int]]:
    return [(s["from_seq"], s["to_seq"]) for s in diff["segments"]]


class AuditDiffTests(unittest.TestCase):
    def test_revision_changes_conclusion_segments(self) -> None:
        ledger, w = basic_ledger()
        w2 = ledger.submit_event(
            {
                "event_id": "M-1",
                "seq": 3,
                "rev": 2,
                "type": "merge",
                "sources": [
                    {"sample_id": "A", "amount": 2},
                    {"sample_id": "B", "amount": 1},
                ],
                "output": "MIX",
            }
        )["write_id"]
        diff = ledger.audit_sample_diff("C", w["S-1"], w2, 0, 10)
        self.assertTrue(diff["changed"])
        self.assertEqual([(4, 10)], segment_spans(diff))
        seg = diff["segments"][0]
        self.assertEqual("available", seg["before"]["status"])
        self.assertEqual("available", seg["after"]["status"])
        self.assertEqual("1", seg["before"]["available_quantity"])
        self.assertEqual("1", seg["after"]["available_quantity"])
        self.assertEqual(
            {"A": "1/3", "B": "2/3"}, seg["before"]["precise_contributions"]
        )
        self.assertEqual(
            {"A": "2/3", "B": "1/3"}, seg["after"]["precise_contributions"]
        )
        ledger.close()

    def test_late_predecessor_unblocks_lineage(self) -> None:
        ledger = Ledger.open(":memory:")
        w1 = ledger.submit_event(
            {
                "event_id": "LATE-USE",
                "seq": 20,
                "rev": 1,
                "type": "split",
                "source": "LATE-ROOT",
                "outputs": [{"sample_id": "LATE-CHILD", "amount": 4}],
            }
        )["write_id"]
        w2 = ledger.submit_event(
            {
                "event_id": "LATE-ORIGIN",
                "seq": 10,
                "rev": 1,
                "type": "original",
                "sample_id": "LATE-ROOT",
                "base_amount": 10,
            }
        )["write_id"]
        diff = ledger.audit_sample_diff("LATE-CHILD", w1, w2, 0, 30)
        self.assertEqual([(20, 30)], segment_spans(diff))
        seg = diff["segments"][0]
        self.assertEqual("blocked", seg["before"]["status"])
        self.assertEqual("0", seg["before"]["available_quantity"])
        self.assertEqual("0", seg["before"]["releasable_quantity"])
        self.assertEqual("available", seg["after"]["status"])
        self.assertEqual("4", seg["after"]["available_quantity"])
        self.assertEqual({"LATE-ROOT": "4"}, seg["after"]["precise_contributions"])
        self.assertEqual("4", seg["after"]["releasable_quantity"])
        ledger.close()

    def test_quarantine_changes_releasability_only(self) -> None:
        ledger, w = basic_ledger()
        w2 = ledger.submit_event(
            {
                "event_id": "Q-A",
                "seq": 5,
                "rev": 1,
                "type": "quarantine",
                "source_id": "A",
                "reason": "污染风险",
            }
        )["write_id"]
        diff = ledger.audit_sample_diff("C", w["S-1"], w2, 0, 10)
        self.assertEqual([(5, 10)], segment_spans(diff))
        seg = diff["segments"][0]
        # Physical inventory is unchanged; only the release judgement differs.
        self.assertEqual("1", seg["before"]["available_quantity"])
        self.assertEqual("1", seg["after"]["available_quantity"])
        self.assertEqual("1", seg["before"]["releasable_quantity"])
        self.assertEqual([], seg["before"]["quarantined_source_ids"])
        self.assertEqual("0", seg["after"]["releasable_quantity"])
        self.assertEqual(["A"], seg["after"]["quarantined_source_ids"])
        ledger.close()

    def test_unrelated_write_produces_empty_diff(self) -> None:
        ledger, w = basic_ledger()
        w2 = ledger.submit_event(
            {
                "event_id": "OTHER-ROOT",
                "seq": 50,
                "rev": 1,
                "type": "original",
                "sample_id": "OTHER",
                "base_amount": 1,
            }
        )["write_id"]
        diff = ledger.audit_sample_diff("C", w["S-1"], w2, 0, 100)
        self.assertFalse(diff["changed"])
        self.assertEqual([], diff["segments"])
        ledger.close()

    def test_equal_cutoffs_produce_empty_diff(self) -> None:
        ledger, w = basic_ledger()
        diff = ledger.audit_sample_diff("C", w["M-1"], w["M-1"], 0, 10)
        self.assertEqual([], diff["segments"])
        ledger.close()

    def test_zero_cutoff_means_empty_ledger(self) -> None:
        ledger, w = basic_ledger()
        diff = ledger.audit_sample_diff("C", 0, w["S-1"], 0, 10)
        self.assertEqual([(4, 10)], segment_spans(diff))
        seg = diff["segments"][0]
        self.assertEqual("not_formed", seg["before"]["status"])
        self.assertEqual("0", seg["before"]["available_quantity"])
        self.assertEqual("available", seg["after"]["status"])
        self.assertEqual("1", seg["after"]["available_quantity"])
        ledger.close()

    def test_invalid_inputs_raise(self) -> None:
        ledger, w = basic_ledger()
        # Cutoffs out of order.
        with self.assertRaises(ValidationRejected):
            ledger.audit_sample_diff("C", w["S-1"], w["M-1"], 0, 10)
        # Cutoff does not exist.
        with self.assertRaises(ValidationRejected):
            ledger.audit_sample_diff("C", 0, 10 ** 9, 0, 10)
        with self.assertRaises(ValidationRejected):
            ledger.audit_sample_diff("C", 10 ** 9, 10 ** 9, 0, 10)
        # Business range must be an increasing non-negative integer range.
        with self.assertRaises(ValidationRejected):
            ledger.audit_sample_diff("C", 0, w["S-1"], 10, 10)
        with self.assertRaises(ValidationRejected):
            ledger.audit_sample_diff("C", 0, w["S-1"], 9, 3)
        with self.assertRaises(ValidationRejected):
            ledger.audit_sample_diff("C", 0, w["S-1"], -1, 3)
        with self.assertRaises(ValidationRejected):
            ledger.audit_sample_diff("C", 0, w["S-1"], 0.5, 3)
        with self.assertRaises(ValidationRejected):
            ledger.audit_sample_diff("C", 0, w["S-1"], True, 3)
        with self.assertRaises(ValidationRejected):
            ledger.audit_sample_diff("C", -1, w["S-1"], 0, 3)
        ledger.close()

    def test_interval_clips_segments(self) -> None:
        ledger, w = basic_ledger()
        w2 = ledger.submit_event(
            {
                "event_id": "Q-A",
                "seq": 5,
                "rev": 1,
                "type": "quarantine",
                "source_id": "A",
            }
        )["write_id"]
        # The quarantine takes effect at seq 5, outside [0, 5).
        diff = ledger.audit_sample_diff("C", w["S-1"], w2, 0, 5)
        self.assertEqual([], diff["segments"])
        # A narrow interval inside the changed region is reported as-is.
        diff = ledger.audit_sample_diff("C", w["S-1"], w2, 6, 8)
        self.assertEqual([(6, 8)], segment_spans(diff))
        seg = diff["segments"][0]
        self.assertEqual("1", seg["before"]["releasable_quantity"])
        self.assertEqual("0", seg["after"]["releasable_quantity"])
        ledger.close()

    def test_adjacent_segments_with_equal_sides_merge(self) -> None:
        ledger = Ledger.open(":memory:")
        ledger.submit_event(
            {
                "event_id": "O-Z",
                "seq": 1,
                "rev": 1,
                "type": "original",
                "sample_id": "Z",
                "base_amount": 10,
            }
        )
        ledger.submit_event(
            {
                "event_id": "O-A",
                "seq": 2,
                "rev": 1,
                "type": "original",
                "sample_id": "A",
                "base_amount": 3,
            }
        )
        ledger.submit_event(
            {
                "event_id": "O-B",
                "seq": 3,
                "rev": 1,
                "type": "original",
                "sample_id": "B",
                "base_amount": 5,
            }
        )
        ledger.submit_event(
            {
                "event_id": "M-1",
                "seq": 4,
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
                "seq": 5,
                "rev": 1,
                "type": "split",
                "source": "MIX",
                "outputs": [{"sample_id": "C", "amount": 1}],
            }
        )
        w1 = ledger.submit_event(
            {
                "event_id": "T-1",
                "seq": 12,
                "rev": 1,
                "type": "transfer",
                "source": "Z",
                "destination": "C",
                "amount": 4,
            }
        )["write_id"]
        # Between the cutoffs: Z is quarantined at seq 8 (before its material
        # reaches C at seq 12, so seq 8 is a no-op boundary for C) and the
        # split is revised to output 2 units instead of 1.
        ledger.submit_event(
            {
                "event_id": "Q-Z",
                "seq": 8,
                "rev": 1,
                "type": "quarantine",
                "source_id": "Z",
            }
        )
        w2 = ledger.submit_event(
            {
                "event_id": "S-1",
                "seq": 5,
                "rev": 2,
                "type": "split",
                "source": "MIX",
                "outputs": [{"sample_id": "C", "amount": 2}],
            }
        )["write_id"]
        diff = ledger.audit_sample_diff("C", w1, w2, 0, 20)
        # [5,8) and [8,12) differ identically on both sides, so the no-op
        # boundary at seq 8 is merged away.
        self.assertEqual([(5, 12), (12, 20)], segment_spans(diff))
        first, second = diff["segments"]
        self.assertEqual("1", first["before"]["available_quantity"])
        self.assertEqual("1", first["before"]["releasable_quantity"])
        self.assertEqual("2", first["after"]["available_quantity"])
        self.assertEqual("2", first["after"]["releasable_quantity"])
        self.assertEqual([], first["after"]["quarantined_source_ids"])
        self.assertEqual("5", second["before"]["available_quantity"])
        self.assertEqual("5", second["before"]["releasable_quantity"])
        self.assertEqual("6", second["after"]["available_quantity"])
        self.assertEqual("0", second["after"]["releasable_quantity"])
        self.assertEqual(["Z"], second["after"]["quarantined_source_ids"])
        self.assertEqual(
            {"A": "2/3", "B": "4/3", "Z": "4"},
            second["after"]["precise_contributions"],
        )
        ledger.close()

    def test_superseded_claim_still_blocks_original(self) -> None:
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
        w3 = ledger.submit_event(
            {
                "event_id": "D-SPLIT",
                "seq": 2,
                "rev": 2,
                "type": "split",
                "source": "D-A",
                "outputs": [{"sample_id": "D-OTHER", "amount": 1}],
            }
        )["write_id"]
        w4 = ledger.submit_event(
            {
                "event_id": "D-FAKE",
                "seq": 3,
                "rev": 1,
                "type": "original",
                "sample_id": "D-CHILD",
                "base_amount": 1,
            }
        )["write_id"]
        # The derived-output claim registered by the superseded rev1 of
        # D-SPLIT still blocks re-rooting D-CHILD as an original.
        self.assertEqual("blocked", ledger._derivation("D-FAKE")["status"])
        diff = ledger.audit_sample_diff("D-CHILD", w3, w4, 0, 10)
        self.assertEqual([(3, 10)], segment_spans(diff))
        seg = diff["segments"][0]
        self.assertEqual("not_formed", seg["before"]["status"])
        self.assertEqual("blocked", seg["after"]["status"])
        ledger.close()

    def test_reservation_invisible_but_fulfillment_counts(self) -> None:
        ledger = Ledger.open(":memory:")
        w1 = ledger.submit_event(
            {
                "event_id": "RO-Z",
                "seq": 1,
                "rev": 1,
                "type": "original",
                "sample_id": "Z",
                "base_amount": 10,
            }
        )["write_id"]
        ledger.reserve_sample("RES-1", "Z", 4)
        w_res = ledger.conn.execute(
            "SELECT MAX(write_id) AS w FROM ledger_write"
        ).fetchone()["w"]
        # A reservation is a pure capacity hold: no conclusion changes.
        diff = ledger.audit_sample_diff("Z", w1, w_res, 0, 10)
        self.assertEqual([], diff["segments"])
        # Fulfillment registers a normal discard withdrawal event.
        ledger.fulfill_reservation("RES-1", 2, 1, operation_id="FUL-1")
        w_ful = ledger.conn.execute(
            "SELECT MAX(write_id) AS w FROM ledger_write"
        ).fetchone()["w"]
        diff = ledger.audit_sample_diff("Z", w_res, w_ful, 0, 10)
        self.assertEqual([(2, 10)], segment_spans(diff))
        seg = diff["segments"][0]
        self.assertEqual("10", seg["before"]["available_quantity"])
        self.assertEqual("9", seg["after"]["available_quantity"])
        ledger.close()

    def test_after_conclusions_match_live_sample_report(self) -> None:
        ledger, w = basic_ledger()
        ledger.submit_event(
            {
                "event_id": "Q-A",
                "seq": 5,
                "rev": 1,
                "type": "quarantine",
                "source_id": "A",
            }
        )
        ledger.submit_event(
            {
                "event_id": "R-A",
                "seq": 6,
                "rev": 1,
                "type": "release",
                "source_id": "A",
            }
        )
        ledger.submit_event(
            {
                "event_id": "D-C",
                "seq": 7,
                "rev": 1,
                "type": "discard",
                "sample": "C",
                "amount": "1/2",
            }
        )
        w_max = ledger.conn.execute(
            "SELECT MAX(write_id) AS w FROM ledger_write"
        ).fetchone()["w"]
        # Diffing the empty ledger against the latest write exposes the whole
        # current conclusion function as the "after" side; every segment must
        # match the live sample report at the same business point.
        diff = ledger.audit_sample_diff("C", 0, w_max, 0, 12)
        self.assertTrue(diff["segments"])
        for seg in diff["segments"]:
            report = ledger.sample("C", at_seq=seg["from_seq"])
            after = seg["after"]
            self.assertEqual(report["status"], after["status"])
            self.assertEqual(report["available_quantity"], after["available_quantity"])
            self.assertEqual(
                {
                    x["original_id"]: x["amount"]
                    for x in report["precise_contributions"]
                },
                after["precise_contributions"],
            )
            self.assertEqual(
                report["releasable_quantity"], after["releasable_quantity"]
            )
            self.assertEqual(
                report["quarantined_source_ids"], after["quarantined_source_ids"]
            )
        ledger.close()

    def test_large_unrelated_ledger_and_wide_interval(self) -> None:
        ledger = Ledger.open(":memory:")
        for i in range(300):
            ledger.submit_event(
                {
                    "event_id": f"U-ROOT-{i}",
                    "seq": 1000 + i,
                    "rev": 1,
                    "type": "original",
                    "sample_id": f"U{i}",
                    "base_amount": 1,
                }
            )
        w1 = ledger.submit_event(
            {
                "event_id": "T-ROOT",
                "seq": 5000,
                "rev": 1,
                "type": "original",
                "sample_id": "T",
                "base_amount": 10,
            }
        )["write_id"]
        w2 = ledger.submit_event(
            {
                "event_id": "T-USE",
                "seq": 6000,
                "rev": 1,
                "type": "split",
                "source": "T",
                "outputs": [{"sample_id": "T-CHILD", "amount": 3}],
            }
        )["write_id"]
        # A per-business-point enumeration over this interval would hang.
        diff = ledger.audit_sample_diff("T-CHILD", w1, w2, 0, 10 ** 9)
        self.assertEqual([(6000, 10 ** 9)], segment_spans(diff))
        seg = diff["segments"][0]
        self.assertEqual("not_formed", seg["before"]["status"])
        self.assertEqual("available", seg["after"]["status"])
        self.assertEqual("3", seg["after"]["available_quantity"])
        ledger.close()

    def test_query_does_not_modify_database(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "audit.sqlite3")
            ledger = Ledger.open(path)
            w1 = ledger.submit_event(
                {
                    "event_id": "O-A",
                    "seq": 1,
                    "rev": 1,
                    "type": "original",
                    "sample_id": "A",
                    "base_amount": 3,
                }
            )["write_id"]
            w2 = ledger.submit_event(
                {
                    "event_id": "S-1",
                    "seq": 2,
                    "rev": 1,
                    "type": "split",
                    "source": "A",
                    "outputs": [{"sample_id": "C", "amount": 1}],
                }
            )["write_id"]

            def snapshot() -> dict[str, str]:
                state = {}
                for table in MAIN_TABLES:
                    rows = ledger.conn.execute(
                        f"SELECT * FROM {table}"
                    ).fetchall()
                    state[table] = json.dumps(
                        [dict(r) for r in rows], sort_keys=True, default=str
                    )
                return state

            before = snapshot()
            diff = ledger.audit_sample_diff("C", w1, w2, 0, 10)
            self.assertEqual([(2, 10)], segment_spans(diff))
            self.assertEqual(before, snapshot())
            # The connection is left usable for further writes.
            ledger.submit_event(
                {
                    "event_id": "O-X",
                    "seq": 3,
                    "rev": 1,
                    "type": "original",
                    "sample_id": "X",
                    "base_amount": 1,
                }
            )
            ledger.close()

            # A strictly read-only connection suffices and yields the same diff.
            conn = connect_readonly(path)
            try:
                ro_diff = sample_diff_between_writes(conn, "C", w1, w2, 0, 10)
            finally:
                conn.close()
            self.assertEqual(diff, ro_diff)

            reopened = Ledger.open(path)
            self.assertEqual(3, len(reopened.list_events()))
            reopened.close()

    def test_cli_audit_diff(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "cli.sqlite3")
            ledger = Ledger.open(path)
            w1 = ledger.submit_event(
                {
                    "event_id": "O-A",
                    "seq": 1,
                    "rev": 1,
                    "type": "original",
                    "sample_id": "A",
                    "base_amount": 3,
                }
            )["write_id"]
            w2 = ledger.submit_event(
                {
                    "event_id": "S-1",
                    "seq": 2,
                    "rev": 1,
                    "type": "split",
                    "source": "A",
                    "outputs": [{"sample_id": "C", "amount": 1}],
                }
            )["write_id"]
            ledger.close()

            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = cli_main(["audit-diff", path, "C", str(w1), str(w2), "0", "10"])
            self.assertEqual(0, rc)
            payload = json.loads(out.getvalue())
            self.assertEqual("C", payload["sample_id"])
            self.assertEqual(w1, payload["from_write_id"])
            self.assertEqual(w2, payload["to_write_id"])
            self.assertEqual(0, payload["seq_start"])
            self.assertEqual(10, payload["seq_end"])
            self.assertTrue(payload["changed"])
            self.assertEqual([(2, 10)], segment_spans(payload))

            # Invalid cutoffs exit non-zero.
            with contextlib.redirect_stderr(io.StringIO()):
                rc = cli_main(["audit-diff", path, "C", str(w2), str(w1), "0", "10"])
            self.assertEqual(1, rc)
            with contextlib.redirect_stderr(io.StringIO()):
                rc = cli_main(["audit-diff", path, "C", "0", "99999", "0", "10"])
            self.assertEqual(1, rc)
            with contextlib.redirect_stderr(io.StringIO()):
                rc = cli_main(["audit-diff", path, "C", "0", str(w2), "5", "5"])
            self.assertEqual(1, rc)


if __name__ == "__main__":
    unittest.main()
