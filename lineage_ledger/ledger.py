"""Persistent exact lineage ledger facade."""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from .citations import append_explanation
from .db import connect
from .engine import (
    affected_closure,
    now_iso,
    recompute_closure,
    sample_report,
)
from .errors import ConflictRejected, PersistenceClosed, ValidationRejected
from .normalize import content_hash, normalize_event, output_claims_from_event, refs_from_event
from .rational_utils import parse_fraction, positive_fraction, q_to_db, q_to_str


class Ledger:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self._closed = False
        self.conn.execute(
            """
            INSERT OR IGNORE INTO unit_conversion(
                unit, factor_num, factor_den, note, created_at
            ) VALUES ('BASE', '1', '1', 'base unit', ?)
            """,
            (now_iso(),),
        )
        self.conn.commit()

    @classmethod
    def open(cls, path: str | Path = ":memory:") -> "Ledger":
        return cls(connect(path))

    def close(self) -> None:
        if not self._closed:
            self.conn.close()
            self._closed = True

    def __enter__(self) -> "Ledger":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _check_open(self) -> None:
        if self._closed:
            raise PersistenceClosed("ledger is closed")

    # ------------------------------------------------------------------ units
    def register_unit(
        self, unit: str, factor: Any, note: str = ""
    ) -> dict[str, Any]:
        self._check_open()
        if not isinstance(unit, str) or not unit.strip():
            raise ValidationRejected("unit must be a non-empty string")
        unit = unit.strip()
        ratio = parse_fraction(factor, "factor")
        if ratio <= 0:
            raise ValidationRejected("unit factor must be positive")
        row = self.conn.execute(
            "SELECT factor_num, factor_den FROM unit_conversion WHERE unit = ?",
            (unit,),
        ).fetchone()
        n, d = q_to_db(ratio)
        if row is not None:
            from fractions import Fraction

            existing = Fraction(int(row["factor_num"]), int(row["factor_den"]))
            if existing != ratio:
                raise ConflictRejected(
                    f"unit {unit!r} already has factor {q_to_str(existing)}, "
                    f"cannot replace with {q_to_str(ratio)}"
                )
            return {"status": "duplicate", "unit": unit, "factor": q_to_str(ratio)}
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO unit_conversion(
                    unit, factor_num, factor_den, note, created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (unit, n, d, note, now_iso()),
            )
        return {"status": "registered", "unit": unit, "factor": q_to_str(ratio)}

    # ------------------------------------------------------------------ events
    def submit_event(self, raw_event: dict[str, Any]) -> dict[str, Any]:
        self._check_open()
        if not isinstance(raw_event, dict):
            raise ValidationRejected("event must be an object")

        # Normalization also validates unit conversions and dependency ordering.
        event = normalize_event(self.conn, raw_event)
        event_id = event["event_id"]
        rev = int(event["rev"])
        seq = int(event["seq"])
        digest = content_hash(event)
        payload_json = json.dumps(event, ensure_ascii=False, sort_keys=True, default=str)
        raw_json = json.dumps(
            raw_event, ensure_ascii=False, sort_keys=True, default=str
        )

        version = self.conn.execute(
            "SELECT content_hash FROM event_version WHERE event_id = ? AND rev = ?",
            (event_id, rev),
        ).fetchone()
        if version is not None:
            if version["content_hash"] == digest:
                derivation = self._derivation(event_id)
                return {
                    "status": "duplicate_noop",
                    "event_id": event_id,
                    "rev": rev,
                    "seq": seq,
                    "content_hash": digest,
                    "derivation": derivation,
                    "restated_citations": [],
                }
            raise ConflictRejected(
                f"event {event_id!r} rev{rev} already exists with different content"
            )

        head = self.conn.execute(
            "SELECT current_rev, seq FROM event_head WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        old_rev: int | None = None
        if head is not None:
            old_rev = int(head["current_rev"])
            if rev <= old_rev:
                raise ConflictRejected(
                    f"event {event_id!r}: rev{rev} is not higher than current rev{old_rev}"
                )
            if int(head["seq"]) != seq:
                raise ValidationRejected(
                    f"event {event_id!r}: business seq is stable across revisions; "
                    f"expected {head['seq']}, got {seq}"
                )
        else:
            if rev != 1:
                raise ValidationRejected("a new event_id must start at rev=1")
            seq_row = self.conn.execute(
                "SELECT event_id FROM event_head WHERE seq = ?", (seq,)
            ).fetchone()
            if seq_row is not None:
                raise ConflictRejected(
                    f"business seq {seq} already used by event {seq_row['event_id']!r}"
                )

        restated: list[dict[str, Any]] = []
        with self.conn:
            committed_at = now_iso()
            self.conn.execute(
                """
                INSERT INTO event_version(
                    event_id, rev, seq, content_hash, payload_json, raw_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (event_id, rev, seq, digest, payload_json, raw_json, committed_at),
            )
            if head is None:
                self.conn.execute(
                    """
                    INSERT INTO event_head(event_id, current_rev, seq, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (event_id, rev, seq, committed_at, committed_at),
                )
            else:
                self.conn.execute(
                    "UPDATE event_head SET current_rev = ?, updated_at = ? WHERE event_id = ?",
                    (rev, committed_at, event_id),
                )

            for sample_id, role in refs_from_event(event):
                self.conn.execute(
                    """
                    INSERT OR IGNORE INTO event_sample_ref(
                        event_id, rev, seq, sample_id, role
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (event_id, rev, seq, sample_id, role),
                )

            for dep in event.get("dependencies", []):
                self.conn.execute(
                    """
                    INSERT OR REPLACE INTO event_dependency(
                        event_id, rev, depends_on_event_id, dep_seq
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (event_id, rev, dep["event_id"], dep["seq"]),
                )

            self.conn.execute(
                """
                INSERT OR IGNORE INTO sample_identity_claim(
                    sample_id, event_id, rev, seq, claim_kind
                ) VALUES (?, ?, ?, ?, 'original')
                """,
                (event["sample_id"], event_id, rev, seq),
            ) if event["type"] == "original" else None
            for sample_id in output_claims_from_event(event):
                self.conn.execute(
                    """
                    INSERT OR IGNORE INTO sample_identity_claim(
                        sample_id, event_id, rev, seq, claim_kind
                    ) VALUES (?, ?, ?, ?, 'derived_output')
                    """,
                    (sample_id, event_id, rev, seq),
                )

            write_cur = self.conn.execute(
                """
                INSERT INTO ledger_write(event_id, rev, operation, content_hash, committed_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    rev,
                    "revision" if old_rev is not None else "submit",
                    digest,
                    committed_at,
                ),
            )

            affected = affected_closure(self.conn, event_id, old_rev)
            recompute_closure(self.conn, affected)

            sample_rows = self.conn.execute(
                """
                SELECT DISTINCT r.sample_id
                FROM event_sample_ref r
                JOIN event_head h ON h.event_id = r.event_id AND h.current_rev = r.rev
                WHERE r.event_id IN (%s)
                """
                % ",".join("?" for _ in affected),
                tuple(affected),
            ).fetchall() if affected else []
            touched_samples = {r["sample_id"] for r in sample_rows}
            if old_rev is not None:
                old_rows = self.conn.execute(
                    "SELECT DISTINCT sample_id FROM event_sample_ref WHERE event_id = ? AND rev = ?",
                    (event_id, old_rev),
                ).fetchall()
                touched_samples.update(r["sample_id"] for r in old_rows)

            citation_ids: set[str] = set()
            if touched_samples:
                placeholders = ",".join("?" for _ in touched_samples)
                cites = self.conn.execute(
                    f"""
                    SELECT citation_id, at_seq
                    FROM citation
                    WHERE sample_id IN ({placeholders})
                      AND at_seq >= ?
                    """,
                    (*touched_samples, seq),
                ).fetchall()
                citation_ids.update(r["citation_id"] for r in cites)

            for citation_id in sorted(citation_ids):
                result = append_explanation(
                    self.conn, citation_id, event_id, rev
                )
                restated.append(
                    {
                        "citation_id": citation_id,
                        "version": result["version"],
                        "status": result["status"],
                    }
                )

            derivation = self._derivation(event_id)
            return {
                "status": "revision_accepted" if old_rev is not None else "accepted",
                "event_id": event_id,
                "rev": rev,
                "old_rev": old_rev,
                "seq": seq,
                "content_hash": digest,
                "write_id": write_cur.lastrowid,
                "affected_event_count": len(affected),
                "affected_event_ids": sorted(affected),
                "derivation": derivation,
                "restated_citations": restated,
            }

    def _derivation(self, event_id: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT * FROM event_derivation WHERE event_id = ?", (event_id,)
        ).fetchone()
        if row is None:
            return None
        result = dict(row)
        if result.get("reason_json"):
            result["reason"] = json.loads(result["reason_json"])
        return result

    # -------------------------------------------------------------- citations
    def add_citation(
        self,
        citation_id: str,
        sample_id: str,
        at_seq: int,
        claimed_amount: Any | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._check_open()
        if not isinstance(citation_id, str) or not citation_id.strip():
            raise ValidationRejected("citation_id must be a non-empty string")
        if not isinstance(sample_id, str) or not sample_id.strip():
            raise ValidationRejected("sample_id must be a non-empty string")
        if isinstance(at_seq, bool) or not isinstance(at_seq, int) or at_seq < 0:
            raise ValidationRejected("at_seq must be a non-negative integer")
        if metadata is not None and not isinstance(metadata, dict):
            raise ValidationRejected("metadata must be an object")

        existing = self.conn.execute(
            "SELECT 1 FROM citation WHERE citation_id = ?", (citation_id,)
        ).fetchone()
        if existing:
            raise ConflictRejected(f"citation {citation_id!r} already exists")

        claimed = None
        n = d = None
        if claimed_amount is not None:
            claimed = positive_fraction(claimed_amount, "claimed_amount")
            n, d = q_to_db(claimed)

        with self.conn:
            self.conn.execute(
                """
                INSERT INTO citation(
                    citation_id, sample_id, at_seq, claimed_num, claimed_den,
                    metadata_json, current_version, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, 0, ?)
                """,
                (
                    citation_id,
                    sample_id,
                    at_seq,
                    n,
                    d,
                    json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True),
                    now_iso(),
                ),
            )
            result = append_explanation(self.conn, citation_id)
        return result

    def get_citation(
        self, citation_id: str, version: int | None = None
    ) -> dict[str, Any]:
        self._check_open()
        if version is None:
            row = self.conn.execute(
                """
                SELECT e.* FROM citation c
                JOIN citation_explanation e
                  ON e.citation_id = c.citation_id AND e.version = c.current_version
                WHERE c.citation_id = ?
                """,
                (citation_id,),
            ).fetchone()
        else:
            row = self.conn.execute(
                "SELECT * FROM citation_explanation WHERE citation_id = ? AND version = ?",
                (citation_id, version),
            ).fetchone()
        if row is None:
            raise KeyError(citation_id)
        result = dict(row)
        result["report"] = json.loads(result.pop("report_json"))
        return result

    def list_citation_versions(self, citation_id: str) -> list[dict[str, Any]]:
        self._check_open()
        rows = self.conn.execute(
            """
            SELECT citation_id, version, trigger_event_id, trigger_rev,
                   status, traceable, as_of_seq, reason_code, created_at
            FROM citation_explanation
            WHERE citation_id = ?
            ORDER BY version
            """,
            (citation_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    # ---------------------------------------------------------------- queries
    def sample(self, sample_id: str, at_seq: int | None = None) -> dict[str, Any]:
        self._check_open()
        return sample_report(self.conn, sample_id, at_seq=at_seq)

    def get_event(self, event_id: str, rev: int | None = None) -> dict[str, Any]:
        self._check_open()
        if rev is None:
            row = self.conn.execute(
                """
                SELECT v.* FROM event_head h
                JOIN event_version v ON v.event_id = h.event_id AND v.rev = h.current_rev
                WHERE h.event_id = ?
                """,
                (event_id,),
            ).fetchone()
        else:
            row = self.conn.execute(
                "SELECT * FROM event_version WHERE event_id = ? AND rev = ?",
                (event_id, rev),
            ).fetchone()
        if row is None:
            raise KeyError(event_id)
        result = dict(row)
        result["payload"] = json.loads(result.pop("payload_json"))
        result["derivation"] = self._derivation(event_id)
        return result

    def list_events(self) -> list[dict[str, Any]]:
        self._check_open()
        rows = self.conn.execute(
            """
            SELECT h.event_id, h.current_rev AS rev, h.seq,
                   d.status, d.reason_code
            FROM event_head h
            LEFT JOIN event_derivation d ON d.event_id = h.event_id
            ORDER BY h.seq
            """
        ).fetchall()
        return [dict(r) for r in rows]
