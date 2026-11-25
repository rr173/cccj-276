"""Persistent exact lineage ledger facade."""
from __future__ import annotations

import contextlib
import json
import sqlite3
import uuid
from fractions import Fraction
from pathlib import Path
from typing import Any

from .audit import sample_diff_between_writes
from .citations import append_explanation
from .db import connect
from .engine import (
    affected_closure,
    effective_source_actions,
    now_iso,
    recompute_closure,
    sample_report,
    samples_referencing_source,
    source_action_records,
)
from .errors import ConflictRejected, PersistenceClosed, ValidationRejected
from .normalize import content_hash, normalize_event, output_claims_from_event, refs_from_event
from .rational_utils import parse_fraction, positive_fraction, q_to_db, q_to_str
from .reservations import (
    all_coverage,
    list_operations,
    list_reservations,
    operation_content_hash,
    releasable_capacity,
    reservation_content_hash,
    reservation_coverage,
    reserved_total,
)


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

    @contextlib.contextmanager
    def _immediate_tx(self):
        """Serialize mutating operations against other connections.

        A reservation reads current capacity and then writes a hold; taking a
        RESERVED (write) lock up front makes concurrent reservations on the
        same sample strictly serial, so their combined holds can never exceed
        the releasable stock. Contention surfaces as ``sqlite3.OperationalError``
        after the configured busy timeout rather than as a silent overbook.
        """
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield
        except Exception:
            self.conn.rollback()
            raise
        else:
            self.conn.commit()

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
        prepared = self._prepare_event(raw_event)
        with self.conn:
            return self._submit_event_in_tx(prepared)

    def _prepare_event(self, raw_event: dict[str, Any]) -> dict[str, Any]:
        """Normalize an event and run all version/head conflict checks.

        No row is written here. The returned descriptor can be applied inside
        an existing transaction via :meth:`_submit_event_in_tx`, which keeps
        composite operations (e.g. reservation fulfillment) atomic.
        """
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
        duplicate = False
        if version is not None:
            if version["content_hash"] == digest:
                duplicate = True
            else:
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
            if not duplicate and rev <= old_rev:
                raise ConflictRejected(
                    f"event {event_id!r}: rev{rev} is not higher than current rev{old_rev}"
                )
            if int(head["seq"]) != seq:
                raise ValidationRejected(
                    f"event {event_id!r}: business seq is stable across revisions; "
                    f"expected {head['seq']}, got {seq}"
                )
        elif not duplicate:
            if rev != 1:
                raise ValidationRejected("a new event_id must start at rev=1")
            seq_row = self.conn.execute(
                "SELECT event_id FROM event_head WHERE seq = ?", (seq,)
            ).fetchone()
            if seq_row is not None:
                raise ConflictRejected(
                    f"business seq {seq} already used by event {seq_row['event_id']!r}"
                )

        return {
            "event": event,
            "event_id": event_id,
            "rev": rev,
            "seq": seq,
            "digest": digest,
            "payload_json": payload_json,
            "raw_json": raw_json,
            "old_rev": old_rev,
            "duplicate": duplicate,
        }

    def _submit_event_in_tx(self, prepared: dict[str, Any]) -> dict[str, Any]:
        """Persist a prepared event. Must run inside an open transaction."""
        event = prepared["event"]
        event_id = prepared["event_id"]
        rev = prepared["rev"]
        seq = prepared["seq"]
        digest = prepared["digest"]
        old_rev = prepared["old_rev"]

        if prepared["duplicate"]:
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

        restated: list[dict[str, Any]] = []
        committed_at = now_iso()
        self.conn.execute(
            """
            INSERT INTO event_version(
                event_id, rev, seq, content_hash, payload_json, raw_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (event_id, rev, seq, digest, prepared["payload_json"],
             prepared["raw_json"], committed_at),
        )
        if old_rev is None:
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

        if event["type"] in {"quarantine", "release"}:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO quarantine_record(
                    event_id, rev, seq, source_id, action, reason
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    rev,
                    seq,
                    event["source_id"],
                    event["type"],
                    event["reason"],
                ),
            )

        if event["type"] == "original":
            self.conn.execute(
                """
                INSERT OR IGNORE INTO sample_identity_claim(
                    sample_id, event_id, rev, seq, claim_kind
                ) VALUES (?, ?, ?, ?, 'original')
                """,
                (event["sample_id"], event_id, rev, seq),
            )
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

        # Quarantine/release has no physical sample ref; the affected set is
        # every container whose current composition holds a positive amount
        # of the recorded source (checked against both the new and, on a
        # revision, the superseded source so a correction retracts it too).
        source_ids = set()
        for source_rev in {rev, old_rev} - {None}:
            source_row = self.conn.execute(
                """
                SELECT source_id FROM quarantine_record
                WHERE event_id = ? AND rev = ?
                """,
                (event_id, source_rev),
            ).fetchone()
            if source_row is not None:
                source_ids.add(source_row["source_id"])
        for source_id in source_ids:
            touched_samples |= samples_referencing_source(
                self.conn, source_id
            )

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
            "affected_samples": sorted(touched_samples),
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

    # ---------------------------------------------------------- quarantines
    def record_source_event(
        self,
        event_id: str,
        seq: int,
        source_id: str,
        action: str,
        *,
        rev: int = 1,
        reason: str = "",
        dependencies: list[Any] | None = None,
    ) -> dict[str, Any]:
        """Record a quarantine or release for an original source at a business seq."""
        if action not in {"quarantine", "release"}:
            raise ValidationRejected("action must be 'quarantine' or 'release'")
        event: dict[str, Any] = {
            "event_id": event_id,
            "seq": seq,
            "rev": rev,
            "type": action,
            "source_id": source_id,
            "reason": reason,
        }
        if dependencies is not None:
            event["dependencies"] = dependencies
        return self.submit_event(event)

    def quarantine_source(
        self,
        event_id: str,
        seq: int,
        source_id: str,
        *,
        rev: int = 1,
        reason: str = "",
        dependencies: list[Any] | None = None,
    ) -> dict[str, Any]:
        return self.record_source_event(
            event_id,
            seq,
            source_id,
            "quarantine",
            rev=rev,
            reason=reason,
            dependencies=dependencies,
        )

    def release_source(
        self,
        event_id: str,
        seq: int,
        source_id: str,
        *,
        rev: int = 1,
        reason: str = "",
        dependencies: list[Any] | None = None,
    ) -> dict[str, Any]:
        return self.record_source_event(
            event_id,
            seq,
            source_id,
            "release",
            rev=rev,
            reason=reason,
            dependencies=dependencies,
        )

    def source_actions(
        self, at_seq: int | None = None, source_id: str | None = None
    ) -> list[dict[str, Any]]:
        """Current-revision quarantine/release records effective at a seq."""
        records = source_action_records(self.conn, at_seq=at_seq)
        if source_id is not None:
            records = [r for r in records if r["source_id"] == source_id]
        return records

    def source_action_history(
        self, source_id: str | None = None
    ) -> list[dict[str, Any]]:
        """Every stored revision, including superseded and blocked records."""
        sql = """
            SELECT q.event_id, q.rev, q.seq, q.source_id, q.action, q.reason,
                   (h.current_rev = q.rev) AS is_current,
                   d.status AS derivation_status, d.reason_code AS derivation_reason
            FROM quarantine_record q
            LEFT JOIN event_head h ON h.event_id = q.event_id
            LEFT JOIN event_derivation d ON d.event_id = q.event_id
        """
        params: tuple[Any, ...] = ()
        if source_id is not None:
            sql += " WHERE q.source_id = ?"
            params = (source_id,)
        sql += " ORDER BY q.seq ASC, q.event_id ASC, q.rev ASC"
        return [dict(r) for r in self.conn.execute(sql, params).fetchall()]

    def source_status(
        self, source_id: str, at_seq: int | None = None
    ) -> dict[str, Any]:
        """Whether a source is currently quarantined and by which record."""
        actions = effective_source_actions(self.conn, at_seq=at_seq)
        record = actions.get(source_id)
        return {
            "source_id": source_id,
            "at_seq": at_seq,
            "status": "quarantined" if record and record["action"] == "quarantine"
            else "released",
            "record": record,
        }

    # ------------------------------------------------------------ reservations
    @staticmethod
    def _require_id(value: Any, field: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValidationRejected(f"{field} must be a non-empty string")
        return value.strip()

    def _reservation_row(self, request_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM reservation WHERE request_id = ?", (request_id,)
        ).fetchone()

    def reserve_sample(
        self,
        request_id: str,
        sample_id: str,
        amount: Any,
        *,
        note: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Reserve an exact amount of currently releasable sample.

        The request is all-or-nothing: it is either fully accepted against the
        releasable stock remaining after other open reservations, or rejected.
        A reservation never writes an effect, so physical inventory and source
        traceability are unchanged. Re-sending the same ``request_id`` with the
        same content is an idempotent no-op; different content is a conflict.
        """
        self._check_open()
        request_id = self._require_id(request_id, "request_id")
        sample_id = self._require_id(sample_id, "sample_id")
        if not isinstance(note, str):
            raise ValidationRejected("note must be a string")
        if metadata is not None and not isinstance(metadata, dict):
            raise ValidationRejected("metadata must be an object")
        try:
            quantity = positive_fraction(amount, "amount")
        except Exception as exc:  # rational parse errors -> envelope rejection
            raise ValidationRejected(str(exc)) from exc
        metadata = metadata or {}

        canonical = {
            "request_id": request_id,
            "sample_id": sample_id,
            "amount": q_to_str(quantity),
            "note": note.strip(),
            "metadata": metadata,
        }
        digest = reservation_content_hash(canonical)

        num, den = q_to_db(quantity)
        ts = now_iso()
        with self._immediate_tx():
            # Idempotency and capacity are both decided inside the write
            # transaction so concurrent sends of the same request_id return a
            # clean no-op and concurrent bookings can never overbook.
            existing = self._reservation_row(request_id)
            if existing is not None:
                if existing["content_hash"] != digest:
                    raise ConflictRejected(
                        f"reservation request {request_id!r} already exists with "
                        "different content"
                    )
                return self._reservation_result(existing, duplicate=True)

            # Re-check inside the write transaction so two concurrent requests
            # cannot both observe the same free capacity and overbook.
            report = sample_report(self.conn, sample_id)
            if not report["releasable"]:
                raise ConflictRejected(
                    f"sample {sample_id!r} is not releasable: "
                    f"{report['release_block_reason']}"
                )
            capacity = releasable_capacity(self.conn, sample_id)
            if quantity > capacity:
                raise ConflictRejected(
                    f"reservation of {q_to_str(quantity)} for sample {sample_id!r} "
                    f"exceeds remaining bookable releasable quantity "
                    f"{q_to_str(capacity)} (releasable "
                    f"{report['releasable_quantity']} minus open reservations "
                    f"{q_to_str(reserved_total(self.conn, sample_id))})"
                )
            self.conn.execute(
                """
                INSERT INTO reservation(
                    request_id, sample_id, amount_num, amount_den,
                    remaining_num, remaining_den, status, content_hash,
                    note, metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?)
                """,
                (
                    request_id,
                    sample_id,
                    num,
                    den,
                    num,
                    den,
                    digest,
                    note.strip(),
                    json.dumps(metadata, ensure_ascii=False, sort_keys=True),
                    ts,
                    ts,
                ),
            )
            self.conn.execute(
                """
                INSERT INTO ledger_write(event_id, rev, operation, content_hash, committed_at)
                VALUES (?, NULL, 'reservation', ?, ?)
                """,
                (request_id, digest, ts),
            )
            row = self._reservation_row(request_id)
        return self._reservation_result(row, duplicate=False)

    def cancel_reservation(
        self,
        request_id: str,
        *,
        amount: Any | None = None,
        operation_id: str | None = None,
        reason: str = "",
    ) -> dict[str, Any]:
        """Cancel part, or all, of the still-open amount of a reservation.

        Cancellation is bounded by the reservation's remaining amount: an
        over-cancel is rejected instead of going negative. The same
        ``operation_id`` retried with the same content is a no-op; reused with
        different content it is a conflict.
        """
        self._check_open()
        request_id = self._require_id(request_id, "request_id")
        operation_id = self._require_id(
            operation_id or f"cancel-{uuid.uuid4()}", "operation_id"
        )
        if not isinstance(reason, str):
            raise ValidationRejected("reason must be a string")

        row = self._reservation_row(request_id)
        if row is None:
            raise KeyError(request_id)

        if amount is None:
            cancel_all = True
        else:
            try:
                positive_fraction(amount, "amount")
            except Exception as exc:
                raise ValidationRejected(str(exc)) from exc
            cancel_all = False

        canonical = {
            "operation_id": operation_id,
            "request_id": request_id,
            "op_type": "cancel",
            "amount": q_to_str(
                positive_fraction(amount, "amount") if amount is not None else None
            ) if amount is not None else None,
            "cancel_all": cancel_all,
            "reason": reason.strip(),
        }
        # Resolve "cancel all" against the row's actual remaining amount inside
        # the transaction; the hash therefore records the concrete quantity.
        with self._immediate_tx():
            row = self._reservation_row(request_id)
            remaining = Fraction(
                int(row["remaining_num"]), int(row["remaining_den"])
            )
            if remaining <= 0:
                raise ConflictRejected(
                    f"reservation {request_id!r} has no remaining amount "
                    f"(status={row['status']})"
                )
            if amount is None:
                requested = remaining
            else:
                requested = positive_fraction(amount, "amount")
                if requested > remaining:
                    raise ConflictRejected(
                        f"cancel amount {q_to_str(requested)} exceeds remaining "
                        f"reservation amount {q_to_str(remaining)} for {request_id!r}"
                    )
            canonical["amount"] = q_to_str(requested)
            digest = operation_content_hash(canonical)
            existing = self._operation_row(operation_id)
            if existing is not None:
                if existing["content_hash"] != digest:
                    raise ConflictRejected(
                        f"operation {operation_id!r} already exists with different content"
                    )
                return self._operation_result(existing, duplicate=True)

            new_remaining = remaining - requested
            new_status = "cancelled" if new_remaining == 0 else "active"
            num, den = q_to_db(requested)
            rn, rd = q_to_db(new_remaining)
            ts = now_iso()
            self.conn.execute(
                """
                INSERT INTO reservation_operation(
                    operation_id, request_id, op_type, seq,
                    amount_num, amount_den, applied_num, applied_den,
                    withdrawal_event_id, content_hash, created_at
                ) VALUES (?, ?, 'cancel', NULL, ?, ?, ?, ?, NULL, ?, ?)
                """,
                (operation_id, request_id, num, den, num, den, digest, ts),
            )
            self.conn.execute(
                """
                UPDATE reservation
                SET remaining_num = ?, remaining_den = ?, status = ?,
                    content_hash = content_hash, updated_at = ?
                WHERE request_id = ?
                """,
                (rn, rd, new_status, ts, request_id),
            )
            self.conn.execute(
                """
                INSERT INTO ledger_write(event_id, rev, operation, content_hash, committed_at)
                VALUES (?, NULL, 'reservation_cancel', ?, ?)
                """,
                (operation_id, digest, ts),
            )
            row = self._reservation_row(request_id)
            op_row = self._operation_row(operation_id)
        return self._operation_result(op_row, duplicate=False, reservation_row=row)

    def fulfill_reservation(
        self,
        request_id: str,
        seq: int,
        amount: Any,
        *,
        operation_id: str | None = None,
        withdrawal_event_id: str | None = None,
        reason: str = "",
    ) -> dict[str, Any]:
        """Register an actual withdrawal against a reservation (fulfillment).

        The physical stock is removed through the ledger's normal ``discard``
        event inside the same transaction that debits the reservation, so the
        two can never diverge. The withdrawal is bounded by both the
        reservation's remaining amount and the currently releasable stock.
        Idempotent per ``operation_id``; reservations themselves stay at their
        original reserved amount, only their remaining amount is debited.
        """
        self._check_open()
        request_id = self._require_id(request_id, "request_id")
        operation_id = self._require_id(
            operation_id or f"fulfill-{uuid.uuid4()}", "operation_id"
        )
        if isinstance(seq, bool) or not isinstance(seq, int) or seq < 0:
            raise ValidationRejected("seq must be a non-negative integer")
        if not isinstance(reason, str):
            raise ValidationRejected("reason must be a string")
        try:
            quantity = positive_fraction(amount, "amount")
        except Exception as exc:
            raise ValidationRejected(str(exc)) from exc

        event_id = self._require_id(
            withdrawal_event_id or f"FULFILL-{operation_id}", "withdrawal_event_id"
        )
        canonical = {
            "operation_id": operation_id,
            "request_id": request_id,
            "op_type": "fulfill",
            "seq": seq,
            "amount": q_to_str(quantity),
            "withdrawal_event_id": event_id,
            "reason": reason.strip(),
        }
        digest = operation_content_hash(canonical)

        raw_event = {
            "event_id": event_id,
            "seq": seq,
            "rev": 1,
            "type": "discard",
            "sample": None,  # sample id is bound to the reservation inside the tx
            "amount": q_to_str(quantity),
            "note": reason.strip() or f"fulfillment of reservation {request_id}",
        }
        num, den = q_to_db(quantity)
        ts = now_iso()
        with self._immediate_tx():
            existing = self._operation_row(operation_id)
            if existing is not None:
                if existing["content_hash"] != digest:
                    raise ConflictRejected(
                        f"operation {operation_id!r} already exists with different content"
                    )
                return self._operation_result(existing, duplicate=True)

            row = self._reservation_row(request_id)
            if row is None:
                raise KeyError(request_id)
            sample_id = row["sample_id"]
            remaining = Fraction(
                int(row["remaining_num"]), int(row["remaining_den"])
            )
            if remaining <= 0:
                raise ConflictRejected(
                    f"reservation {request_id!r} has no remaining amount "
                    f"(status={row['status']})"
                )
            if quantity > remaining:
                raise ConflictRejected(
                    f"fulfillment amount {q_to_str(quantity)} exceeds remaining "
                    f"reservation amount {q_to_str(remaining)} for {request_id!r}"
                )

            report = sample_report(self.conn, sample_id)
            if not report["releasable"]:
                raise ConflictRejected(
                    f"sample {sample_id!r} is not releasable: "
                    f"{report['release_block_reason']}"
                )
            releasable = Fraction(
                int(report["releasable_numerator"]),
                int(report["releasable_denominator"]),
            )
            if quantity > releasable:
                raise ConflictRejected(
                    f"fulfillment amount {q_to_str(quantity)} exceeds releasable "
                    f"stock {q_to_str(releasable)} of sample {sample_id!r}"
                )

            raw_event["sample"] = sample_id
            prepared = self._prepare_event(raw_event)
            submission = self._submit_event_in_tx(prepared)
            if submission["derivation"]["status"] != "valid":
                # The transaction rolls back; nothing is reserved or withdrawn.
                raise ConflictRejected(
                    "fulfillment withdrawal was not derivable: "
                    f"{submission['derivation']['reason_code']}"
                )
            new_remaining = remaining - quantity
            new_status = "fulfilled" if new_remaining == 0 else "active"
            rn, rd = q_to_db(new_remaining)
            self.conn.execute(
                """
                INSERT INTO reservation_operation(
                    operation_id, request_id, op_type, seq,
                    amount_num, amount_den, applied_num, applied_den,
                    withdrawal_event_id, content_hash, created_at
                ) VALUES (?, ?, 'fulfill', ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    operation_id,
                    request_id,
                    seq,
                    num,
                    den,
                    num,
                    den,
                    event_id,
                    digest,
                    ts,
                ),
            )
            self.conn.execute(
                """
                UPDATE reservation
                SET remaining_num = ?, remaining_den = ?, status = ?,
                    updated_at = ?
                WHERE request_id = ?
                """,
                (rn, rd, new_status, ts, request_id),
            )
            self.conn.execute(
                """
                INSERT INTO ledger_write(event_id, rev, operation, content_hash, committed_at)
                VALUES (?, NULL, 'reservation_fulfill', ?, ?)
                """,
                (operation_id, digest, ts),
            )
            row = self._reservation_row(request_id)
            op_row = self._operation_row(operation_id)
        return self._operation_result(
            op_row, duplicate=False, reservation_row=row, submission=submission
        )

    def _operation_row(self, operation_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM reservation_operation WHERE operation_id = ?",
            (operation_id,),
        ).fetchone()

    def _reservation_result(
        self, row: sqlite3.Row, *, duplicate: bool
    ) -> dict[str, Any]:
        result = list_reservations(
            self.conn, request_id=row["request_id"], with_coverage=True
        )[0]
        # "status" is the reservation lifecycle state; the response outcome of
        # this call is reported separately so retries are distinguishable.
        result["request_outcome"] = "duplicate_noop" if duplicate else "accepted"
        return result

    def _operation_result(
        self,
        op_row: sqlite3.Row,
        *,
        duplicate: bool,
        reservation_row: sqlite3.Row | None = None,
        submission: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        ops = list_operations(self.conn, op_row["request_id"])
        operation = next(o for o in ops if o["operation_id"] == op_row["operation_id"])
        if reservation_row is None:
            reservation_row = self._reservation_row(op_row["request_id"])
        result = {
            "status": "duplicate_noop" if duplicate else "accepted",
            "operation": operation,
            "reservation": list_reservations(
                self.conn,
                request_id=reservation_row["request_id"],
                with_coverage=True,
            )[0],
        }
        if submission is not None:
            result["withdrawal_event"] = {
                "event_id": submission["event_id"],
                "seq": submission["seq"],
                "rev": submission["rev"],
                "derivation": submission["derivation"],
            }
        return result

    def get_reservation(self, request_id: str) -> dict[str, Any]:
        self._check_open()
        request_id = self._require_id(request_id, "request_id")
        rows = list_reservations(self.conn, request_id=request_id)
        if not rows:
            raise KeyError(request_id)
        result = dict(rows[0])
        result["operations"] = list_operations(self.conn, request_id)
        return result

    def list_reservations(
        self,
        *,
        sample_id: str | None = None,
        status: str | None = None,
        with_coverage: bool = True,
    ) -> list[dict[str, Any]]:
        self._check_open()
        if status is not None and status not in {
            "active",
            "fulfilled",
            "cancelled",
            "closed",
        }:
            raise ValidationRejected(
                "status must be one of active, fulfilled, cancelled, closed"
            )
        return list_reservations(
            self.conn,
            sample_id=sample_id,
            status=status,
            with_coverage=with_coverage,
        )

    def reservation_operations(
        self, request_id: str | None = None
    ) -> list[dict[str, Any]]:
        self._check_open()
        return list_operations(self.conn, request_id)

    def reservable_quantity(self, sample_id: str) -> dict[str, Any]:
        """Remaining quantity that can still be reserved on a sample now."""
        self._check_open()
        sample_id = self._require_id(sample_id, "sample_id")
        return reservation_coverage(self.conn, sample_id)

    def reservation_coverage_report(
        self, sample_id: str | None = None
    ) -> list[dict[str, Any]]:
        """Name reservations left uncovered by later stock changes.

        Reservation amounts are never rewritten. When actual withdrawals or
        source quarantine later reduce releasable stock below the open holds,
        the report names every affected reservation and the total uncovered
        amount, attributed first-come-first-served.
        """
        self._check_open()
        if sample_id is not None:
            sample_id = self._require_id(sample_id, "sample_id")
            return [reservation_coverage(self.conn, sample_id)]
        return all_coverage(self.conn)

    # ------------------------------------------------------------------ audit
    def audit_sample_diff(
        self,
        sample_id: str,
        from_write_id: int,
        to_write_id: int,
        from_seq: int,
        to_seq: int,
    ) -> dict[str, Any]:
        """Read-only diff of a sample's conclusions between two write cutoffs.

        Compares the ledger's interpretation of ``sample_id`` as of
        ``from_write_id`` (earlier) and ``to_write_id`` (later, inclusive;
        0 means the empty ledger) over the business interval
        ``[from_seq, to_seq)``, and returns only the segments where the
        conclusions differ.  The query never modifies the database.
        """
        self._check_open()
        return sample_diff_between_writes(
            self.conn, sample_id, from_write_id, to_write_id, from_seq, to_seq
        )

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
