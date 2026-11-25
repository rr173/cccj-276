"""Sample reservation capacity and coverage interpretation.

Reservations are *capacity holds*, never physical movements.  All quantities
here are exact :class:`~fractions.Fraction` values in BASE units.  The only
physical mutation in the reservation workflow happens at fulfillment time,
when a normal ``discard`` event is written through the regular event pipeline;
lineage effects and source traceability are therefore produced exactly as for
any other actual withdrawal.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from fractions import Fraction
from typing import Any

from .engine import sample_report
from .rational_utils import q_from_db, q_to_str


def reservation_content_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def operation_content_hash(payload: dict[str, Any]) -> str:
    return reservation_content_hash(payload)


def _fraction(num: Any, den: Any) -> Fraction:
    return q_from_db(num, den)


def _q_fields(q: Fraction) -> dict[str, str]:
    return {"numerator": str(q.numerator), "denominator": str(q.denominator)}


def active_reservations(
    conn: sqlite3.Connection, sample_id: str | None = None
) -> list[sqlite3.Row]:
    """Active reservations for a sample (or every sample), oldest first."""
    sql = """
        SELECT * FROM reservation
        WHERE status = 'active'
    """
    params: tuple[Any, ...] = ()
    if sample_id is not None:
        sql += " AND sample_id = ?"
        params = (sample_id,)
    sql += " ORDER BY created_at ASC, rowid ASC"
    return list(conn.execute(sql, params).fetchall())


def reserved_total(
    conn: sqlite3.Connection, sample_id: str
) -> Fraction:
    """Sum of the still-open (remaining) reservation amounts on a sample."""
    total = Fraction(0, 1)
    for row in active_reservations(conn, sample_id):
        total += _fraction(row["remaining_num"], row["remaining_den"])
    return total


def releasable_capacity(conn: sqlite3.Connection, sample_id: str) -> Fraction:
    """Currently releasable physical quantity minus open reservation holds."""
    report = sample_report(conn, sample_id)
    releasable = Fraction(
        int(report["releasable_numerator"]),
        int(report["releasable_denominator"]),
    )
    return releasable - reserved_total(conn, sample_id)


def reservation_coverage(
    conn: sqlite3.Connection, sample_id: str
) -> dict[str, Any]:
    """Report how open reservations compare with the current releasable stock.

    Reservations keep their original amounts forever.  When physical stock is
    later reduced (actual withdrawals) or a contributing source is
    quarantined, the releasable stock can fall below the reserved total.  The
    gap is attributed to reservations in first-come-first-served order so the
    report names exactly which requests lose coverage and by how much; no
    reservation amount is silently rewritten and no inventory is fabricated.
    """
    report = sample_report(conn, sample_id)
    releasable = Fraction(
        int(report["releasable_numerator"]),
        int(report["releasable_denominator"]),
    )
    available = Fraction(
        int(report["available_numerator"]),
        int(report["available_denominator"]),
    )
    rows = active_reservations(conn, sample_id)
    reserved = sum(
        (_fraction(r["remaining_num"], r["remaining_den"]) for r in rows),
        Fraction(0, 1),
    )
    bookable = releasable - reserved

    # FIFO allocation of the currently releasable stock against open holds.
    pool = releasable
    per_reservation: list[dict[str, Any]] = []
    affected: list[dict[str, Any]] = []
    uncovered_total = Fraction(0, 1)
    for row in rows:
        remaining = _fraction(row["remaining_num"], row["remaining_den"])
        covered = min(remaining, max(pool, Fraction(0, 1)))
        uncovered = remaining - covered
        pool -= covered
        item = {
            "request_id": row["request_id"],
            "remaining_amount": q_to_str(remaining),
            **_q_fields(remaining),
            "covered_quantity": q_to_str(covered),
            **{f"covered_{k}": v for k, v in _q_fields(covered).items()},
            "uncovered_quantity": q_to_str(uncovered),
            **{f"uncovered_{k}": v for k, v in _q_fields(uncovered).items()},
            "fully_covered": uncovered == 0,
        }
        per_reservation.append(item)
        if uncovered > 0:
            affected.append(item)
            uncovered_total += uncovered

    reasons: list[str] = []
    if report["release_block_reason"] is not None:
        reasons.append(report["release_block_reason"])
    if reserved > releasable and report["release_block_reason"] is None:
        reasons.append("insufficient_releasable_quantity")

    return {
        "sample_id": sample_id,
        "available_quantity": q_to_str(available),
        **{f"available_{k}": v for k, v in _q_fields(available).items()},
        "releasable_quantity": q_to_str(releasable),
        **{f"releasable_{k}": v for k, v in _q_fields(releasable).items()},
        "releasable": report["releasable"],
        "release_block_reason": report["release_block_reason"],
        "reserved_quantity": q_to_str(reserved),
        **{f"reserved_{k}": v for k, v in _q_fields(reserved).items()},
        "bookable_quantity": q_to_str(bookable),
        **{f"bookable_{k}": v for k, v in _q_fields(bookable).items()},
        "fully_covered": uncovered_total == 0,
        "uncovered_total": q_to_str(uncovered_total),
        **{f"uncovered_total_{k}": v for k, v in _q_fields(uncovered_total).items()},
        "shortfall_reasons": reasons,
        "reservations": per_reservation,
        "affected_reservations": affected,
    }


def all_coverage(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Coverage report for every sample that still has an open reservation."""
    rows = conn.execute(
        "SELECT DISTINCT sample_id FROM reservation WHERE status = 'active' "
        "ORDER BY sample_id"
    ).fetchall()
    return [reservation_coverage(conn, row["sample_id"]) for row in rows]


def row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    result = dict(row)
    result["metadata"] = json.loads(result.pop("metadata_json") or "{}")
    for key in (
        "amount",
        "remaining",
    ):
        num = result.pop(f"{key}_num")
        den = result.pop(f"{key}_den")
        q = _fraction(num, den)
        result[f"{key}_quantity"] = q_to_str(q)
        result[f"{key}_numerator"] = str(q.numerator)
        result[f"{key}_denominator"] = str(q.denominator)
    return result


def list_reservations(
    conn: sqlite3.Connection,
    *,
    sample_id: str | None = None,
    status: str | None = None,
    request_id: str | None = None,
    with_coverage: bool = True,
) -> list[dict[str, Any]]:
    sql = "SELECT * FROM reservation WHERE 1 = 1"
    params: list[Any] = []
    if sample_id is not None:
        sql += " AND sample_id = ?"
        params.append(sample_id)
    if status is not None:
        sql += " AND status = ?"
        params.append(status)
    if request_id is not None:
        sql += " AND request_id = ?"
        params.append(request_id)
    sql += " ORDER BY rowid ASC"
    rows = conn.execute(sql, params).fetchall()

    coverage_by_sample: dict[str, dict[str, Any]] = {}
    if with_coverage:
        wanted = {r["sample_id"] for r in rows}
        for sid in wanted:
            coverage_by_sample[sid] = reservation_coverage(conn, sid)

    result: list[dict[str, Any]] = []
    for row in rows:
        item = row_to_dict(row)
        if with_coverage:
            coverage = coverage_by_sample[row["sample_id"]]
            match = next(
                (
                    r
                    for r in coverage["reservations"]
                    if r["request_id"] == row["request_id"]
                ),
                None,
            )
            item["coverage"] = match
        result.append(item)
    return result


def list_operations(
    conn: sqlite3.Connection, request_id: str | None = None
) -> list[dict[str, Any]]:
    sql = "SELECT * FROM reservation_operation"
    params: tuple[Any, ...] = ()
    if request_id is not None:
        sql += " WHERE request_id = ?"
        params = (request_id,)
    sql += " ORDER BY rowid ASC"
    rows = []
    for row in conn.execute(sql, params).fetchall():
        item = dict(row)
        for key in ("amount", "applied"):
            num = item.pop(f"{key}_num")
            den = item.pop(f"{key}_den")
            q = _fraction(num, den)
            item[f"{key}_quantity"] = q_to_str(q)
            item[f"{key}_numerator"] = str(q.numerator)
            item[f"{key}_denominator"] = str(q.denominator)
        rows.append(item)
    return rows
