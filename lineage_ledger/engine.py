"""Deterministic local derivation engine and bounded lineage queries."""
from __future__ import annotations

import json
import sqlite3
from collections import deque
from datetime import datetime, timezone
from fractions import Fraction
from typing import Any, Iterable

from .normalize import refs_from_event
from .rational_utils import q_from_db, q_to_db, q_to_str


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


# ---------------------------------------------------------------------------
# Exact state
# ---------------------------------------------------------------------------

def sample_state(
    conn: sqlite3.Connection, sample_id: str, at_seq: int | None = None
) -> dict[str, Any]:
    if at_seq is None:
        qty_rows = conn.execute(
            "SELECT delta_num, delta_den FROM effect WHERE sample_id = ?",
            (sample_id,),
        ).fetchall()
        source_rows = conn.execute(
            """
            SELECT original_id, delta_num, delta_den
            FROM effect_source
            WHERE sample_id = ?
            """,
            (sample_id,),
        ).fetchall()
    else:
        qty_rows = conn.execute(
            """
            SELECT e.delta_num, e.delta_den
            FROM effect e
            WHERE e.sample_id = ? AND e.seq <= ?
            """,
            (sample_id, at_seq),
        ).fetchall()
        source_rows = conn.execute(
            """
            SELECT es.original_id, es.delta_num, es.delta_den
            FROM effect_source es
            JOIN effect e
              ON e.event_id = es.event_id AND e.sample_id = es.sample_id
            WHERE es.sample_id = ? AND e.seq <= ?
            """,
            (sample_id, at_seq),
        ).fetchall()

    quantity = sum(
        (q_from_db(r["delta_num"], r["delta_den"]) for r in qty_rows),
        Fraction(0, 1),
    )
    composition: dict[str, Fraction] = {}
    for row in source_rows:
        delta = q_from_db(row["delta_num"], row["delta_den"])
        composition[row["original_id"]] = (
            composition.get(row["original_id"], Fraction(0, 1)) + delta
        )
    composition = {k: v for k, v in composition.items() if v != 0}
    return {
        "sample_id": sample_id,
        "formed": len(qty_rows) > 0,
        "quantity": quantity,
        "composition": composition,
        "at_seq": at_seq,
    }


def latest_sample_touch(
    conn: sqlite3.Connection,
    sample_id: str,
    *,
    before_seq: int | None = None,
    at_or_before_seq: int | None = None,
) -> sqlite3.Row | None:
    where = ["r.sample_id = ?"]
    params: list[Any] = [sample_id]
    if before_seq is not None:
        where.append("r.seq < ?")
        params.append(before_seq)
    if at_or_before_seq is not None:
        where.append("r.seq <= ?")
        params.append(at_or_before_seq)
    sql = f"""
        SELECT DISTINCT h.event_id, h.current_rev AS rev, h.seq,
               d.status, d.reason_code, d.reason_json
        FROM event_sample_ref r
        JOIN event_head h
          ON h.event_id = r.event_id AND h.current_rev = r.rev
        LEFT JOIN event_derivation d
          ON d.event_id = h.event_id
        WHERE {' AND '.join(where)}
        ORDER BY h.seq DESC, h.event_id
        LIMIT 1
    """
    return conn.execute(sql, params).fetchone()


def take_composition(state: dict[str, Any], amount: Fraction) -> dict[str, Fraction]:
    if not state.get("formed"):
        raise _Precondition(
            "predecessor_not_formed",
            {"sample_id": state.get("sample_id"), "role": "source"},
        )
    available: Fraction = state["quantity"]
    if amount > available:
        raise _Insufficient(amount, available)
    if available == 0:
        raise _Insufficient(amount, available)
    ratio = amount / available
    return {
        original_id: contribution * ratio
        for original_id, contribution in state["composition"].items()
        if contribution != 0
    }


class _Insufficient(Exception):
    def __init__(self, requested: Fraction, available: Fraction):
        self.requested = requested
        self.available = available


# ---------------------------------------------------------------------------
# Source quarantine / release interpretation
# ---------------------------------------------------------------------------

def source_action_records(
    conn: sqlite3.Connection, at_seq: int | None = None
) -> list[dict[str, Any]]:
    """Current-revision quarantine/release records effective at a business point.

    Records are ordered by business ``seq``.  Only the current revision of each
    stable event is interpreted, so a later revision corrects that business-time
    operation without erasing the superseded row.  A record whose current
    revision is blocked (for example by an invalid explicit dependency) does not
    take effect.
    """
    sql = """
        SELECT q.event_id, q.rev, q.seq, q.source_id, q.action, q.reason
        FROM quarantine_record q
        JOIN event_head h
          ON h.event_id = q.event_id AND h.current_rev = q.rev
        JOIN event_derivation d
          ON d.event_id = q.event_id AND d.status = 'valid'
    """
    params: tuple[Any, ...] = ()
    if at_seq is not None:
        sql += " WHERE q.seq <= ?"
        params = (at_seq,)
    sql += " ORDER BY q.seq ASC, q.event_id ASC"
    return [dict(r) for r in conn.execute(sql, params).fetchall()]


def effective_source_actions(
    conn: sqlite3.Connection, at_seq: int | None = None
) -> dict[str, dict[str, Any]]:
    """Return the latest effective action per original source at a business point."""
    latest: dict[str, dict[str, Any]] = {}
    for record in source_action_records(conn, at_seq=at_seq):
        latest[record["source_id"]] = record
    return latest


def samples_referencing_source(
    conn: sqlite3.Connection, source_id: str, at_seq: int | None = None
) -> set[str]:
    """Every sample that ever received a non-zero amount of ``source_id``.

    Used for citation restatement: a source event can change the release status
    of a historical reference even if that source has since been fully drained
    from the container, so this is deliberately broader than the *current*
    positive composition check.
    """
    sql = """
        SELECT DISTINCT es.sample_id
        FROM effect_source es
        JOIN effect e ON e.event_id = es.event_id AND e.sample_id = es.sample_id
        WHERE es.original_id = ?
    """
    params: tuple[Any, ...] = (source_id,)
    if at_seq is not None:
        sql += " AND e.seq <= ?"
        params += (at_seq,)
    return {row["sample_id"] for row in conn.execute(sql, params).fetchall()}


def release_assessment(
    conn: sqlite3.Connection,
    sample_id: str,
    state: dict[str, Any],
    status: str,
    at_seq: int | None = None,
) -> dict[str, Any]:
    """Decide whether the whole (uniformly mixed) sample may currently be released.

    Quarantine never changes physical quantity.  If any original source with a
    non-zero current contribution is quarantined at the queried business point,
    the entire homogeneous sample is non-releasable even though its actual
    inventory remains on the books.
    """
    actions = effective_source_actions(conn, at_seq=at_seq)
    blocked_sources: list[dict[str, Any]] = []
    for original_id, amount in sorted(state["composition"].items()):
        if amount <= 0:
            continue
        record = actions.get(original_id)
        if record is None or record["action"] != "quarantine":
            continue
        blocked_sources.append(
            {
                "original_id": original_id,
                "amount": q_to_str(amount),
                "numerator": str(amount.numerator),
                "denominator": str(amount.denominator),
                "quarantine_event_id": record["event_id"],
                "quarantine_rev": record["rev"],
                "quarantine_seq": record["seq"],
                "reason": record["reason"],
            }
        )

    if status == "blocked":
        reason = "lineage_blocked"
    elif status == "not_formed":
        reason = "not_formed"
    elif status == "exhausted":
        reason = "no_available_quantity"
    elif status == "invalid_negative":
        reason = "invalid_negative_quantity"
    elif state["quantity"] <= 0:
        reason = "no_available_quantity"
    elif blocked_sources:
        reason = "quarantined_source"
    else:
        reason = None

    releasable = reason is None
    releasable_quantity = state["quantity"] if releasable else Fraction(0, 1)

    effective_records = [
        dict(record, status=record["action"])
        for record in source_action_records(conn, at_seq=at_seq)
    ]
    return {
        "sample_id": sample_id,
        "at_seq": at_seq,
        "releasable": releasable,
        "releasable_quantity": releasable_quantity,
        "reason": reason,
        "blocked_sources": blocked_sources,
        "quarantined_source_ids": [b["original_id"] for b in blocked_sources],
        "effective_source_records": effective_records,
    }



# ---------------------------------------------------------------------------
# Affected lineage closure
# ---------------------------------------------------------------------------

def affected_closure(
    conn: sqlite3.Connection, trigger_event_id: str, old_rev: int | None = None
) -> set[str]:
    affected: set[str] = {trigger_event_id}
    queue: deque[str] = deque([trigger_event_id])

    old_sample_seed: set[str] = set()
    if old_rev:
        rows = conn.execute(
            "SELECT DISTINCT sample_id FROM event_sample_ref WHERE event_id = ? AND rev = ?",
            (trigger_event_id, old_rev),
        ).fetchall()
        old_sample_seed.update(r["sample_id"] for r in rows)

    while queue:
        event_id = queue.popleft()
        head = conn.execute(
            "SELECT current_rev AS rev, seq FROM event_head WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        if head is None:
            continue

        sample_rows = conn.execute(
            "SELECT DISTINCT sample_id FROM event_sample_ref WHERE event_id = ? AND rev = ?",
            (event_id, head["rev"]),
        ).fetchall()
        samples = {r["sample_id"] for r in sample_rows}
        if event_id == trigger_event_id:
            samples |= old_sample_seed

        for sample_id in samples:
            downstream_rows = conn.execute(
                """
                SELECT DISTINCT h.event_id
                FROM event_sample_ref r
                JOIN event_head h
                  ON h.event_id = r.event_id AND h.current_rev = r.rev
                WHERE r.sample_id = ? AND h.seq > ?
                """,
                (sample_id, head["seq"]),
            ).fetchall()
            for row in downstream_rows:
                other = row["event_id"]
                if other not in affected:
                    affected.add(other)
                    queue.append(other)

        dep_rows = conn.execute(
            """
            SELECT DISTINCT d.event_id
            FROM event_dependency d
            JOIN event_head h ON h.event_id = d.event_id AND h.current_rev = d.rev
            WHERE d.depends_on_event_id = ?
            """,
            (event_id,),
        ).fetchall()
        for row in dep_rows:
            other = row["event_id"]
            if other not in affected:
                affected.add(other)
                queue.append(other)

    return affected


def recompute_closure(conn: sqlite3.Connection, event_ids: Iterable[str]) -> None:
    ids = list(event_ids)
    if not ids:
        return
    placeholders = ",".join("?" for _ in ids)
    heads = conn.execute(
        f"""
        SELECT event_id, current_rev AS rev, seq
        FROM event_head
        WHERE event_id IN ({placeholders})
        ORDER BY seq ASC, event_id ASC
        """,
        ids,
    ).fetchall()
    for head in heads:
        derive_event(conn, head["event_id"])


# ---------------------------------------------------------------------------
# Single-event derivation
# ---------------------------------------------------------------------------

def _clear_derived(conn: sqlite3.Connection, event_id: str) -> None:
    conn.execute("DELETE FROM effect_source WHERE event_id = ?", (event_id,))
    conn.execute("DELETE FROM effect WHERE event_id = ?", (event_id,))
    conn.execute("DELETE FROM event_upstream WHERE event_id = ?", (event_id,))
    conn.execute("DELETE FROM event_derivation WHERE event_id = ?", (event_id,))


def _add_upstream(
    conn: sqlite3.Connection, event_id: str, upstream_id: str, kind: str
) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO event_upstream(event_id, upstream_event_id, kind)
        VALUES (?, ?, ?)
        """,
        (event_id, upstream_id, kind),
    )


def _block(
    conn: sqlite3.Connection,
    event_id: str,
    rev: int,
    seq: int,
    code: str,
    details: Any,
) -> dict[str, Any]:
    conn.execute(
        """
        INSERT INTO event_derivation(
            event_id, rev, seq, status, reason_code, reason_json, derived_at
        ) VALUES (?, ?, ?, 'blocked', ?, ?, ?)
        """,
        (event_id, rev, seq, code, json_dumps(details), now_iso()),
    )
    return {"status": "blocked", "reason_code": code, "details": details}


def _valid(conn: sqlite3.Connection, event_id: str, rev: int, seq: int) -> dict[str, Any]:
    conn.execute(
        """
        INSERT INTO event_derivation(
            event_id, rev, seq, status, reason_code, reason_json, derived_at
        ) VALUES (?, ?, ?, 'valid', NULL, NULL, ?)
        """,
        (event_id, rev, seq, now_iso()),
    )
    return {"status": "valid"}


def _insert_effect(
    conn: sqlite3.Connection,
    event_id: str,
    rev: int,
    seq: int,
    sample_id: str,
    delta_qty: Fraction,
    composition_delta: dict[str, Fraction],
) -> None:
    n, d = q_to_db(delta_qty)
    conn.execute(
        """
        INSERT INTO effect(event_id, rev, seq, sample_id, delta_num, delta_den)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (event_id, rev, seq, sample_id, n, d),
    )
    for original_id, delta in sorted(composition_delta.items()):
        if delta == 0:
            continue
        n, d = q_to_db(delta)
        conn.execute(
            """
            INSERT INTO effect_source(
                event_id, sample_id, original_id, delta_num, delta_den
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (event_id, sample_id, original_id, n, d),
        )


def derive_event(conn: sqlite3.Connection, event_id: str) -> dict[str, Any]:
    head = conn.execute(
        "SELECT event_id, current_rev AS rev, seq FROM event_head WHERE event_id = ?",
        (event_id,),
    ).fetchone()
    if head is None:
        raise KeyError(event_id)
    rev = int(head["rev"])
    seq = int(head["seq"])
    version = conn.execute(
        "SELECT payload_json FROM event_version WHERE event_id = ? AND rev = ?",
        (event_id, rev),
    ).fetchone()
    event = json.loads(version["payload_json"])
    _clear_derived(conn, event_id)

    problems: list[dict[str, Any]] = []

    # Explicit dependencies.
    for dep in event.get("dependencies", []):
        dep_id = dep["event_id"]
        _add_upstream(conn, event_id, dep_id, "explicit_dependency")
        dep_head = conn.execute(
            "SELECT seq FROM event_head WHERE event_id = ?", (dep_id,)
        ).fetchone()
        if dep_head is None:
            problems.append(
                {
                    "code": "missing_explicit_dependency",
                    "event_id": dep_id,
                    "declared_seq": dep["seq"],
                }
            )
            continue
        if int(dep_head["seq"]) != int(dep["seq"]) or int(dep_head["seq"]) >= seq:
            problems.append(
                {
                    "code": "dependency_sequence_invalid",
                    "event_id": dep_id,
                    "declared_seq": dep["seq"],
                    "actual_seq": dep_head["seq"],
                    "dependent_seq": seq,
                }
            )
        dep_der = conn.execute(
            "SELECT status, reason_code FROM event_derivation WHERE event_id = ?",
            (dep_id,),
        ).fetchone()
        if dep_der is None or dep_der["status"] != "valid":
            problems.append(
                {
                    "code": "blocked_explicit_dependency",
                    "event_id": dep_id,
                    "reason_code": dep_der["reason_code"] if dep_der else "not_derived",
                }
            )

    # Natural sample-touch predecessors.
    for sample_id, role in refs_from_event(event):
        prior = latest_sample_touch(conn, sample_id, before_seq=seq)
        if prior is not None:
            _add_upstream(
                conn, event_id, prior["event_id"], f"prior_touch:{role}"
            )
            if prior["status"] != "valid":
                problems.append(
                    {
                        "code": "blocked_predecessor",
                        "sample_id": sample_id,
                        "role": role,
                        "event_id": prior["event_id"],
                        "seq": prior["seq"],
                        "reason_code": prior["reason_code"],
                    }
                )

    # Identity rule: an invalid/superseded derived output is never a new original.
    if event["type"] == "original":
        sample_id = event["sample_id"]
        derived_claim = conn.execute(
            """
            SELECT event_id, rev FROM sample_identity_claim
            WHERE sample_id = ? AND claim_kind = 'derived_output'
            LIMIT 1
            """,
            (sample_id,),
        ).fetchone()
        if derived_claim is not None:
            problems.append(
                {
                    "code": "derived_sample_cannot_be_original",
                    "sample_id": sample_id,
                    "claimed_by_event_id": derived_claim["event_id"],
                    "rev": derived_claim["rev"],
                }
            )
        other_original = conn.execute(
            """
            SELECT event_id, rev FROM sample_identity_claim
            WHERE sample_id = ? AND claim_kind = 'original' AND event_id <> ?
            LIMIT 1
            """,
            (sample_id, event_id),
        ).fetchone()
        if other_original is not None:
            problems.append(
                {
                    "code": "duplicate_original",
                    "sample_id": sample_id,
                    "other_event_id": other_original["event_id"],
                }
            )

    if problems:
        # Identity violations explain why a sample can never be re-rooted; make
        # that the primary code, while retaining every detected problem.
        primary = next(
            (
                p
                for p in problems
                if p["code"]
                in {"derived_sample_cannot_be_original", "duplicate_original"}
            ),
            problems[0],
        )
        return _block(
            conn, event_id, rev, seq, primary["code"], {"problems": problems}
        )

    effects: dict[str, tuple[Fraction, dict[str, Fraction]]] = {}

    def add_delta(
        sample_id: str, qty_delta: Fraction, comp_delta: dict[str, Fraction]
    ) -> None:
        if sample_id in effects:
            old_q, old_comp = effects[sample_id]
            merged = dict(old_comp)
            for k, v in comp_delta.items():
                merged[k] = merged.get(k, Fraction(0, 1)) + v
            effects[sample_id] = (old_q + qty_delta, merged)
        else:
            effects[sample_id] = (qty_delta, dict(comp_delta))

    try:
        t = event["type"]
        if t == "original":
            qty = Fraction(event["base_amount"], 1)
            add_delta(event["sample_id"], qty, {event["sample_id"]: qty})

        elif t in {"quarantine", "release"}:
            # Pure source-level decision: no physical effect is written, so
            # inventory and pre-quarantine detection interpretation are untouched.
            pass

        elif t == "discard":
            sid = event["sample"]
            # Normalized amount is an exact Fraction string such as "3/2".
            amount = Fraction(event["amount"])
            state = sample_state_before(conn, sid, seq)
            removed = take_composition(state, amount)
            add_delta(sid, -amount, {k: -v for k, v in removed.items()})

        elif t == "transfer":
            source = event["source"]
            dest = event["destination"]
            amount = Fraction(event["amount"])
            src_state = sample_state_before(conn, source, seq)
            moved = take_composition(src_state, amount)
            add_delta(source, -amount, {k: -v for k, v in moved.items()})
            add_delta(dest, amount, dict(moved))

        elif t == "split":
            source = event["source"]
            src_state = sample_state_before(conn, source, seq)
            total_out = sum((Fraction(o["amount"]) for o in event["outputs"]), Fraction(0))
            # Validate total first by attempting proportional slices independently
            # against a logical remaining state. Exact allocation is linear, so each
            # slice uses the original source ratio; total_out <= source quantity.
            if not src_state["formed"]:
                raise _Precondition(
                    "predecessor_not_formed", {"sample_id": source, "role": "source"}
                )
            if total_out > src_state["quantity"]:
                raise _Insufficient(total_out, src_state["quantity"])
            for out in event["outputs"]:
                amount = Fraction(out["amount"])
                part = take_composition(src_state, amount)
                add_delta(source, -amount, {k: -v for k, v in part.items()})
                add_delta(out["sample_id"], amount, dict(part))

        elif t == "merge":
            output = event["output"]
            received_comp: dict[str, Fraction] = {}
            total = Fraction(0, 1)
            for src in event["sources"]:
                sid = src["sample_id"]
                amount = Fraction(src["amount"])
                state = sample_state_before(conn, sid, seq)
                part = take_composition(state, amount)
                add_delta(sid, -amount, {k: -v for k, v in part.items()})
                total += amount
                for original_id, value in part.items():
                    received_comp[original_id] = (
                        received_comp.get(original_id, Fraction(0, 1)) + value
                    )
            add_delta(output, total, received_comp)

    except _Precondition as exc:
        return _block(conn, event_id, rev, seq, exc.code, exc.details)
    except _Insufficient as exc:
        return _block(
            conn,
            event_id,
            rev,
            seq,
            "insufficient_quantity",
            {
                "requested": q_to_str(exc.requested),
                "available": q_to_str(exc.available),
            },
        )

    for sample_id, (delta_qty, comp_delta) in sorted(effects.items()):
        _insert_effect(conn, event_id, rev, seq, sample_id, delta_qty, comp_delta)
    return _valid(conn, event_id, rev, seq)


def sample_state_before(
    conn: sqlite3.Connection, sample_id: str, seq: int
) -> dict[str, Any]:
    # Effects are inclusive by seq; pre-state must exclude the current seq.
    # Global seq uniqueness makes seq < event.seq equivalent here.
    return sample_state(conn, sample_id, at_seq=seq - 1)


class _Precondition(Exception):
    def __init__(self, code: str, details: dict[str, Any]):
        self.code = code
        self.details = details


# ---------------------------------------------------------------------------
# Evidence and reports
# ---------------------------------------------------------------------------

def evidence_for_events(
    conn: sqlite3.Connection, root_event_ids: Iterable[str]
) -> list[dict[str, Any]]:
    roots = [e for e in dict.fromkeys(root_event_ids) if e]
    if not roots:
        return []
    placeholders = ",".join("?" for _ in roots)
    rows = conn.execute(
        f"""
        WITH RECURSIVE anc(event_id) AS (
            SELECT event_id FROM (
                SELECT value AS event_id FROM json_each(?)
            )
            UNION
            SELECT u.upstream_event_id
            FROM event_upstream u
            JOIN anc a ON u.event_id = a.event_id
        )
        SELECT DISTINCT anc.event_id AS event_id,
               h.current_rev AS rev, h.seq AS seq,
               d.status, d.reason_code
        FROM anc
        LEFT JOIN event_head h ON h.event_id = anc.event_id
        LEFT JOIN event_derivation d ON d.event_id = anc.event_id
        ORDER BY h.seq IS NULL, h.seq ASC, anc.event_id ASC
        """,
        (json_dumps(roots),),
    ).fetchall()
    return [dict(r) for r in rows]


def sample_report(
    conn: sqlite3.Connection, sample_id: str, at_seq: int | None = None
) -> dict[str, Any]:
    state = sample_state(conn, sample_id, at_seq=at_seq)
    touch = latest_sample_touch(conn, sample_id, at_or_before_seq=at_seq)

    root_events = [
        r["event_id"]
        for r in conn.execute(
            """
            SELECT event_id FROM effect
            WHERE sample_id = ?
            """
            + ("AND seq <= ?" if at_seq is not None else "")
            + " ORDER BY seq",
            (sample_id,) if at_seq is None else (sample_id, at_seq),
        ).fetchall()
    ]
    if touch is not None:
        root_events.append(touch["event_id"])
    evidence = evidence_for_events(conn, root_events)

    if touch is not None and touch["status"] == "blocked":
        status = "blocked"
    elif not state["formed"]:
        status = "not_formed"
    elif state["quantity"] == 0:
        status = "exhausted"
    elif state["quantity"] < 0:
        status = "invalid_negative"
    else:
        status = "available"

    release = release_assessment(
        conn, sample_id, state, status, at_seq=at_seq
    )

    contributions = [
        {
            "original_id": original_id,
            "amount": q_to_str(amount),
            "numerator": str(amount.numerator),
            "denominator": str(amount.denominator),
        }
        for original_id, amount in sorted(state["composition"].items())
    ]

    if at_seq is None:
        hist_rows = conn.execute(
            """
            SELECT original_id, delta_num, delta_den
            FROM effect_source
            WHERE sample_id = ?
            """,
            (sample_id,),
        ).fetchall()
    else:
        hist_rows = conn.execute(
            """
            SELECT es.original_id, es.delta_num, es.delta_den
            FROM effect_source es
            JOIN effect e
              ON e.event_id = es.event_id AND e.sample_id = es.sample_id
            WHERE es.sample_id = ?
              AND e.seq <= ?
            """,
            (sample_id, at_seq),
        ).fetchall()
    historical: dict[str, Fraction] = {}
    for row in hist_rows:
        delta = q_from_db(row["delta_num"], row["delta_den"])
        if delta > 0:
            historical[row["original_id"]] = (
                historical.get(row["original_id"], Fraction(0, 1)) + delta
            )
    historical_contributions = [
        {
            "original_id": original_id,
            "entered_amount": q_to_str(amount),
            "numerator": str(amount.numerator),
            "denominator": str(amount.denominator),
        }
        for original_id, amount in sorted(historical.items())
        if amount != 0
    ]

    return {
        "sample_id": sample_id,
        "at_seq": at_seq,
        "status": status,
        "formed": state["formed"],
        "releasable": release["releasable"],
        "releasable_quantity": q_to_str(release["releasable_quantity"]),
        "releasable_numerator": str(release["releasable_quantity"].numerator),
        "releasable_denominator": str(release["releasable_quantity"].denominator),
        "release_block_reason": release["reason"],
        "blocked_sources": release["blocked_sources"],
        "quarantined_source_ids": release["quarantined_source_ids"],
        "effective_source_records": release["effective_source_records"],
        "available_quantity": q_to_str(state["quantity"]),
        "available_numerator": str(state["quantity"].numerator),
        "available_denominator": str(state["quantity"].denominator),
        "precise_contributions": contributions,
        "historical_input_contributions": historical_contributions,
        "latest_touch": dict(touch) if touch is not None else None,
        "evidence": evidence,
    }
