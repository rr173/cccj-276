"""Read-only audit diff of a sample's conclusions between two write cutoffs.

When late events or revisions arrive, an auditor needs to know during which
business-time periods a sample's ledger conclusions changed.  This module
reconstructs the interpretation of the ledger at two commit cutoffs --
expressed as ``ledger_write.write_id`` values (0 means the empty ledger) --
and reports the business-seq segments inside ``[from_seq, to_seq)`` where the
sample's conclusions differ.  No audit object has to be registered and no
snapshot has to be prepared ahead of time.

Semantics at a cutoff ``W``:

* only writes with ``write_id <= W`` are on the books (the cutoff write is
  included);
* each event is interpreted at the newest revision committed by ``W``;
* sample identity claims registered by ``W`` stay in force even when they
  came from a superseded or invalid event version;
* the usual lineage / blocking / source-quarantine rules are interpreted by
  business ``seq`` -- an event at seq ``s`` is in effect at business point
  ``s``;
* reservations are pure capacity holds and never change these conclusions,
  while fulfillment withdrawals are normal events and are counted.

The query never modifies the original database.  The as-of interpretation is
replayed by the stock derivation engine on TEMP tables that shadow only the
sample's relevant lineage closure, so unrelated lineages are never scanned,
the database is never copied wholesale, and business points are never
enumerated one by one: each conclusion is piecewise constant in ``seq``, so
only the breakpoints where an input changes are evaluated.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Iterable

from .engine import (
    latest_sample_touch,
    recompute_closure,
    release_assessment,
    sample_state,
)
from .errors import ValidationRejected
from .rational_utils import q_to_str

# ledger_write operations that commit event versions.  Reservation writes are
# pure capacity bookkeeping and cannot change any physical conclusion.
_EVENT_OPERATIONS = ("submit", "revision")

# Main tables the derivation/report engine reads or writes.  Each is shadowed
# by a TEMP table of the same name holding only the sample's relevant closure
# (SQLite resolves unqualified table names to the temp schema first), so the
# stock engine code replays the as-of-cutoff interpretation without ever
# touching the real tables.
_SHADOW_TABLES = (
    "event_head",
    "event_version",
    "event_sample_ref",
    "sample_identity_claim",
    "quarantine_record",
    "event_derivation",
    "effect",
    "effect_source",
    "event_upstream",
)

_HELPER_TABLES = ("_audit_inc", "_audit_writes")


def _chunks(items: list[Any], size: int = 500) -> Iterable[list[Any]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _require_non_negative_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValidationRejected(f"{field} must be a non-negative integer")
    return value


# ---------------------------------------------------------------------------
# Relevant lineage closure
# ---------------------------------------------------------------------------

def _candidate_closure(
    conn: sqlite3.Connection, sample_id: str
) -> tuple[set[str], set[str]]:
    """Events and samples that can influence ``sample_id``'s conclusions.

    The walk only uses indexes keyed by sample/event id, so unrelated
    lineages are never scanned.  Every revision of every candidate event is
    considered here (a superseded revision may still be the newest one
    committed at an earlier cutoff); per-cutoff head selection happens later.
    Quarantine/release records join through their ``source_id`` because they
    carry no sample references.  Explicit dependencies are followed because a
    dependency's own derivation decides whether the dependent event is
    blocked.
    """
    events: set[str] = set()
    samples: set[str] = set()
    sample_frontier = {sample_id}
    event_frontier: set[str] = set()
    while sample_frontier or event_frontier:
        if sample_frontier:
            batch = sorted(sample_frontier)
            samples.update(batch)
            sample_frontier = set()
            for chunk in _chunks(batch):
                marks = ",".join("?" for _ in chunk)
                rows = conn.execute(
                    f"""
                    SELECT DISTINCT event_id FROM event_sample_ref
                    WHERE sample_id IN ({marks})
                    """,
                    chunk,
                ).fetchall()
                rows += conn.execute(
                    f"""
                    SELECT DISTINCT event_id FROM quarantine_record
                    WHERE source_id IN ({marks})
                    """,
                    chunk,
                ).fetchall()
                for row in rows:
                    event_id = row["event_id"]
                    if event_id not in events:
                        event_frontier.add(event_id)
        if event_frontier:
            batch = sorted(event_frontier)
            event_frontier = set()
            new_events = [e for e in batch if e not in events]
            events.update(batch)
            for chunk in _chunks(new_events):
                marks = ",".join("?" for _ in chunk)
                rows = conn.execute(
                    f"""
                    SELECT DISTINCT sample_id FROM event_sample_ref
                    WHERE event_id IN ({marks})
                    """,
                    chunk,
                ).fetchall()
                for row in rows:
                    sid = row["sample_id"]
                    if sid not in samples:
                        sample_frontier.add(sid)
                rows = conn.execute(
                    f"""
                    SELECT DISTINCT depends_on_event_id FROM event_dependency
                    WHERE event_id IN ({marks})
                    """,
                    chunk,
                ).fetchall()
                for row in rows:
                    dep_id = row["depends_on_event_id"]
                    if dep_id not in events:
                        event_frontier.add(dep_id)
    return events, samples


def _commit_map(
    conn: sqlite3.Connection, event_ids: set[str], max_write_id: int
) -> dict[str, list[tuple[int, int]]]:
    """Map each candidate event to its ``(write_id, rev)`` commits.

    A single bounded scan of the slim commit log (``write_id <=
    max_write_id``) filtered to the candidate closure -- the write log is the
    system of record for the cutoff coordinate, and no unrelated lineage data
    (payloads, effects) is read here.
    """
    writes: dict[str, list[tuple[int, int]]] = {}
    for chunk in _chunks(sorted(event_ids)):
        marks = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"""
            SELECT event_id, rev, write_id
            FROM ledger_write
            WHERE operation IN ('submit', 'revision')
              AND write_id <= ?
              AND event_id IN ({marks})
            """,
            (max_write_id, *chunk),
        ).fetchall()
        for row in rows:
            writes.setdefault(row["event_id"], []).append(
                (int(row["write_id"]), int(row["rev"]))
            )
    for versions in writes.values():
        versions.sort()
    return writes


def _heads_at(
    writes: dict[str, list[tuple[int, int]]], cutoff: int
) -> dict[str, int]:
    """Newest committed revision per event at a write cutoff."""
    heads: dict[str, int] = {}
    for event_id, versions in writes.items():
        eligible = [rev for write_id, rev in versions if write_id <= cutoff]
        if eligible:
            heads[event_id] = eligible[-1]
    return heads


# ---------------------------------------------------------------------------
# As-of-cutoff replay on TEMP shadow tables
# ---------------------------------------------------------------------------

def _drop_shadow(conn: sqlite3.Connection) -> None:
    for name in _SHADOW_TABLES + _HELPER_TABLES:
        conn.execute(f"DROP TABLE IF EXISTS temp.{name}")


def _build_shadow(
    conn: sqlite3.Connection,
    heads: dict[str, int],
    writes: dict[str, list[tuple[int, int]]],
    cutoff: int,
) -> None:
    """Materialize the as-of-``cutoff`` ledger slice into TEMP shadow tables.

    Only the sample's candidate closure is copied -- never the whole
    database.  Identity claims are filtered to versions committed by the
    cutoff, so claims from superseded versions stay in force exactly as the
    ledger rules require.
    """
    conn.execute(
        "CREATE TEMP TABLE _audit_inc (event_id TEXT PRIMARY KEY, rev INTEGER NOT NULL)"
    )
    conn.executemany(
        "INSERT INTO temp._audit_inc (event_id, rev) VALUES (?, ?)",
        sorted(heads.items()),
    )
    conn.execute(
        """
        CREATE TEMP TABLE _audit_writes (
            event_id TEXT NOT NULL, rev INTEGER NOT NULL, write_id INTEGER NOT NULL
        )
        """
    )
    conn.executemany(
        "INSERT INTO temp._audit_writes (event_id, rev, write_id) VALUES (?, ?, ?)",
        [
            (event_id, rev, write_id)
            for event_id, versions in writes.items()
            for write_id, rev in versions
            if write_id <= cutoff
        ],
    )
    conn.execute(
        """
        CREATE TEMP TABLE event_head AS
        SELECT v.event_id, v.rev AS current_rev, v.seq,
               v.created_at, v.created_at AS updated_at
        FROM main.event_version v
        JOIN temp._audit_inc i ON i.event_id = v.event_id AND i.rev = v.rev
        """
    )
    conn.execute(
        """
        CREATE TEMP TABLE event_version AS
        SELECT v.*
        FROM main.event_version v
        JOIN temp._audit_inc i ON i.event_id = v.event_id AND i.rev = v.rev
        """
    )
    conn.execute(
        """
        CREATE TEMP TABLE event_sample_ref AS
        SELECT r.*
        FROM main.event_sample_ref r
        JOIN temp._audit_inc i ON i.event_id = r.event_id AND i.rev = r.rev
        """
    )
    conn.execute(
        """
        CREATE TEMP TABLE sample_identity_claim AS
        SELECT c.*
        FROM main.sample_identity_claim c
        JOIN temp._audit_writes w
          ON w.event_id = c.event_id AND w.rev = c.rev
        """
    )
    # Every revision of an included quarantine/release event is copied; the
    # head join in the engine's source_action_records picks the revision that
    # was current at the cutoff.
    conn.execute(
        """
        CREATE TEMP TABLE quarantine_record AS
        SELECT q.*
        FROM main.quarantine_record q
        JOIN temp._audit_inc i ON i.event_id = q.event_id
        """
    )
    for name in ("event_derivation", "effect", "effect_source", "event_upstream"):
        conn.execute(
            f"CREATE TEMP TABLE {name} AS SELECT * FROM main.{name} WHERE 0"
        )


def _conclusion_at(
    conn: sqlite3.Connection, sample_id: str, at_seq: int
) -> dict[str, Any]:
    """The compared conclusion fields for a sample at one business point.

    Mirrors ``engine.sample_report`` for exactly the fields the audit
    compares: sample status, exact inventory, per-source precise
    contributions, releasable quantity and the blocking source ids.
    """
    state = sample_state(conn, sample_id, at_seq=at_seq)
    touch = latest_sample_touch(conn, sample_id, at_or_before_seq=at_seq)
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
    release = release_assessment(conn, sample_id, state, status, at_seq=at_seq)
    return {
        "status": status,
        "available_quantity": q_to_str(state["quantity"]),
        "precise_contributions": {
            original_id: q_to_str(amount)
            for original_id, amount in sorted(state["composition"].items())
        },
        "releasable_quantity": q_to_str(release["releasable_quantity"]),
        "quarantined_source_ids": list(release["quarantined_source_ids"]),
    }


def _breakpoints(
    conn: sqlite3.Connection, sample_id: str, from_seq: int, to_seq: int
) -> list[int]:
    """Business points inside ``(from_seq, to_seq)`` where the conclusion may
    change: an effect on the sample, a touch of the sample, or an effective
    quarantine/release record takes effect there."""
    points: set[int] = set()
    rows = conn.execute(
        "SELECT DISTINCT seq FROM effect WHERE sample_id = ? AND seq > ? AND seq < ?",
        (sample_id, from_seq, to_seq),
    ).fetchall()
    points.update(int(row["seq"]) for row in rows)
    rows = conn.execute(
        """
        SELECT DISTINCT seq FROM event_sample_ref
        WHERE sample_id = ? AND seq > ? AND seq < ?
        """,
        (sample_id, from_seq, to_seq),
    ).fetchall()
    points.update(int(row["seq"]) for row in rows)
    rows = conn.execute(
        """
        SELECT DISTINCT q.seq FROM quarantine_record q
        JOIN event_head h ON h.event_id = q.event_id AND h.current_rev = q.rev
        JOIN event_derivation d ON d.event_id = q.event_id AND d.status = 'valid'
        WHERE q.seq > ? AND q.seq < ?
        """,
        (from_seq, to_seq),
    ).fetchall()
    points.update(int(row["seq"]) for row in rows)
    return sorted(points)


def _segments(
    conn: sqlite3.Connection, sample_id: str, from_seq: int, to_seq: int
) -> list[tuple[int, int, dict[str, Any]]]:
    """Piecewise-constant conclusions over ``[from_seq, to_seq)``.

    Only the breakpoints are evaluated, never every business point in the
    interval.  An event at seq ``s`` is in effect at point ``s``, so the
    conclusion for ``[a, b)`` is the as-of interpretation at ``a``.
    """
    points = [from_seq, *_breakpoints(conn, sample_id, from_seq, to_seq), to_seq]
    return [
        (points[i], points[i + 1], _conclusion_at(conn, sample_id, points[i]))
        for i in range(len(points) - 1)
    ]


def _replay_segments(
    conn: sqlite3.Connection,
    sample_id: str,
    heads: dict[str, int],
    writes: dict[str, list[tuple[int, int]]],
    cutoff: int,
    from_seq: int,
    to_seq: int,
) -> list[tuple[int, int, dict[str, Any]]]:
    _drop_shadow(conn)
    try:
        _build_shadow(conn, heads, writes, cutoff)
        # Re-derive the whole relevant closure in business-seq order with the
        # stock engine; every read/write lands on the TEMP shadow tables.
        recompute_closure(conn, sorted(heads))
        return _segments(conn, sample_id, from_seq, to_seq)
    finally:
        _drop_shadow(conn)
        # Seal the temp-table work so no implicit transaction is left open on
        # the caller's connection (temp writes never touch the real database).
        if conn.in_transaction:
            conn.commit()


def _explode(
    segments: list[tuple[int, int, dict[str, Any]]], bounds: list[int]
) -> dict[int, dict[str, Any]]:
    """Conclusion of each atomic interval, keyed by its start bound."""
    conclusions: dict[int, dict[str, Any]] = {}
    index = 0
    for bound in bounds[:-1]:
        while not (segments[index][0] <= bound < segments[index][1]):
            index += 1
        conclusions[bound] = segments[index][2]
    return conclusions


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def sample_diff_between_writes(
    conn: sqlite3.Connection,
    sample_id: Any,
    from_write_id: Any,
    to_write_id: Any,
    from_seq: Any,
    to_seq: Any,
) -> dict[str, Any]:
    """Diff a sample's ledger conclusions between two write cutoffs.

    ``from_write_id``/``to_write_id`` are ``ledger_write.write_id`` values
    given earliest first (they may be equal); each cutoff includes that write
    and 0 means the empty ledger.  The business interval is
    ``[from_seq, to_seq)``.  Returns the envelope with only the segments
    where the two interpretations differ, in ascending business-seq order;
    adjacent boundaries are merged when both sides' conclusions are
    respectively unchanged across them.
    """
    if not isinstance(sample_id, str) or not sample_id.strip():
        raise ValidationRejected("sample_id must be a non-empty string")
    sample_id = sample_id.strip()
    from_write_id = _require_non_negative_int(from_write_id, "from_write_id")
    to_write_id = _require_non_negative_int(to_write_id, "to_write_id")
    if from_write_id > to_write_id:
        raise ValidationRejected(
            "write cutoffs must be given earliest first: "
            f"from_write_id {from_write_id} > to_write_id {to_write_id}"
        )
    from_seq = _require_non_negative_int(from_seq, "from_seq")
    to_seq = _require_non_negative_int(to_seq, "to_seq")
    if from_seq >= to_seq:
        raise ValidationRejected(
            "business seq range must be an increasing non-negative integer "
            f"range: from_seq {from_seq} must be < to_seq {to_seq}"
        )
    for cutoff in (from_write_id, to_write_id):
        if cutoff == 0:
            continue
        row = conn.execute(
            "SELECT 1 FROM ledger_write WHERE write_id = ?", (cutoff,)
        ).fetchone()
        if row is None:
            raise ValidationRejected(
                f"write cutoff {cutoff} does not exist in this ledger"
            )

    result: dict[str, Any] = {
        "sample_id": sample_id,
        "from_write_id": from_write_id,
        "to_write_id": to_write_id,
        "seq_start": from_seq,
        "seq_end": to_seq,
        "changed": False,
        "segments": [],
    }

    events, _family_samples = _candidate_closure(conn, sample_id)
    writes = _commit_map(conn, events, to_write_id)
    heads_before = _heads_at(writes, from_write_id)
    heads_after = _heads_at(writes, to_write_id)
    if heads_before == heads_after:
        # No commit between the cutoffs touches the sample's closure, so the
        # two interpretations are identical.
        return result

    before_segments = _replay_segments(
        conn, sample_id, heads_before, writes, from_write_id, from_seq, to_seq
    )
    after_segments = _replay_segments(
        conn, sample_id, heads_after, writes, to_write_id, from_seq, to_seq
    )

    bounds = sorted(
        {from_seq, to_seq}
        | {a for a, _, _ in before_segments}
        | {b for _, b, _ in before_segments}
        | {a for a, _, _ in after_segments}
        | {b for _, b, _ in after_segments}
    )
    before_map = _explode(before_segments, bounds)
    after_map = _explode(after_segments, bounds)

    merged: list[dict[str, Any]] = []
    for i in range(len(bounds) - 1):
        a, b = bounds[i], bounds[i + 1]
        before, after = before_map[a], after_map[a]
        if before == after:
            continue
        if (
            merged
            and merged[-1]["to_seq"] == a
            and merged[-1]["before"] == before
            and merged[-1]["after"] == after
        ):
            merged[-1]["to_seq"] = b
        else:
            merged.append(
                {"from_seq": a, "to_seq": b, "before": before, "after": after}
            )
    result["segments"] = merged
    result["changed"] = bool(merged)
    return result
