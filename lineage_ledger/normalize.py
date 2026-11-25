"""Event envelope validation and exact unit normalization."""
from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
from fractions import Fraction
from typing import Any

from .errors import ValidationRejected
from .rational_utils import positive_fraction


def require_sample_id(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationRejected(f"{field} must be a non-empty string")
    return value.strip()


def require_int(value: Any, field: str, *, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationRejected(f"{field} must be an integer")
    if minimum is not None and value < minimum:
        raise ValidationRejected(f"{field} must be >= {minimum}")
    return value


def unit_factor(conn: sqlite3.Connection, unit: Any) -> Fraction:
    if unit is None:
        unit = "BASE"
    if not isinstance(unit, str) or not unit.strip():
        raise ValidationRejected("unit must be a non-empty string")
    unit = unit.strip()
    if unit == "BASE":
        return Fraction(1, 1)
    row = conn.execute(
        "SELECT factor_num, factor_den FROM unit_conversion WHERE unit = ?",
        (unit,),
    ).fetchone()
    if row is None:
        raise ValidationRejected(f"unknown unit conversion: {unit!r}")
    return Fraction(int(row["factor_num"]), int(row["factor_den"]))


def convert_amount(
    conn: sqlite3.Connection,
    holder: dict[str, Any],
    *,
    field: str = "amount",
    default_unit: str = "BASE",
) -> Fraction:
    if not isinstance(holder, dict) or field not in holder:
        raise ValidationRejected(f"missing {field}")
    unit = holder.get("unit", default_unit)
    factor = unit_factor(conn, unit)
    qty = positive_fraction(holder[field], field)
    base = qty * factor
    holder[field] = str(base)
    holder["unit"] = "BASE"
    return base


def normalize_dependencies(
    conn: sqlite3.Connection, raw_deps: Any, seq: int
) -> list[dict[str, Any]]:
    if raw_deps is None:
        raw_deps = []
    if not isinstance(raw_deps, list):
        raise ValidationRejected("dependencies must be a list")
    result: dict[str, dict[str, Any]] = {}
    for dep in raw_deps:
        if isinstance(dep, str):
            eid = dep
            row = conn.execute(
                "SELECT seq FROM event_head WHERE event_id = ?", (eid,)
            ).fetchone()
            if row is None:
                raise ValidationRejected(
                    f"dependency {eid!r} is unknown; use {{event_id, seq}} for a late predecessor"
                )
            dep_seq = int(row["seq"])
        elif isinstance(dep, dict):
            eid = require_sample_id(dep.get("event_id"), "dependency.event_id")
            dep_seq = require_int(dep.get("seq"), "dependency.seq")
            row = conn.execute(
                "SELECT seq FROM event_head WHERE event_id = ?", (eid,)
            ).fetchone()
            if row is not None and int(row["seq"]) != dep_seq:
                raise ValidationRejected(
                    f"dependency {eid!r} declares seq {dep_seq}, actual seq {row['seq']}"
                )
        else:
            raise ValidationRejected("dependency must be an event ID or {event_id, seq}")
        if dep_seq >= seq:
            raise ValidationRejected(
                f"dependency {eid!r} seq {dep_seq} must be smaller than event seq {seq}"
            )
        result[eid] = {"event_id": eid, "seq": dep_seq}
    return [result[k] for k in sorted(result)]


def normalize_event(conn: sqlite3.Connection, event: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(event, dict):
        raise ValidationRejected("event must be an object")
    norm = copy.deepcopy(event)

    event_id = require_sample_id(norm.get("event_id"), "event_id")
    seq = require_int(norm.get("seq"), "seq", minimum=0)
    rev = require_int(norm.get("rev"), "rev", minimum=1)
    event_type = norm.get("type")
    if event_type not in {
        "original",
        "split",
        "merge",
        "transfer",
        "discard",
        "quarantine",
        "release",
    }:
        raise ValidationRejected(f"unsupported event type: {event_type!r}")

    norm["event_id"] = event_id
    norm["seq"] = seq
    norm["rev"] = rev
    norm["dependencies"] = normalize_dependencies(
        conn, norm.get("dependencies"), seq
    )

    default_unit = norm.get("unit", "BASE")
    if not isinstance(default_unit, str):
        raise ValidationRejected("unit must be a string")

    if event_type == "original":
        sample_id = require_sample_id(norm.get("sample_id"), "sample_id")
        base_amount = require_int(norm.get("base_amount"), "base_amount", minimum=1)
        norm["sample_id"] = sample_id
        norm["base_amount"] = base_amount

    elif event_type == "split":
        source = require_sample_id(norm.get("source"), "source")
        outputs = norm.get("outputs")
        if not isinstance(outputs, list) or not outputs:
            raise ValidationRejected("split.outputs must be a non-empty list")
        seen: set[str] = set()
        for i, out in enumerate(outputs):
            if not isinstance(out, dict):
                raise ValidationRejected(f"outputs[{i}] must be an object")
            sid = require_sample_id(out.get("sample_id"), f"outputs[{i}].sample_id")
            if sid in seen:
                raise ValidationRejected(f"duplicate output sample {sid!r}")
            if sid == source:
                raise ValidationRejected("split output must differ from source")
            seen.add(sid)
            out["sample_id"] = sid
            convert_amount(conn, out, default_unit=default_unit)
        norm["source"] = source

    elif event_type == "merge":
        sources = norm.get("sources")
        if not isinstance(sources, list) or len(sources) < 2:
            raise ValidationRejected("merge.sources must contain at least two items")
        seen = set()
        for i, src in enumerate(sources):
            if not isinstance(src, dict):
                raise ValidationRejected(f"sources[{i}] must be an object")
            sid = require_sample_id(src.get("sample_id"), f"sources[{i}].sample_id")
            if sid in seen:
                raise ValidationRejected(f"duplicate merge source {sid!r}")
            seen.add(sid)
            src["sample_id"] = sid
            convert_amount(conn, src, default_unit=default_unit)
        output = require_sample_id(norm.get("output"), "output")
        if output in seen:
            raise ValidationRejected("merge output must differ from every source")
        norm["output"] = output

    elif event_type == "transfer":
        source = require_sample_id(norm.get("source"), "source")
        destination = require_sample_id(norm.get("destination"), "destination")
        if source == destination:
            raise ValidationRejected("transfer source and destination must differ")
        convert_amount(conn, norm, default_unit=default_unit)
        norm["source"] = source
        norm["destination"] = destination

    elif event_type == "discard":
        sample = require_sample_id(norm.get("sample"), "sample")
        convert_amount(conn, norm, default_unit=default_unit)
        norm["sample"] = sample

    elif event_type in {"quarantine", "release"}:
        # A source event is a pure business-time record: it names an original
        # source and never changes any physical sample quantity.
        source_id = require_sample_id(norm.get("source_id"), "source_id")
        reason = norm.get("reason", "")
        if not isinstance(reason, str):
            raise ValidationRejected("reason must be a string")
        norm["source_id"] = source_id
        norm["reason"] = reason.strip()

    # All quantity holders have been normalized to exact BASE fractions.
    if "unit" in norm:
        norm["unit"] = "BASE"
    return norm


def content_hash(normalized_event: dict[str, Any]) -> str:
    encoded = json.dumps(
        normalized_event,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def refs_from_event(event: dict[str, Any]) -> list[tuple[str, str]]:
    """Return ``(sample_id, role)`` pairs."""
    t = event["type"]
    refs: list[tuple[str, str]] = []
    if t == "original":
        refs.append((event["sample_id"], "original"))
    elif t == "split":
        refs.append((event["source"], "source"))
        refs.extend((o["sample_id"], "output") for o in event["outputs"])
    elif t == "merge":
        refs.extend((s["sample_id"], "source") for s in event["sources"])
        refs.append((event["output"], "output"))
    elif t == "transfer":
        refs.append((event["source"], "source"))
        refs.append((event["destination"], "destination"))
    elif t == "discard":
        refs.append((event["sample"], "sample"))
    elif t in {"quarantine", "release"}:
        # A source record is deliberately not a natural sample touch: it must
        # not rewrite the physical lineage chain of any container.
        pass
    return refs


def output_claims_from_event(event: dict[str, Any]) -> list[str]:
    t = event["type"]
    if t == "split":
        return [o["sample_id"] for o in event["outputs"]]
    if t == "merge":
        return [event["output"]]
    if t == "transfer":
        return [event["destination"]]
    return []
