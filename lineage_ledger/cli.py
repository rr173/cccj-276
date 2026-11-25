"""Command line interface for the persistent lineage ledger."""
from __future__ import annotations

import argparse
import json
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any

from . import ConflictRejected, Ledger, ValidationRejected


def _print_json(value: Any) -> None:
    json.dump(value, sys.stdout, ensure_ascii=False, indent=2, sort_keys=True, default=str)
    sys.stdout.write("\n")


def _load_event(path: str) -> dict[str, Any]:
    text = Path(path).read_text(encoding="utf-8")
    return json.loads(text, parse_float=Decimal)


def _sample_text(report: dict[str, Any]) -> str:
    lines = [
        f"样本 {report['sample_id']}"
        + (f" @seq<={report['at_seq']}" if report["at_seq"] is not None else " 当前"),
        f"状态: {report['status']}",
        f"可用量: {report['available_quantity']} (基础单位, 精确有理数)",
        "当前余量中的原始来源贡献:",
    ]
    if report["precise_contributions"]:
        for item in report["precise_contributions"]:
            lines.append(f"  - {item['original_id']}: {item['amount']}")
    else:
        lines.append("  - 无正余量贡献")
    lines.append("历史累计进入贡献(耗尽后仍保留):")
    if report["historical_input_contributions"]:
        for item in report["historical_input_contributions"]:
            lines.append(f"  - {item['original_id']}: {item['entered_amount']}")
    else:
        lines.append("  - 无")
    if report.get("latest_touch") and report["latest_touch"].get("status") == "blocked":
        touch = report["latest_touch"]
        lines.append(
            "阻断: "
            f"{touch['event_id']} rev{touch['rev']} seq{touch['seq']} "
            f"{touch.get('reason_code')}"
        )
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="lineage-ledger")
    sub = parser.add_subparsers(dest="command", required=True)

    p_unit = sub.add_parser("register-unit", help="register exact unit factor to BASE")
    p_unit.add_argument("db")
    p_unit.add_argument("unit")
    p_unit.add_argument("factor", help="e.g. 6 or 1/3; factor * unit = BASE quantity")
    p_unit.add_argument("--note", default="")

    p_submit = sub.add_parser("submit", help="submit one event JSON file")
    p_submit.add_argument("db")
    p_submit.add_argument("event_file")

    p_sample = sub.add_parser("sample", help="show exact availability and contributions")
    p_sample.add_argument("db")
    p_sample.add_argument("sample_id")
    p_sample.add_argument("--at-seq", type=int)
    p_sample.add_argument("--json", action="store_true")

    p_add = sub.add_parser("add-citation", help="record a detection/reference")
    p_add.add_argument("db")
    p_add.add_argument("citation_id")
    p_add.add_argument("sample_id")
    p_add.add_argument("at_seq", type=int)
    p_add.add_argument("--claimed-amount")
    p_add.add_argument("--metadata-json", default="{}")

    p_show = sub.add_parser("citation", help="show one citation explanation version")
    p_show.add_argument("db")
    p_show.add_argument("citation_id")
    p_show.add_argument("--version", type=int)
    p_show.add_argument("--json", action="store_true")

    p_versions = sub.add_parser("citation-versions")
    p_versions.add_argument("db")
    p_versions.add_argument("citation_id")

    p_events = sub.add_parser("events")
    p_events.add_argument("db")

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "register-unit":
            with Ledger.open(args.db) as ledger:
                result = ledger.register_unit(args.unit, args.factor, args.note)
            _print_json(result)
            return 0

        if args.command == "submit":
            event = _load_event(args.event_file)
            with Ledger.open(args.db) as ledger:
                result = ledger.submit_event(event)
            _print_json(result)
            return 0

        if args.command == "sample":
            with Ledger.open(args.db) as ledger:
                report = ledger.sample(args.sample_id, args.at_seq)
            if args.json:
                _print_json(report)
            else:
                print(_sample_text(report))
            return 0

        if args.command == "add-citation":
            metadata = json.loads(args.metadata_json)
            with Ledger.open(args.db) as ledger:
                result = ledger.add_citation(
                    args.citation_id,
                    args.sample_id,
                    args.at_seq,
                    args.claimed_amount,
                    metadata,
                )
            _print_json(
                {
                    "citation_id": result["citation_id"],
                    "version": result["version"],
                    "status": result["status"],
                    "explanation": result["evaluation"]["explanation_text"],
                }
            )
            return 0

        if args.command == "citation":
            with Ledger.open(args.db) as ledger:
                row = ledger.get_citation(args.citation_id, args.version)
            if args.json:
                _print_json(row)
            else:
                print(row["explanation_text"])
            return 0

        if args.command == "citation-versions":
            with Ledger.open(args.db) as ledger:
                rows = ledger.list_citation_versions(args.citation_id)
            _print_json(rows)
            return 0

        if args.command == "events":
            with Ledger.open(args.db) as ledger:
                rows = ledger.list_events()
            _print_json(rows)
            return 0

        return 2
    except (ValidationRejected, ConflictRejected, FileNotFoundError, KeyError) as exc:
        print(f"error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
