"""Command line interface for the persistent lineage ledger."""
from __future__ import annotations

import argparse
import json
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any

from . import ConflictRejected, Ledger, ValidationRejected
from .audit import sample_diff_between_writes
from .db import connect_readonly


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
        f"实际库存(可用量): {report['available_quantity']} (基础单位, 精确有理数)",
        f"当前可发放量: {report['releasable_quantity']}"
        + ("（可发放）" if report["releasable"] else "（不可发放）"),
    ]
    if not report["releasable"]:
        lines.append(f"不可发放原因: {report['release_block_reason']}")
        if report.get("blocked_sources"):
            lines.append("导致不可发放的原始来源:")
            for item in report["blocked_sources"]:
                lines.append(
                    f"  - {item['original_id']}: 当前贡献 {item['amount']}"
                    f"（隔离事件 {item['quarantine_event_id']} "
                    f"rev{item['quarantine_rev']} seq{item['quarantine_seq']}）"
                )
    lines.append("当前余量中的原始来源贡献:")
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


def _reservable_text(report: dict[str, Any]) -> str:
    lines = [
        f"样本 {report['sample_id']} 预约容量",
        f"实际库存: {report['available_quantity']} 基础单位",
        f"当前可发放量: {report['releasable_quantity']}"
        + ("（可发放）" if report["releasable"] else f"（不可发放: {report['release_block_reason']}）"),
        f"已预约未履约量: {report['reserved_quantity']}",
        f"剩余可预约量: {report['bookable_quantity']}",
        f"预约是否全部被库存覆盖: {'是' if report['fully_covered'] else '否'}",
    ]
    if report["uncovered_total"] != "0":
        lines.append(f"未覆盖总量: {report['uncovered_total']}")
        lines.append("受影响预约:")
        for item in report["affected_reservations"]:
            lines.append(
                f"  - {item['request_id']}: 剩余 {item['remaining_amount']}，"
                f"未覆盖 {item['uncovered_quantity']}"
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

    p_q = sub.add_parser(
        "quarantine", help="record a source quarantine (or --release)"
    )
    p_q.add_argument("db")
    p_q.add_argument("event_id")
    p_q.add_argument("seq", type=int)
    p_q.add_argument("source_id")
    p_q.add_argument("--rev", type=int, default=1)
    p_q.add_argument("--reason", default="")
    p_q.add_argument("--release", action="store_true")

    p_qs = sub.add_parser("quarantine-status", help="show effective source records")
    p_qs.add_argument("db")
    p_qs.add_argument("source_id", nargs="?")
    p_qs.add_argument("--at-seq", type=int)
    p_qs.add_argument(
        "--history", action="store_true", help="include superseded revisions"
    )

    p_res = sub.add_parser(
        "reserve", help="reserve an exact amount of releasable sample"
    )
    p_res.add_argument("db")
    p_res.add_argument("request_id")
    p_res.add_argument("sample_id")
    p_res.add_argument("amount", help="exact BASE quantity, e.g. 2 or 1/3")
    p_res.add_argument("--note", default="")
    p_res.add_argument("--metadata-json", default="{}")

    p_cancel = sub.add_parser(
        "reservation-cancel", help="cancel part or all of an open reservation"
    )
    p_cancel.add_argument("db")
    p_cancel.add_argument("request_id")
    p_cancel.add_argument("--amount", help="omit to cancel the full remaining amount")
    p_cancel.add_argument("--operation-id", required=True)
    p_cancel.add_argument("--reason", default="")

    p_fulfill = sub.add_parser(
        "reservation-fulfill",
        help="register an actual withdrawal against a reservation",
    )
    p_fulfill.add_argument("db")
    p_fulfill.add_argument("request_id")
    p_fulfill.add_argument("seq", type=int)
    p_fulfill.add_argument("amount")
    p_fulfill.add_argument("--operation-id", required=True)
    p_fulfill.add_argument("--withdrawal-event-id")
    p_fulfill.add_argument("--reason", default="")

    p_book = sub.add_parser(
        "reservable", help="show remaining quantity open for reservation"
    )
    p_book.add_argument("db")
    p_book.add_argument("sample_id")
    p_book.add_argument("--json", action="store_true")

    p_list = sub.add_parser("reservations", help="list reservations")
    p_list.add_argument("db")
    p_list.add_argument("--sample-id")
    p_list.add_argument(
        "--status", choices=["active", "fulfilled", "cancelled", "closed"]
    )
    p_list.add_argument("--no-coverage", action="store_true")

    p_get = sub.add_parser("reservation", help="show one reservation with operations")
    p_get.add_argument("db")
    p_get.add_argument("request_id")

    p_cov = sub.add_parser(
        "reservation-coverage",
        help="report reservations uncovered by later stock changes",
    )
    p_cov.add_argument("db")
    p_cov.add_argument("sample_id", nargs="?")

    p_audit = sub.add_parser(
        "audit-diff",
        help=(
            "read-only: list the business-seq segments in [from_seq, to_seq) "
            "where a sample's conclusions differ between two write cutoffs"
        ),
    )
    p_audit.add_argument("db")
    p_audit.add_argument("sample_id")
    p_audit.add_argument(
        "from_write_id",
        type=int,
        help="earlier write cutoff (ledger_write.write_id, inclusive; 0 = empty ledger)",
    )
    p_audit.add_argument(
        "to_write_id",
        type=int,
        help="later write cutoff (ledger_write.write_id, inclusive)",
    )
    p_audit.add_argument("from_seq", type=int, help="business interval start (inclusive)")
    p_audit.add_argument("to_seq", type=int, help="business interval end (exclusive)")

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

        if args.command == "quarantine":
            with Ledger.open(args.db) as ledger:
                result = ledger.record_source_event(
                    args.event_id,
                    args.seq,
                    args.source_id,
                    "release" if args.release else "quarantine",
                    rev=args.rev,
                    reason=args.reason,
                )
            _print_json(result)
            return 0

        if args.command == "quarantine-status":
            with Ledger.open(args.db) as ledger:
                if args.history:
                    rows = ledger.source_action_history(args.source_id)
                elif args.source_id is None:
                    rows = ledger.source_actions(at_seq=args.at_seq)
                else:
                    rows = ledger.source_status(
                        args.source_id, at_seq=args.at_seq
                    )
            _print_json(rows)
            return 0

        if args.command == "reserve":
            metadata = json.loads(args.metadata_json)
            with Ledger.open(args.db) as ledger:
                result = ledger.reserve_sample(
                    args.request_id,
                    args.sample_id,
                    args.amount,
                    note=args.note,
                    metadata=metadata,
                )
            _print_json(result)
            return 0

        if args.command == "reservation-cancel":
            with Ledger.open(args.db) as ledger:
                result = ledger.cancel_reservation(
                    args.request_id,
                    amount=args.amount,
                    operation_id=args.operation_id,
                    reason=args.reason,
                )
            _print_json(result)
            return 0

        if args.command == "reservation-fulfill":
            with Ledger.open(args.db) as ledger:
                result = ledger.fulfill_reservation(
                    args.request_id,
                    args.seq,
                    args.amount,
                    operation_id=args.operation_id,
                    withdrawal_event_id=args.withdrawal_event_id,
                    reason=args.reason,
                )
            _print_json(result)
            return 0

        if args.command == "reservable":
            with Ledger.open(args.db) as ledger:
                report = ledger.reservable_quantity(args.sample_id)
            if args.json:
                _print_json(report)
            else:
                print(_reservable_text(report))
            return 0

        if args.command == "reservations":
            with Ledger.open(args.db) as ledger:
                rows = ledger.list_reservations(
                    sample_id=args.sample_id,
                    status=args.status,
                    with_coverage=not args.no_coverage,
                )
            _print_json(rows)
            return 0

        if args.command == "reservation":
            with Ledger.open(args.db) as ledger:
                row = ledger.get_reservation(args.request_id)
            _print_json(row)
            return 0

        if args.command == "reservation-coverage":
            with Ledger.open(args.db) as ledger:
                rows = ledger.reservation_coverage_report(args.sample_id)
            _print_json(rows)
            return 0

        if args.command == "audit-diff":
            # Strictly read-only: the original database is opened with
            # SQLite's read-only mode and only TEMP tables are used.
            conn = connect_readonly(args.db)
            try:
                result = sample_diff_between_writes(
                    conn,
                    args.sample_id,
                    args.from_write_id,
                    args.to_write_id,
                    args.from_seq,
                    args.to_seq,
                )
            finally:
                conn.close()
            _print_json(result)
            return 0

        return 2
    except (ValidationRejected, ConflictRejected, FileNotFoundError, KeyError) as exc:
        print(f"error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
