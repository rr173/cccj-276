"""Detection/reference evaluation and versioned restatement."""
from __future__ import annotations

import json
import sqlite3
from typing import Any

from .engine import now_iso, sample_report
from .rational_utils import q_to_str


def _text_report(evaluation: dict[str, Any]) -> str:
    lines: list[str] = []
    c = evaluation["citation"]
    r = evaluation["sample_report"]
    lines.append(f"检测引用 {c['citation_id']} / 样本 {c['sample_id']} / 业务时点 seq<={c['at_seq']}")
    lines.append(f"结论：{evaluation['status_cn']}（{evaluation['status']}）")
    lines.append(f"当时有效可用量：{r['available_quantity']} 基础单位")
    if c.get("claimed_amount") is not None:
        lines.append(f"引用声称/核定量：{c['claimed_amount']} 基础单位")
    if r["precise_contributions"]:
        lines.append("原始样本精确贡献：")
        for item in r["precise_contributions"]:
            lines.append(f"  - {item['original_id']}: {item['amount']}")
    else:
        lines.append("原始样本精确贡献：无有效成立来源")
    if evaluation["reason_code"]:
        lines.append(f"待核原因：{evaluation['reason_code']}")
    blocked = r.get("latest_touch")
    if blocked and blocked.get("status") == "blocked":
        lines.append(
            "阻断事件："
            f"{blocked['event_id']} rev{blocked['rev']} seq{blocked['seq']} "
            f"({blocked.get('reason_code')})"
        )
    if r.get("evidence"):
        lines.append("证据事件（按业务序号）：")
        for ev in r["evidence"]:
            lines.append(
                f"  - seq={ev.get('seq')} {ev.get('event_id')} "
                f"rev={ev.get('rev')} status={ev.get('status')}"
            )
    lines.append("说明：后续耗尽不会改写本时点的历史解释；修订会新增解释版本并保留旧版。")
    return "\n".join(lines)


def evaluate_citation(conn: sqlite3.Connection, citation: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    c = dict(citation)
    at_seq = int(c["at_seq"])
    report = sample_report(conn, c["sample_id"], at_seq=at_seq)

    claimed = None
    if c.get("claimed_num") is not None and c.get("claimed_den") is not None:
        from fractions import Fraction

        claimed = Fraction(int(c["claimed_num"]), int(c["claimed_den"]))

    quantity = report["available_quantity"]
    from fractions import Fraction

    qty = Fraction(int(report["available_numerator"]), int(report["available_denominator"]))
    comp_sum = sum(
        (Fraction(int(x["numerator"]), int(x["denominator"])) for x in report["precise_contributions"]),
        Fraction(0, 1),
    )

    checks = {
        "sample_formed": report["formed"],
        "quantity_positive": qty > 0,
        "composition_matches_quantity": comp_sum == qty,
        "no_blocked_touch_at_or_before_time": not (
            report.get("latest_touch")
            and report["latest_touch"].get("status") == "blocked"
        ),
        "claimed_amount_available": True if claimed is None else qty >= claimed,
    }

    reason_code = None
    if report["status"] == "blocked" or not checks["no_blocked_touch_at_or_before_time"]:
        status = "pending"
        reason_code = "blocked_predecessor_at_reference_time"
    elif not report["formed"] or report["status"] == "not_formed":
        status = "pending"
        reason_code = "sample_not_formed_at_reference_time"
    elif qty <= 0:
        status = "pending"
        reason_code = "no_quantity_at_reference_time"
    elif claimed is not None and qty < claimed:
        status = "pending"
        reason_code = "claimed_amount_exceeds_available_at_reference_time"
    elif not checks["composition_matches_quantity"]:
        status = "pending"
        reason_code = "source_composition_inconsistent"
    else:
        status = "traceable"

    evaluation = {
        "status": status,
        "status_cn": "可追溯" if status == "traceable" else "待核",
        "reason_code": reason_code,
        "checks": checks,
        "claimed_amount": q_to_str(claimed),
        "citation": {
            "citation_id": c["citation_id"],
            "sample_id": c["sample_id"],
            "at_seq": at_seq,
            "claimed_amount": q_to_str(claimed),
            "metadata": json.loads(c.get("metadata_json") or "{}"),
        },
        "sample_report": report,
    }
    evaluation["explanation_text"] = _text_report(evaluation)
    return evaluation


def append_explanation(
    conn: sqlite3.Connection,
    citation_id: str,
    trigger_event_id: str | None = None,
    trigger_rev: int | None = None,
) -> dict[str, Any]:
    citation = conn.execute(
        "SELECT * FROM citation WHERE citation_id = ?", (citation_id,)
    ).fetchone()
    if citation is None:
        raise KeyError(citation_id)

    evaluation = evaluate_citation(conn, citation)
    next_version = conn.execute(
        "SELECT COALESCE(MAX(version), 0) + 1 AS v FROM citation_explanation WHERE citation_id = ?",
        (citation_id,),
    ).fetchone()["v"]

    report_json = json.dumps(evaluation, ensure_ascii=False, sort_keys=True, default=str)
    q_num = evaluation["sample_report"]["available_numerator"]
    q_den = evaluation["sample_report"]["available_denominator"]
    conn.execute(
        """
        INSERT INTO citation_explanation(
            citation_id, version, trigger_event_id, trigger_rev,
            status, traceable, as_of_seq, available_num, available_den,
            reason_code, report_json, explanation_text, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            citation_id,
            next_version,
            trigger_event_id,
            trigger_rev,
            evaluation["status"],
            1 if evaluation["status"] == "traceable" else 0,
            citation["at_seq"],
            q_num,
            q_den,
            evaluation["reason_code"],
            report_json,
            evaluation["explanation_text"],
            now_iso(),
        ),
    )
    conn.execute(
        "UPDATE citation SET current_version = ? WHERE citation_id = ?",
        (next_version, citation_id),
    )
    return {
        "citation_id": citation_id,
        "version": next_version,
        "status": evaluation["status"],
        "traceable": evaluation["status"] == "traceable",
        "evaluation": evaluation,
    }
