#!/usr/bin/env python3
"""Runnable end-to-end example for the persistent lineage ledger.

Run:
    python3 /workspace/demo.py
    python3 /workspace/demo.py /tmp/my-ledger.sqlite3
"""
from __future__ import annotations

import sys
from pathlib import Path

from lineage_ledger import ConflictRejected, Ledger
from lineage_ledger.cli import _sample_text


def heading(title: str) -> None:
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)


def show_sample(ledger: Ledger, sample_id: str, at_seq: int | None = None) -> None:
    print(_sample_text(ledger.sample(sample_id, at_seq)))


def main(db_path: str) -> int:
    path = Path(db_path)
    for suffix in ("", "-wal", "-shm"):
        Path(str(path) + suffix).unlink(missing_ok=True)

    ledger = Ledger.open(path)
    print(f"持久台账: {path}")

    # ------------------------------------------------------------------
    heading("1. 合并再拆分：精确有理来源比例")
    ledger.register_unit("BOX", "6", "1 BOX = 6 BASE")

    for event in [
        {
            "event_id": "O-A",
            "seq": 1,
            "rev": 1,
            "type": "original",
            "sample_id": "A",
            "base_amount": 3,
            "note": "整数基础单位",
        },
        {
            "event_id": "O-B",
            "seq": 2,
            "rev": 1,
            "type": "original",
            "sample_id": "B",
            "base_amount": 5,
        },
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
        },
        {
            "event_id": "S-1",
            "seq": 4,
            "rev": 1,
            "type": "split",
            "source": "MIX",
            "outputs": [
                {"sample_id": "C", "amount": 1},
                {"sample_id": "C2", "amount": "1/2"},
            ],
        },
    ]:
        result = ledger.submit_event(event)
        print(
            f"提交 {event['event_id']}: {result['status']} / "
            f"{result['derivation']['status']}"
        )

    show_sample(ledger, "C")
    c = ledger.sample("C")
    assert c["available_quantity"] == "1"
    contrib = {x["original_id"]: x["amount"] for x in c["precise_contributions"]}
    assert contrib == {"A": "1/3", "B": "2/3"}

    # ------------------------------------------------------------------
    heading("2. 检测引用与时点；之后耗尽不抹去此前来源")
    ledger.add_citation("DET-C-OK", "C", at_seq=4, metadata={"analyst": "lab-demo"})
    print(ledger.get_citation("DET-C-OK")["explanation_text"])

    ledger.submit_event(
        {
            "event_id": "D-C",
            "seq": 5,
            "rev": 1,
            "type": "discard",
            "sample": "C",
            "amount": 1,
        }
    )
    ledger.add_citation("DET-C-AFTER", "C", at_seq=5)
    print("\n-- C 在 seq=5 弃置后的当前台账 --")
    show_sample(ledger, "C")
    print("\n-- seq=5 之后才作的引用：应为待核 --")
    print(ledger.get_citation("DET-C-AFTER")["explanation_text"])
    print("\n-- 旧检测引用 v1 仍保留且可追溯 --")
    old = ledger.get_citation("DET-C-OK", version=1)
    print(old["explanation_text"])
    assert old["status"] == "traceable"
    assert ledger.get_citation("DET-C-AFTER")["status"] == "pending"

    # ------------------------------------------------------------------
    heading("3. 迟到前序：先收到 seq=20 的拆分，后收到 seq=10 的原始样本")
    waiting = ledger.submit_event(
        {
            "event_id": "LATE-SPLIT",
            "seq": 20,
            "rev": 1,
            "type": "split",
            "source": "LATE-ROOT",
            "outputs": [{"sample_id": "LATE-CHILD", "amount": 4}],
        }
    )
    print("迟到前序到达前:", waiting["derivation"]["status"], waiting["derivation"]["reason_code"])
    ledger.add_citation("DET-LATE", "LATE-CHILD", at_seq=20)
    print("引用 v1:", ledger.get_citation("DET-LATE", 1)["status"])

    arrived = ledger.submit_event(
        {
            "event_id": "LATE-ORIGIN",
            "seq": 10,
            "rev": 1,
            "type": "original",
            "sample_id": "LATE-ROOT",
            "base_amount": 10,
        }
    )
    print("补到前序后重算谱系:", arrived["affected_event_ids"])
    print("受影响检测新版本:", arrived["restated_citations"])
    print("DET-LATE 版本史:", ledger.list_citation_versions("DET-LATE"))
    show_sample(ledger, "LATE-CHILD")
    assert ledger.get_citation("DET-LATE")["status"] == "traceable"

    # ------------------------------------------------------------------
    heading("4. 修订致下游超量；旧解释保留，新解释待核")
    ledger.submit_event(
        {
            "event_id": "R-ORIGIN",
            "seq": 100,
            "rev": 1,
            "type": "original",
            "sample_id": "R-A",
            "base_amount": 10,
        }
    )
    ledger.submit_event(
        {
            "event_id": "R-SPLIT",
            "seq": 110,
            "rev": 1,
            "type": "split",
            "source": "R-A",
            "outputs": [{"sample_id": "R-CHILD", "amount": 6}],
        }
    )
    ledger.add_citation("DET-R", "R-CHILD", at_seq=110)
    print("修订前 DET-R:", ledger.get_citation("DET-R")["status"])

    # Independent lineage, placed at its own sequences.
    ledger.submit_event(
        {
            "event_id": "I-ORIGIN",
            "seq": 150,
            "rev": 1,
            "type": "original",
            "sample_id": "I",
            "base_amount": 7,
        }
    )
    ledger.add_citation("DET-I", "I", at_seq=150)

    revised = ledger.submit_event(
        {
            "event_id": "R-ORIGIN",
            "seq": 100,
            "rev": 2,
            "type": "original",
            "sample_id": "R-A",
            "base_amount": 5,
            "note": "实验室复核：原始整数基础量由10修订为5",
        }
    )
    print("修订后仅重算:", revised["affected_event_ids"])
    print("修订后重述检测:", revised["restated_citations"])
    print("R-SPLIT 推导:", ledger._derivation("R-SPLIT")["status"],
          ledger._derivation("R-SPLIT")["reason_code"])
    show_sample(ledger, "R-CHILD")
    print("DET-R 版本史:", ledger.list_citation_versions("DET-R"))
    print("独立谱系 DET-I 版本史:", ledger.list_citation_versions("DET-I"))
    assert ledger.get_citation("DET-R", 1)["status"] == "traceable"
    assert ledger.get_citation("DET-R", 2)["status"] == "pending"
    assert ledger.get_citation("DET-I")["version"] == 1
    assert ledger.sample("I")["available_quantity"] == "7"

    # ------------------------------------------------------------------
    heading("5. 幂等重送与同版本异内容拒绝")
    duplicate = ledger.submit_event(
        {
            "event_id": "R-ORIGIN",
            "seq": 100,
            "rev": 2,
            "type": "original",
            "sample_id": "R-A",
            "base_amount": 5,
            "note": "实验室复核：原始整数基础量由10修订为5",
        }
    )
    print("完全相同重送:", duplicate["status"])
    assert duplicate["status"] == "duplicate_noop"
    try:
        ledger.submit_event(
            {
                "event_id": "R-ORIGIN",
                "seq": 100,
                "rev": 2,
                "type": "original",
                "sample_id": "R-A",
                "base_amount": 999,
            }
        )
    except ConflictRejected as exc:
        print("同ID同修订号异内容: 已拒绝 ->", exc)
    else:
        raise AssertionError("conflicting revision was not rejected")

    ledger.close()

    # ------------------------------------------------------------------
    heading("6. 来源隔离与解除：实际库存 vs 当前可发放量")
    quarantine_demo(path)

    # ------------------------------------------------------------------
    heading("7. 样本预约与履约：只占容量、不动实物，缺口显式报告")
    reservation_demo(path)

    # ------------------------------------------------------------------
    heading("8. 关闭后重开：持久结果与旧解释均在")
    reopened = Ledger.open(path)
    show_sample(reopened, "C", at_seq=4)
    print("重开后 DET-R 当前状态:", reopened.get_citation("DET-R")["status"])
    print("重开后 DET-R v1 状态:", reopened.get_citation("DET-R", 1)["status"])
    print("重开后事件数:", len(reopened.list_events()))
    reopened.close()

    print("\n全部示例断言通过。")
    print(f"可使用 CLI 查询，例如：python3 -m lineage_ledger sample {path} C")
    return 0


def quarantine_demo(db_path: str) -> None:
    """Isolation, release and a corrected source revisal on a small mixture."""
    ledger = Ledger.open(db_path)
    ledger.submit_event(
        {
            "event_id": "QO-X",
            "seq": 200,
            "rev": 1,
            "type": "original",
            "sample_id": "X",
            "base_amount": 3,
        }
    )
    ledger.submit_event(
        {
            "event_id": "QO-Y",
            "seq": 201,
            "rev": 1,
            "type": "original",
            "sample_id": "Y",
            "base_amount": 5,
        }
    )
    ledger.submit_event(
        {
            "event_id": "QM-1",
            "seq": 202,
            "rev": 1,
            "type": "merge",
            "sources": [
                {"sample_id": "X", "amount": 1},
                {"sample_id": "Y", "amount": 2},
            ],
            "output": "QMIX",
        }
    )
    show_sample(ledger, "QMIX")
    assert ledger.sample("QMIX")["releasable"] is True

    print("\n-- X 检出污染风险：隔离来源 X (seq=205) --")
    ledger.quarantine_source("Q-X", 205, "X", reason="X 原始样本污染风险")
    show_sample(ledger, "QMIX")
    mix = ledger.sample("QMIX")
    # Physical quantity is unchanged; the whole homogeneous mixture is barred.
    assert mix["available_quantity"] == "3"
    assert mix["releasable"] is False
    assert mix["releasable_quantity"] == "0"
    assert [b["original_id"] for b in mix["blocked_sources"]] == ["X"]
    # The unrelated Y-only lineage is untouched.
    assert ledger.sample("Y")["releasable"] is True
    print("纯 Y 谱系不受影响:", ledger.sample("Y")["available_quantity"], "可发放")

    print("\n-- 时点查询：隔离前 seq=202 仍可发放 --")
    print("QMIX @202 可发放量:", ledger.sample("QMIX", at_seq=202)["releasable_quantity"])
    print("QMIX @205 可发放量:", ledger.sample("QMIX", at_seq=205)["releasable_quantity"])

    print("\n-- 检测引用因来源事件重述（只影响 at_seq>=205） --")
    ledger.add_citation("DET-Q-202", "QMIX", at_seq=202)
    ledger.add_citation("DET-Q-205", "QMIX", at_seq=205)
    print("DET-Q-202:", ledger.get_citation("DET-Q-202")["status"])
    print("DET-Q-205:", ledger.get_citation("DET-Q-205")["status"],
          ledger.get_citation("DET-Q-205")["reason_code"])

    print("\n-- 修订：污染来源实为 Y，纠正隔离记录 rev2 --")
    revised = ledger.submit_event(
        {
            "event_id": "Q-X",
            "seq": 205,
            "rev": 2,
            "type": "quarantine",
            "source_id": "Y",
            "reason": "复核更正：应为 Y",
        }
    )
    print("重算样本:", revised["affected_samples"], "重述检测:", revised["restated_citations"])
    show_sample(ledger, "QMIX")
    assert ledger.sample("QMIX")["quarantined_source_ids"] == ["Y"]
    assert ledger.sample("X")["releasable"] is True
    assert ledger.get_citation("DET-Q-205", 1)["status"] == "pending"
    assert ledger.get_citation("DET-Q-205")["status"] == "pending"

    print("\n-- 解除最后一个相关隔离来源 Y (seq=210)：实际库存重新可发放 --")
    ledger.add_citation("DET-Q-210", "QMIX", at_seq=210)
    print("解除前 DET-Q-210:", ledger.get_citation("DET-Q-210")["status"])
    ledger.release_source("R-Y", 210, "Y", reason="复核解除")
    show_sample(ledger, "QMIX")
    released = ledger.sample("QMIX")
    assert released["available_quantity"] == "3"
    assert released["releasable"] is True
    assert released["releasable_quantity"] == "3"
    # at_seq=210 sees the release; the earlier at_seq=205 reference keeps the
    # business-time interpretation that Y was still quarantined then.
    assert ledger.get_citation("DET-Q-210")["status"] == "traceable"
    assert ledger.get_citation("DET-Q-205")["status"] == "pending"
    print("DET-Q-210 版本史:", ledger.list_citation_versions("DET-Q-210"))
    print("DET-Q-205 仍为当时判断:",
          ledger.get_citation("DET-Q-205")["status"])

    print("\n来源记录史（含被修订的旧版本）：")
    for record in ledger.source_action_history():
        marker = "当前" if record["is_current"] else "已被修订"
        print(
            f"  - seq={record['seq']} {record['event_id']} rev{record['rev']} "
            f"{record['action']} {record['source_id']} "
            f"[{marker}] ({record['reason']})"
        )
    ledger.close()


def reservation_demo(db_path: str) -> None:
    """Reservation, cancel, fulfillment and explicit shortfall reporting."""
    ledger = Ledger.open(db_path)
    ledger.submit_event(
        {
            "event_id": "RO-Z",
            "seq": 300,
            "rev": 1,
            "type": "original",
            "sample_id": "Z",
            "base_amount": 10,
        }
    )
    print("-- 预约前 Z 实际库存与可预约量 --")
    show_sample(ledger, "Z")

    print("\n-- 方案甲/乙分别预约 4 和 3；预约不写 effect，物理库存不变 --")
    r1 = ledger.reserve_sample("RES-Z-甲", "Z", 4, note="方案甲")
    r2 = ledger.reserve_sample("RES-Z-乙", "Z", 3, note="方案乙")
    print("甲:", r1["request_outcome"], "剩余可预约:",
          ledger.reservable_quantity("Z")["bookable_quantity"])
    assert ledger.sample("Z")["available_quantity"] == "10"
    assert ledger.reservable_quantity("Z")["reserved_quantity"] == "7"

    print("\n-- 重送同一 request_id：幂等 no-op，不重复占量 --")
    retry = ledger.reserve_sample("RES-Z-甲", "Z", 4, note="方案甲")
    assert retry["request_outcome"] == "duplicate_noop"
    print("重送结果:", retry["request_outcome"])
    try:
        ledger.reserve_sample("RES-Z-甲", "Z", 5, note="方案甲改量")
    except ConflictRejected as exc:
        print("同号异内容拒绝 ->", exc)

    print("\n-- 超量预约必须整单拒绝（可预约只剩 3，申请 4）--")
    try:
        ledger.reserve_sample("RES-Z-丙", "Z", 4)
    except ConflictRejected as exc:
        print("整单拒绝 ->", exc)
    assert len(ledger.list_reservations()) == 2

    print("\n-- 方案甲履约 1/2：同一事务写 discard 实物取用并扣减预约剩余量 --")
    fulfilled = ledger.fulfill_reservation(
        "RES-Z-甲", 301, "1/2", operation_id="FUL-甲-1"
    )
    print("履约取用事件:", fulfilled["withdrawal_event"]["event_id"],
          fulfilled["withdrawal_event"]["derivation"]["status"])
    assert ledger.sample("Z")["available_quantity"] == "19/2"
    assert ledger.get_reservation("RES-Z-甲")["remaining_quantity"] == "7/2"

    print("\n-- 取消方案乙的 1 个单位，容量立即释放 --")
    ledger.cancel_reservation("RES-Z-乙", amount=1, operation_id="CAN-乙-1")
    assert ledger.get_reservation("RES-Z-乙")["remaining_quantity"] == "2"

    print("\n-- 预约之外的实际取用导致库存不足：预约原量保留，缺口显式报告 --")
    ledger.submit_event(
        {
            "event_id": "RD-Z",
            "seq": 302,
            "rev": 1,
            "type": "discard",
            "sample": "Z",
            "amount": 8,
        }
    )
    coverage = ledger.reservation_coverage_report("Z")[0]
    print("实际库存:", coverage["available_quantity"],
          "可发放:", coverage["releasable_quantity"],
          "已预约:", coverage["reserved_quantity"])
    print("未覆盖总量:", coverage["uncovered_total"], "原因:", coverage["shortfall_reasons"])
    for item in coverage["affected_reservations"]:
        print(
            f"  受影响预约 {item['request_id']}: 剩余 {item['remaining_amount']}，"
            f"已覆盖 {item['covered_quantity']}，未覆盖 {item['uncovered_quantity']}"
        )
    assert coverage["fully_covered"] is False
    # Reservation amounts were never silently rewritten.
    assert ledger.get_reservation("RES-Z-甲")["remaining_quantity"] == "7/2"
    assert ledger.get_reservation("RES-Z-乙")["remaining_quantity"] == "2"

    print("\n-- 来源隔离同样造成未覆盖，并阻断隔离期履约 --")
    ledger.quarantine_source("RQ-Z", 303, "Z", reason="污染风险")
    blocked = ledger.reservation_coverage_report("Z")[0]
    print("隔离后未覆盖总量:", blocked["uncovered_total"],
          "原因:", blocked["shortfall_reasons"])
    try:
        ledger.fulfill_reservation(
            "RES-Z-乙", 304, 1, operation_id="FUL-乙-BLOCKED"
        )
    except ConflictRejected as exc:
        print("隔离期履约拒绝 ->", exc)
    ledger.release_source("RR-Z", 305, "Z", reason="复核解除")
    print("解除后可发放量:", ledger.sample("Z")["releasable_quantity"])
    ledger.close()


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "/workspace/generated_example.sqlite3"
    raise SystemExit(main(target))
