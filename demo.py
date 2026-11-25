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
    heading("6. 关闭后重开：持久结果与旧解释均在")
    reopened = Ledger.open(path)
    show_sample(reopened, "C", at_seq=4)
    print("重开后 DET-R 当前状态:", reopened.get_citation("DET-R")["status"])
    print("重开后 DET-R v1 状态:", reopened.get_citation("DET-R", 1)["status"])
    print("重开后事件数:", len(reopened.list_events()))
    reopened.close()

    print("\n全部示例断言通过。")
    print(f"可使用 CLI 查询，例如：python3 -m lineage_ledger sample {path} C")
    return 0


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "/workspace/generated_example.sqlite3"
    raise SystemExit(main(target))
