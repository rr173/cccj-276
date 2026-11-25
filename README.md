# 持久样本谱系台账（lineage-ledger）

纯 Python 标准库 + SQLite 实现。所有数量在库内以任意精度整数分子/分母保存，计算使用
`fractions.Fraction`，不会提前转 float 或四舍五入。

## 运行

```bash
cd /workspace

# 运行全部测试
python3 -m unittest discover -s tests -v

# 运行完整业务示例（合并再拆分、迟到前序、修订超量、独立谱系不变、重开持久库）
python3 demo.py /tmp/lineage_demo.sqlite3

# CLI 查询
python3 -m lineage_ledger sample /tmp/lineage_demo.sqlite3 C
python3 -m lineage_ledger citation /tmp/lineage_demo.sqlite3 DET-R --version 1
python3 -m lineage_ledger citation-versions /tmp/lineage_demo.sqlite3 DET-R
python3 -m lineage_ledger events /tmp/lineage_demo.sqlite3
```

CLI 提交事件：

```bash
python3 -m lineage_ledger submit /path/to/ledger.sqlite3 /path/to/event.json
```

## 核心口径

- **原始样本**：`type="original"`，`base_amount` 必须是正整数基础单位。
- **单位换算**：`register_unit("BOX", "1/6")` 表示 `1 BOX = 1/6 BASE`；比例按精确有理数持久化。
- **业务序号 `seq`**：全局唯一，解释和 as-of 查询均按 `seq` 排序。
- **稳定事件 ID + 修订号 `rev`**：
  - `(event_id, rev)` 相同且规范化内容相同：返回 `duplicate_noop`，不写新版本；
  - `(event_id, rev)` 相同但内容不同：拒绝；
  - 同 `event_id` 的更高 `rev`：整份替换当前解释，旧 payload 与旧检测解释保留；
  - 业务序号对同一稳定事件 ID 保持稳定；修订纠正的是该业务时点的操作内容。
- **依赖**：`dependencies=[{"event_id":"E0","seq":9}]`，声明序号必须小于本事件 `seq`。
  未知依赖可先按声明序号入库；前序到达后经索引反向依赖激活。
- **均匀混合**：若当前量为 `Q`、原始来源 `o` 的绝对贡献为 `C_o`，取出 `x` 时，
  该来源被取出 `x * C_o / Q`，剩余 `C_o - x*C_o/Q`。
- **阻断传播**：数量不足、前置样本未形成、显式依赖无效或失效事件屏障后的后续同谱系事件
  均为 `blocked`，不写 effect；独立样本/谱系不在受影响闭包内，继续有效。
- **不得重根**：只要某样本 ID 在任意事件版本中被声明为拆分/合并/转移的派生接收样本，
  再把它登记为 `original` 会被阻断为 `derived_sample_cannot_be_original`，不会把失效事件
  的后代洗成新原始样本。
- **检测引用**：保存 `sample_id` 与 `at_seq` 业务时点。查询 `seq <= at_seq` 的有效 effect。
  后续耗尽只影响更晚时点的引用；旧解释版本不可变。修订相关事件后，受影响且
  `at_seq >= 修订 seq` 的检测会追加新版本，旧版本保留。
- **原子性**：一次提交中的事件版本、当前头、引用/声明、局部重算、检测重述和
  `ledger_write` 在同一个 SQLite 事务中提交；重开不会得到半版派生结果。

## Python API

```python
from lineage_ledger import Ledger

ledger = Ledger.open("lineage.sqlite3")  # 或 ":memory:"
ledger.register_unit("BOX", "6")        # 1 BOX = 6 BASE

ledger.submit_event({
    "event_id": "O-A", "seq": 1, "rev": 1,
    "type": "original", "sample_id": "A", "base_amount": 3,
})
ledger.submit_event({
    "event_id": "M-1", "seq": 3, "rev": 1,
    "type": "merge",
    "sources": [
        {"sample_id": "A", "amount": 1},
        {"sample_id": "B", "amount": 2},
    ],
    "output": "MIX",
})
ledger.submit_event({
    "event_id": "S-1", "seq": 4, "rev": 1,
    "type": "split",
    "source": "MIX",
    "outputs": [{"sample_id": "C", "amount": 1}],  # 可带 "unit": "BOX"
})

ledger.add_citation("DET-1", "C", at_seq=4)
print(ledger.sample("C"))
print(ledger.get_citation("DET-1")["explanation_text"])
ledger.close()
```

数量可写整数、`"1/3"`、`"0.25"`（精确解析为 `1/4`）或
`{"num": 1, "den": 3}`。为避免二进制误差，Python API 不接受裸 `float`。

## 样本报告字段

`Ledger.sample("C", at_seq=4)` 返回：

- `status`: `available | exhausted | blocked | not_formed | invalid_negative`
- `available_quantity`: 当前/时点有效 effect 合计的精确有理数字符串
- `precise_contributions`: 当前正余量中各 `original_id` 的绝对贡献
- `historical_input_contributions`: 累计进入该容器的正来源量；完全弃置后仍可显示“曾含什么”
- `latest_touch`: 最近事件及其有效/阻断原因
- `evidence`: 沿 `event_upstream` 递归得到的事件证据链

当 `status="blocked"` 时，`available_quantity` 是阻断屏障前最后一组有效 effect 的余额；
`status` 明确表示该业务链尚不能继续签发可靠追溯。

## 增量与查询规模

提交或修订时不会 `SELECT * FROM event_version` 重放全库：

1. 从稳定事件 ID 出发；
2. 经 `event_sample_ref(sample_id)` 找更大业务序号的同容器后继；
3. 经 `event_dependency(depends_on_event_id)` 找显式依赖后继；
4. 只对该受影响闭包按 `seq` 局部重算；
5. 只按样本索引重述相关检测。

样本状态通过 `effect(sample_id, seq)`、`effect_source(sample_id)` 聚合，工作量随该样本
接触历史和来源数量增长，不随无关谱系数量增长。

## 主要表

- `event_head` / `event_version`：当前修订与不可变历史 payload/hash
- `event_sample_ref` / `event_dependency`：版本化样本引用和显式依赖
- `sample_identity_claim`：原始/派生身份的永久声明
- `event_derivation`：当前头推导状态与阻断原因
- `effect` / `effect_source`：带符号精确数量/来源增量
- `event_upstream`：当前解释的局部证据边
- `citation` / `citation_explanation`：检测引用及不可变解释版本
