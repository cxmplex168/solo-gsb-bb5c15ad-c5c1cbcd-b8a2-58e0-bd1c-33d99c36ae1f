"""防篡改审计链：异步组卷任务生命周期事件的 SHA-256 哈希链。

每个任务维护一条**独立**事件链，事件按 ``seq = 1, 2, 3, ...`` 连续追加，
只追加、不修改、不删除。每条事件包含序号、时间、类型与最小业务载荷，
事件哈希定义为::

    hash_n = SHA256(prev_hash + canonical_json(envelope))
    envelope = {"seq": n, "time": ..., "type": ..., "payload": ...}

- 首事件的前驱哈希为创世哈希 ``SHA256(b"")``；
- ``canonical_json`` 采用键排序、无多余空白、非 ASCII 不转义的规范化编码，
  同一语义载荷的摘要稳定可复现；
- 哈希提交范围覆盖序号、时间、类型与载荷，任一字段被改动都会导致摘要失配。

校验时逐事件重算，依次检查**序号连续性、前驱哈希一致性、内容摘要正确性**，
发现异常立即在**首个断点**处停止并返回，绝不伪报通过。
"""
from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Optional

#: 创世哈希：首事件的前驱为 SHA256("")
GENESIS_HASH = hashlib.sha256(b"").hexdigest()

#: 事件类型（开放枚举，新增类型不影响历史链的校验）
EVENT_TASK_CREATED = "task_created"
EVENT_TASK_STARTED = "task_started"
EVENT_TASK_COMPLETED = "task_completed"
EVENT_TASK_FAILED = "task_failed"
EVENT_QUOTA_SETTLED = "quota_settled"


def canonical_json(payload: Any) -> str:
    """业务载荷的规范化 JSON：键排序、紧凑分隔、非 ASCII 不转义。"""
    return json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )


def compute_event_hash(
    seq: int,
    event_time: str,
    event_type: str,
    payload: Any,
    prev_hash: str,
) -> str:
    """计算单条事件的链式哈希（覆盖序号、时间、类型与载荷全文）。"""
    envelope = {
        "seq": seq,
        "time": event_time,
        "type": event_type,
        "payload": payload,
    }
    digest_input = prev_hash + canonical_json(envelope)
    return hashlib.sha256(digest_input.encode("utf-8")).hexdigest()


def verify_chain(events: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """按存储顺序逐事件校验哈希链，返回**首个断点**；链完整时返回 ``None``。

    ``events`` 元素为数据库行映射后的字典，键固定为：
    ``seq`` / ``event_time`` / ``event_type`` / ``prev_hash`` /
    ``payload``（入库时的规范化 JSON 文本）/ ``hash``。

    每条事件按下列顺序检查，任一失败即返回断点（不再继续校验后续事件）：
      1. 序号：必须从 1 起且逐条 +1（``seq_gap``）；
      2. 前驱哈希：必须等于上一事件（首事件为创世哈希）的哈希
         （``prev_hash_mismatch``）；
      3. 载荷：存储文本必须是合法、规范化的 JSON
         （``payload_invalid_json`` / ``payload_not_canonical``）；
      4. 内容摘要：用序号/时间/类型/载荷与前驱哈希重算的结果必须等于
         存储哈希（``hash_mismatch``，序号以外字段被篡改均落在此类）。
    """
    if not events:
        return {
            "seq": 0,
            "code": "empty_chain",
            "detail": "任务没有任何审计事件，审计链不完整",
        }

    prev_hash = GENESIS_HASH
    expected_seq = 1
    for ev in events:
        seq = int(ev["seq"])
        stored_prev = str(ev["prev_hash"])
        stored_hash = str(ev["hash"])
        payload_text = str(ev["payload"])

        if seq != expected_seq:
            if expected_seq == 1:
                detail = f"审计链序号必须从 1 开始，实际首事件序号为 {seq}"
            else:
                detail = (
                    f"审计链序号不连续：上一事件序号为 {expected_seq - 1}，"
                    f"下一事件序号应为 {expected_seq}，实际为 {seq}"
                )
            return {
                "seq": seq,
                "code": "seq_gap",
                "expected": expected_seq,
                "actual": seq,
                "detail": detail,
            }

        if stored_prev != prev_hash:
            return {
                "seq": seq,
                "code": "prev_hash_mismatch",
                "expected": prev_hash,
                "actual": stored_prev,
                "detail": (
                    f"事件 {seq} 的前驱哈希与上一事件哈希不一致，链在此处断裂"
                ),
            }

        try:
            payload = json.loads(payload_text)
        except (ValueError, TypeError) as exc:
            return {
                "seq": seq,
                "code": "payload_invalid_json",
                "detail": f"事件 {seq} 的载荷不是合法 JSON：{exc}",
            }

        canonical_text = canonical_json(payload)
        if canonical_text != payload_text:
            return {
                "seq": seq,
                "code": "payload_not_canonical",
                "detail": (
                    f"事件 {seq} 的载荷存储文本与规范化 JSON 不一致，"
                    "载荷编码已被改动"
                ),
            }

        recomputed = compute_event_hash(
            seq, str(ev["event_time"]), str(ev["event_type"]), payload, prev_hash
        )
        if recomputed != stored_hash:
            return {
                "seq": seq,
                "code": "hash_mismatch",
                "expected": recomputed,
                "actual": stored_hash,
                "detail": (
                    f"事件 {seq} 的内容摘要失配：按序号/时间/类型/载荷与"
                    "前驱哈希重算的 SHA-256 与存储哈希不一致"
                ),
            }

        prev_hash = stored_hash
        expected_seq += 1

    return None
