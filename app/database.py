"""SQLite persistence for composition tasks.

任务状态机：pending -> running -> done（结果写入 result 列）。
后台线程与请求线程各自建立连接，配合进程内互斥锁串行化写操作。

组卷模板（templates 表）：创建后不可变——只提供插入与查询，没有任何更新
接口；修改约束配置只能通过创建新版本（同一 root_id 谱系下 version 递增）。

题目曝光配额（quotas / quota_reservations 表）：按题号维护可用次数；
启用配额的任务在可行组卷完成时，于**单个 SQLite 事务**（BEGIN IMMEDIATE）
中串行化校验并原子扣减——任一题余额不足则整组不扣减，任务结果改写为
quota_exceeded 并附缺额明细；扣减成功时逐题写入预留记录（含扣后剩余快照），
供任务查询接口回显“本次预留及剩余次数”。

防篡改审计链（task_audit_events 表，见 app/audit.py）：任务创建、开始、
完成/失败以及配额结算时各追加一条事件；同一任务的事件写入与业务状态更新
**共用同一个 SQLite 事务**（尤其配额结算事件与扣减结果原子提交），
序号在任务内连续、哈希只追加不断链。
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from . import audit

DB_PATH = os.environ.get("EXAM_DB_PATH", "/data/exam.db")

_lock = threading.Lock()


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
    with _lock, get_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                name        TEXT,
                status      TEXT NOT NULL DEFAULT 'pending',
                request     TEXT NOT NULL,
                result      TEXT,
                seed        INTEGER NOT NULL,
                created_at  TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS templates (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                name        TEXT,
                config      TEXT NOT NULL,
                version     INTEGER NOT NULL,
                root_id     INTEGER,
                base_id     INTEGER,
                created_at  TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )
        # 题目曝光配额：question_id 存 JSON 编码的题号（整数 1 与字符串 "1" 互不冲突）
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS quotas (
                question_id TEXT PRIMARY KEY,
                quota       INTEGER NOT NULL CHECK (quota >= 0),
                updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )
        # 每次成功扣减的预留明细（remaining 为本次扣减后的剩余快照）
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS quota_reservations (
                task_id     INTEGER NOT NULL,
                question_id TEXT NOT NULL,
                reserved    INTEGER NOT NULL,
                remaining   INTEGER NOT NULL,
                created_at  TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (task_id, question_id)
            )
            """
        )
        # 防篡改审计链：每个任务一条独立的只追加事件链（见 app/audit.py）。
        # (task_id, seq) 为主键，序号在任务内连续；hash 为含前驱哈希的
        # SHA-256 内容摘要。事件的写入与对应业务状态变更共用同一个事务。
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS task_audit_events (
                task_id     INTEGER NOT NULL,
                seq         INTEGER NOT NULL,
                event_time  TEXT NOT NULL,
                event_type  TEXT NOT NULL,
                payload     TEXT NOT NULL,
                prev_hash   TEXT NOT NULL,
                hash        TEXT NOT NULL,
                PRIMARY KEY (task_id, seq)
            )
            """
        )


# --------------------------------------------------------------------------- #
# 防篡改审计链（事件追加与对应业务写入共用同一事务，见 app/audit.py）
# --------------------------------------------------------------------------- #
def _now_iso() -> str:
    """审计事件时间戳：UTC ISO 8601（秒级），格式为 %Y-%m-%dT%H:%M:%SZ。"""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def _append_audit_event(
    conn: sqlite3.Connection,
    task_id: int,
    event_type: str,
    payload: Dict[str, Any],
) -> None:
    """在**调用方持有的事务**内追加一条审计事件（不在此处提交）。

    序号取该任务当前最大序号 +1，前驱哈希取当前链尾（无事件时为创世哈希）；
    与调用方的业务写入同生共死：业务回滚则事件回滚，事件无法落库则业务回滚。
    """
    payload_text = audit.canonical_json(payload)
    row = conn.execute(
        "SELECT seq, hash FROM task_audit_events WHERE task_id = ? ORDER BY seq DESC"
        " LIMIT 1",
        (task_id,),
    ).fetchone()
    if row is None:
        seq, prev_hash = 1, audit.GENESIS_HASH
    else:
        seq, prev_hash = int(row["seq"]) + 1, str(row["hash"])

    event_time = _now_iso()
    digest = audit.compute_event_hash(
        seq, event_time, event_type, payload, prev_hash
    )
    conn.execute(
        "INSERT INTO task_audit_events"
        " (task_id, seq, event_time, event_type, payload, prev_hash, hash)"
        " VALUES (?, ?, ?, ?, ?, ?, ?)",
        (task_id, seq, event_time, event_type, payload_text, prev_hash, digest),
    )


def _created_payload(request_json: str, seed: int) -> Dict[str, Any]:
    """task_created 的最小业务载荷：任务来源与约束规模，不内嵌题库快照全文。"""
    try:
        req = json.loads(request_json)
    except (ValueError, TypeError):
        return {"seed": int(seed)}
    # 改卷任务的题库快照与原始约束冻结在 source_request 中
    constraints_src = req.get("source_request") if req.get("kind") == "revise" else req
    if not isinstance(constraints_src, dict):
        constraints_src = {}
    payload: Dict[str, Any] = {
        "seed": int(seed),
        "question_count": len(constraints_src.get("questions") or []),
    }
    # 仅记录非缺省值：paper_count>=2 为多卷、alternative_count>=2 为多候选
    if int(constraints_src.get("paper_count") or 1) >= 2:
        payload["paper_count"] = int(constraints_src["paper_count"])
    if int(constraints_src.get("alternative_count") or 1) >= 2:
        payload["alternative_count"] = int(constraints_src["alternative_count"])
    if req.get("use_quota"):
        payload["use_quota"] = True
    kind = req.get("kind")
    if kind in ("revise", "template"):
        payload["kind"] = kind
        if kind == "revise" and req.get("source_task_id") is not None:
            payload["source_task_id"] = int(req["source_task_id"])
        if kind == "template":
            if req.get("template_id") is not None:
                payload["template_id"] = int(req["template_id"])
            if req.get("template_version") is not None:
                payload["template_version"] = int(req["template_version"])
    return payload


def _finished_payload(final: Dict[str, Any]) -> Dict[str, Any]:
    """task_completed / task_failed 的最小业务载荷。"""
    status = str(final.get("status"))
    if status == "error":
        payload: Dict[str, Any] = {"status": "error"}
        # 错误文本截断入链；完整 traceback 仍在任务结果中可查
        error = str(final.get("error") or "unknown error")
        payload["error"] = error[:500]
        return payload
    payload = {"status": status}
    if status == "feasible":
        payload["selected_ids"] = _top_candidate_ids(final)
    return payload


def _top_candidate_ids(result: Dict[str, Any]) -> List[List[Any]]:
    """提取排名第 1 候选各卷的选中题号（单卷任务为单元素列表）。"""
    source = result
    alts = result.get("alternatives")
    if alts:
        source = min(alts, key=lambda a: a.get("rank", 1))
    papers = source.get("papers")
    if papers:
        return [list(p.get("selected_ids") or []) for p in papers]
    return [list(source.get("selected_ids") or [])]


def create_task(name: Optional[str], request_json: str, seed: int) -> int:
    with _lock, get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO tasks (name, status, request, seed) VALUES (?, 'pending', ?, ?)",
            (name, request_json, seed),
        )
        task_id = int(cur.lastrowid)
        # 创建事件与任务插入同一事务：任一失败则两者都不生效
        _append_audit_event(
            conn,
            task_id,
            audit.EVENT_TASK_CREATED,
            _created_payload(request_json, seed),
        )
        return task_id


def mark_task_running(task_id: int) -> None:
    """任务开始执行：状态转 running 并追加 task_started 事件（同一事务）。"""
    with _lock, get_conn() as conn:
        conn.execute("UPDATE tasks SET status = ? WHERE id = ?", ("running", task_id))
        _append_audit_event(conn, task_id, audit.EVENT_TASK_STARTED, {})


def finish_task(task_id: int, result_json: str) -> None:
    with _lock, get_conn() as conn:
        final = json.loads(result_json)
        conn.execute(
            "UPDATE tasks SET status = 'done', result = ? WHERE id = ?",
            (result_json, task_id),
        )
        event_type = (
            audit.EVENT_TASK_FAILED
            if str(final.get("status")) == "error"
            else audit.EVENT_TASK_COMPLETED
        )
        _append_audit_event(conn, task_id, event_type, _finished_payload(final))


def get_task(task_id: int) -> Optional[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()


def list_audit_events(task_id: int) -> List[sqlite3.Row]:
    """按序号读取任务的全部审计事件（只追加，顺序即链顺序）。"""
    with get_conn() as conn:
        return conn.execute(
            "SELECT task_id, seq, event_time, event_type, payload, prev_hash, hash"
            " FROM task_audit_events WHERE task_id = ? ORDER BY seq",
            (task_id,),
        ).fetchall()


# --------------------------------------------------------------------------- #
# 组卷模板（不可变；版本谱系以 root_id 标识，version 在谱系内单调递增）
# --------------------------------------------------------------------------- #
def create_template(
    name: Optional[str], config_json: str, base: Optional[sqlite3.Row]
) -> Tuple[int, int]:
    """插入一条模板记录，返回 (template_id, version)。

    base 为 None 时创建全新谱系（version=1，root_id 指向自身）；
    否则在 base 所在谱系内取当前最大版本号 +1，base_id 记录派生来源。
    模板记录只插不改，天然满足“创建后不可原地修改”。
    """
    with _lock, get_conn() as conn:
        if base is None:
            cur = conn.execute(
                "INSERT INTO templates (name, config, version, root_id, base_id)"
                " VALUES (?, ?, 1, -1, NULL)",
                (name, config_json),
            )
            template_id = int(cur.lastrowid)
            conn.execute(
                "UPDATE templates SET root_id = ? WHERE id = ?",
                (template_id, template_id),
            )
            return template_id, 1

        root_id = int(base["root_id"])
        row = conn.execute(
            "SELECT COALESCE(MAX(version), 0) AS v FROM templates WHERE root_id = ?",
            (root_id,),
        ).fetchone()
        version = int(row["v"]) + 1
        cur = conn.execute(
            "INSERT INTO templates (name, config, version, root_id, base_id)"
            " VALUES (?, ?, ?, ?, ?)",
            (name, config_json, version, root_id, int(base["id"])),
        )
        return int(cur.lastrowid), version


def get_template(template_id: int) -> Optional[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute(
            "SELECT * FROM templates WHERE id = ?", (template_id,)
        ).fetchone()


# --------------------------------------------------------------------------- #
# 题目曝光配额（按题号的可用次数；扣减在单个事务中串行化校验、原子生效）
# --------------------------------------------------------------------------- #
def _qkey(qid: Any) -> str:
    """题号的数据库存储键：JSON 编码，区分整数 1 与字符串 "1"。"""
    return json.dumps(qid, ensure_ascii=False)


def set_quotas(items: List[Tuple[Any, int]]) -> None:
    """按题号设置可用次数（绝对值，存在即覆盖）。"""
    with _lock, get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        for qid, quota in items:
            conn.execute(
                "INSERT INTO quotas (question_id, quota, updated_at)"
                " VALUES (?, ?, datetime('now'))"
                " ON CONFLICT(question_id) DO UPDATE SET"
                "   quota = excluded.quota, updated_at = excluded.updated_at",
                (_qkey(qid), int(quota)),
            )


def list_quotas() -> List[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute(
            "SELECT question_id, quota, updated_at FROM quotas ORDER BY rowid"
        ).fetchall()


def get_quota_reservations(task_id: int) -> List[sqlite3.Row]:
    """某任务本次成功扣减的预留明细（按写入顺序，即规范化题号序）。"""
    with get_conn() as conn:
        return conn.execute(
            "SELECT question_id, reserved, remaining FROM quota_reservations"
            " WHERE task_id = ? ORDER BY rowid",
            (task_id,),
        ).fetchall()


def finish_task_with_quota(
    task_id: int, result: Dict[str, Any], requirements: List[Tuple[Any, int]]
) -> Dict[str, Any]:
    """启用配额的任务落库：校验 -> 原子扣减 -> 完成任务，全部在单个事务中。

    requirements 为 (题号, 需扣减次数) 列表。进程内互斥锁 + BEGIN IMMEDIATE
    保证并发任务的校验-扣减严格串行：
    - 全部题余额充足：逐题扣减、写入预留记录（remaining 为扣后快照），
      任务以原可行结果完成；
    - 任一题余额不足（无配额记录按 0 计）：整组不扣减，任务结果改写为
      quota_exceeded 并附缺额明细。
    任务完成事件（task_completed）与配额结算事件（quota_settled，含扣减明细或
    缺额）在同一事务内追加，与扣减结果一起原子提交。
    返回最终落库的 result。
    """
    with _lock, get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        keys = [(_qkey(qid), qid, int(need)) for qid, need in requirements]
        rows = (
            conn.execute(
                "SELECT question_id, quota FROM quotas WHERE question_id IN"
                f" ({','.join('?' for _ in keys)})",
                [k for k, _, _ in keys],
            ).fetchall()
            if keys
            else []
        )
        balance = {r["question_id"]: int(r["quota"]) for r in rows}

        shortages = []
        for key, qid, need in keys:
            available = balance.get(key, 0)
            if available < need:
                shortages.append(
                    {
                        "question_id": qid,
                        "required": need,
                        "available": available,
                        "shortage": need - available,
                    }
                )

        if shortages:
            final = {
                "status": "quota_exceeded",
                "message": "题目曝光配额不足，本次组卷未扣减任何配额",
                "shortages": shortages,
            }
        else:
            for key, qid, need in keys:
                remaining = balance[key] - need
                conn.execute(
                    "UPDATE quotas SET quota = ?, updated_at = datetime('now')"
                    " WHERE question_id = ?",
                    (remaining, key),
                )
                conn.execute(
                    "INSERT INTO quota_reservations"
                    " (task_id, question_id, reserved, remaining)"
                    " VALUES (?, ?, ?, ?)",
                    (task_id, key, need, remaining),
                )
            final = result

        result_text = json.dumps(final, ensure_ascii=False)
        conn.execute(
            "UPDATE tasks SET status = 'done', result = ? WHERE id = ?",
            (result_text, task_id),
        )
        # 完成事件与配额结算事件均与扣减/结果更新在同一事务内：
        # 扣减结果与审计链状态严格一致，不存在“已扣减无记录”或反之的中间态
        _append_audit_event(
            conn, task_id, audit.EVENT_TASK_COMPLETED, _finished_payload(final)
        )
        if shortages:
            settlement_payload: Dict[str, Any] = {
                "outcome": "exceeded",
                "deducted": 0,
                "shortages": shortages,
            }
        else:
            settlement_payload = {
                "outcome": "reserved",
                "deducted": int(sum(int(need) for _, _, need in keys)),
                "reservations": [
                    {
                        "question_id": qid,
                        "reserved": int(need),
                        "remaining": int(balance[key] - need),
                    }
                    for key, qid, need in keys
                ],
            }
        _append_audit_event(
            conn, task_id, audit.EVENT_QUOTA_SETTLED, settlement_payload
        )
    return final
