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
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from typing import Any, Dict, List, Optional, Tuple

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


def create_task(name: Optional[str], request_json: str, seed: int) -> int:
    with _lock, get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO tasks (name, status, request, seed) VALUES (?, 'pending', ?, ?)",
            (name, request_json, seed),
        )
        return int(cur.lastrowid)


def update_status(task_id: int, status: str) -> None:
    with _lock, get_conn() as conn:
        conn.execute("UPDATE tasks SET status = ? WHERE id = ?", (status, task_id))


def finish_task(task_id: int, result_json: str) -> None:
    with _lock, get_conn() as conn:
        conn.execute(
            "UPDATE tasks SET status = 'done', result = ? WHERE id = ?",
            (result_json, task_id),
        )


def get_task(task_id: int) -> Optional[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()


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

        conn.execute(
            "UPDATE tasks SET status = 'done', result = ? WHERE id = ?",
            (json.dumps(final, ensure_ascii=False), task_id),
        )
    return final
