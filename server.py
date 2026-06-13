"""
BiliBili 掉宝助手 — Web 后端
FastAPI + WebSocket，支持多账户并发挂机、实时日志推送、任务查询与奖励领取。
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import queue
import sys
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ── 将 bilibili_drops_miner 包加入 import 路径 ──────────────────────────────
sys.path.insert(0, str(Path(__file__).parent))

from bilibili_drops_miner.config import MinerConfig
from bilibili_drops_miner.gui_parts.task_presenter import (
    format_reward_claim_results,
    format_task_progress,
)
from bilibili_drops_miner.logging_utils import setup_logging
from bilibili_drops_miner.miner import BilibiliWatchTimeMiner
from bilibili_drops_miner.utils import parse_room_ids, parse_task_ids
from bilibili_drops_miner.client import BilibiliClient

# ═══════════════════════════════════════════════════════════════════════════════
# 数据结构
# ═══════════════════════════════════════════════════════════════════════════════

DATA_FILE = Path(__file__).parent / "accounts.json"
MAX_LOG_LINES = 2000   # 每账户日志最大缓存行数


@dataclass
class LogEntry:
    time: str
    level: str
    msg: str


@dataclass
class AccountState:
    id: str
    name: str
    config: dict[str, Any]
    status: str = "stopped"          # stopped | running | error
    uid: int | None = None
    uname: str = ""
    logs: list[LogEntry] = field(default_factory=list)
    # 运行时对象（不序列化）
    _miner: BilibiliWatchTimeMiner | None = field(default=None, repr=False)
    _thread: threading.Thread | None = field(default=None, repr=False)
    _stop_event: threading.Event = field(default_factory=threading.Event, repr=False)
    _log_queue: queue.Queue = field(default_factory=queue.Queue, repr=False)
    _log_handler: logging.Handler | None = field(default=None, repr=False)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "config": self.config,
            "status": self.status,
            "uid": self.uid,
            "uname": self.uname,
        }

    def save_dict(self) -> dict:
        """只保存持久化字段"""
        return {"id": self.id, "name": self.name, "config": self.config}


# ═══════════════════════════════════════════════════════════════════════════════
# 全局注册表
# ═══════════════════════════════════════════════════════════════════════════════

accounts: dict[str, AccountState] = {}          # id -> AccountState
ws_clients: dict[str, set[WebSocket]] = {}       # account_id -> set of WS connections
ws_clients_lock = threading.Lock()


def get_account(account_id: str) -> AccountState:
    if account_id not in accounts:
        raise HTTPException(status_code=404, detail=f"账户 {account_id} 不存在")
    return accounts[account_id]


# ═══════════════════════════════════════════════════════════════════════════════
# 持久化
# ═══════════════════════════════════════════════════════════════════════════════

def save_accounts() -> None:
    data = [acc.save_dict() for acc in accounts.values()]
    DATA_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load_accounts() -> None:
    if not DATA_FILE.exists():
        return
    try:
        data = json.loads(DATA_FILE.read_text(encoding="utf-8"))
        for item in data:
            acc = AccountState(
                id=item["id"],
                name=item["name"],
                config=item.get("config", {}),
            )
            accounts[acc.id] = acc
            ws_clients[acc.id] = set()
    except Exception as e:
        logging.getLogger(__name__).warning("加载账户数据失败: %s", e)


# ═══════════════════════════════════════════════════════════════════════════════
# 日志系统：每账户独立 logging.Handler → queue → WebSocket 广播
# ═══════════════════════════════════════════════════════════════════════════════

class AccountQueueHandler(logging.Handler):
    """捕获日志记录并放入账户专属 queue"""

    def __init__(self, log_queue: queue.Queue) -> None:
        super().__init__()
        self._queue = log_queue

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            level = record.levelname[:5].upper()
            now = time.strftime("%H:%M:%S", time.localtime(record.created))
            self._queue.put_nowait({"time": now, "level": level, "msg": msg})
        except Exception:
            pass


# ── 独立 logger 工厂，确保各账户互不干扰 ────────────────────────────────────

def make_account_logger(account_id: str, log_queue: queue.Queue, verbose: bool) -> logging.Logger:
    """为每个账户创建一个隔离的 Logger，只向自己的 queue 发送日志"""
    logger_name = f"bdm.{account_id}"
    logger = logging.getLogger(logger_name)
    # 清空旧 handler，防止重复挂载
    for h in logger.handlers[:]:
        logger.removeHandler(h)
    logger.propagate = False  # 不向 root logger 传播，避免混流

    handler = AccountQueueHandler(log_queue)
    fmt_str = (
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        if verbose
        else "%(asctime)s | %(message)s"
    )
    handler.setFormatter(logging.Formatter(fmt_str, datefmt="%H:%M:%S"))
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    return logger


# ── 后台协程：从 queue 取日志 → 存 AccountState.logs → 广播 WS ──────────────

async def log_drain_loop(account_id: str) -> None:
    """持续排空日志队列并广播到所有订阅该账户的 WebSocket 连接"""
    acc = accounts.get(account_id)
    if acc is None:
        return
    q = acc._log_queue
    while True:
        batch: list[dict] = []
        try:
            while True:
                entry = q.get_nowait()
                batch.append(entry)
                acc.logs.append(LogEntry(**entry))
                if len(acc.logs) > MAX_LOG_LINES:
                    acc.logs = acc.logs[-MAX_LOG_LINES:]
        except queue.Empty:
            pass

        if batch:
            msg = json.dumps({"type": "log", "account_id": account_id, "entries": batch})
            await broadcast(account_id, msg)

        await asyncio.sleep(0.15)


# ═══════════════════════════════════════════════════════════════════════════════
# WebSocket 广播
# ═══════════════════════════════════════════════════════════════════════════════

async def broadcast(account_id: str, message: str) -> None:
    with ws_clients_lock:
        clients = set(ws_clients.get(account_id, set()))
    dead: set[WebSocket] = set()
    for ws in clients:
        try:
            await ws.send_text(message)
        except Exception:
            dead.add(ws)
    if dead:
        with ws_clients_lock:
            ws_clients.get(account_id, set()).difference_update(dead)


async def broadcast_status(account_id: str) -> None:
    acc = accounts.get(account_id)
    if acc is None:
        return
    msg = json.dumps({
        "type": "status",
        "account_id": account_id,
        "status": acc.status,
        "uid": acc.uid,
        "uname": acc.uname,
    })
    await broadcast(account_id, msg)


# ═══════════════════════════════════════════════════════════════════════════════
# 挂机核心：在独立线程运行 BilibiliWatchTimeMiner
# ═══════════════════════════════════════════════════════════════════════════════

def _build_miner_config(cfg: dict) -> MinerConfig:
    return MinerConfig(
        cookie=cfg.get("cookie", "").strip(),
        room_ids=parse_room_ids(str(cfg.get("room_ids", ""))),
        thread_count=int(cfg.get("thread_count", 1)),
        reconnect_delay_seconds=int(cfg.get("reconnect_delay_seconds", 8)),
        enable_web_heartbeat=bool(cfg.get("enable_web_heartbeat", True)),
        task_ids=parse_task_ids(str(cfg.get("task_ids", ""))),
        task_query_interval_seconds=int(cfg.get("task_query_interval_seconds", 30)),
        notify_urls=parse_task_ids(str(cfg.get("notify_urls", ""))),
        notify_on_task_complete=bool(cfg.get("notify_on_task_complete", True)),
    )


def _miner_thread(account_id: str, event_loop: asyncio.AbstractEventLoop) -> None:
    """在独立系统线程内运行 miner。

    每个账户拥有独立的 Logger 实例（名称为 bdm.<account_id>），
    通过 AccountQueueHandler 将日志写入该账户专属的 queue。
    BilibiliWatchTimeMiner 和 X25KnWorker 接受 logger 参数直接使用，
    完全不触碰全局 bilibili_drops_miner logger，彻底消除跨账户日志混流。
    """
    acc = accounts.get(account_id)
    if acc is None:
        return

    verbose = bool(acc.config.get("verbose", False))
    # 为本账户创建隔离 logger，propagate=False，不向任何父 logger 传播
    logger = make_account_logger(account_id, acc._log_queue, verbose)

    try:
        config = _build_miner_config(acc.config)
        config.validate()
    except Exception as exc:
        logger.error("配置验证失败: %s", exc)
        acc.status = "error"
        asyncio.run_coroutine_threadsafe(broadcast_status(account_id), event_loop)
        return

    asyncio.run_coroutine_threadsafe(broadcast_status(account_id), event_loop)

    # 将账户专属 logger 注入 miner（miner 再传给所有 X25KnWorker），
    # 无需操作任何全局 / 模块级 logger。
    miner = BilibiliWatchTimeMiner(config, logger=logger)
    acc._miner = miner
    def handle_login(uid: int | None, uname: str):
        acc.uid = uid
        acc.uname = uname
        # 立即通知前端更新 UI
        asyncio.run_coroutine_threadsafe(broadcast_status(account_id), event_loop)
    miner.on_login = handle_login
    acc._miner = miner

    try:
        miner.run()
        logger.info("掉宝助手已正常退出")
    except Exception as exc:
        logger.error("掉宝助手异常退出: %s", exc)
        acc.status = "error"
    finally:
        if acc._miner is miner:
            acc._miner = None
        if acc._thread is threading.current_thread():
            acc._thread = None

        acc.status = "stopped"

        asyncio.run_coroutine_threadsafe(broadcast_status(account_id), event_loop)


def start_account(account_id: str, event_loop: asyncio.AbstractEventLoop) -> None:
    acc = accounts[account_id]
    if acc.status == "running" and acc._thread and acc._thread.is_alive():
        return

    acc.status = "running"
    acc._stop_event.clear()

    t = threading.Thread(
        target=_miner_thread,
        args=(account_id, event_loop),
        name=f"miner-{account_id[:8]}",
        daemon=True,
    )
    acc._thread = t
    t.start()


def stop_account(account_id: str) -> None:
    acc = accounts.get(account_id)
    if acc is None or acc._miner is None:
        if acc:
            acc.status = "stopped"
            acc._thread = None
        return

    acc.status = "stopping"
    miner_to_stop = acc._miner

    acc._miner.stop(force=False)
    # 给 2 秒后强制停止
    def _force(m):
        time.sleep(2.5)
        m.stop(force=True)
    threading.Thread(target=_force, args=(miner_to_stop,), daemon=True).start()


# ═══════════════════════════════════════════════════════════════════════════════
# FastAPI App
# ═══════════════════════════════════════════════════════════════════════════════

app = FastAPI(title="BiliBili 掉宝助手 API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── 主事件循环引用（在 startup 中保存）──────────────────────────────────────
_main_loop: asyncio.AbstractEventLoop | None = None
_drain_tasks: dict[str, asyncio.Task] = {}


@app.on_event("startup")
async def startup() -> None:
    global _main_loop
    _main_loop = asyncio.get_running_loop()
    load_accounts()
    for acc_id in accounts:
        ws_clients.setdefault(acc_id, set())
        _drain_tasks[acc_id] = asyncio.create_task(log_drain_loop(acc_id))
    logging.basicConfig(level=logging.INFO)


@app.on_event("shutdown")
async def shutdown() -> None:
    for acc in accounts.values():
        if acc.status == "running":
            stop_account(acc.id)
    for task in _drain_tasks.values():
        task.cancel()


# ─── 静态文件（前端 HTML）────────────────────────────────────────────────────
STATIC_DIR = Path(__file__).parent / "static"
STATIC_DIR.mkdir(exist_ok=True)

@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    html_path = STATIC_DIR / "index.html"
    if not html_path.exists():
        return HTMLResponse("<h1>前端文件未找到，请将 index.html 放入 static/ 目录</h1>")
    return HTMLResponse(html_path.read_text(encoding="utf-8"))


# ═══════════════════════════════════════════════════════════════════════════════
# REST API — 账户管理
# ═══════════════════════════════════════════════════════════════════════════════

class AccountCreateRequest(BaseModel):
    name: str
    config: dict[str, Any]


class AccountUpdateRequest(BaseModel):
    name: str | None = None
    config: dict[str, Any] | None = None


@app.get("/api/accounts")
async def list_accounts():
    return [acc.to_dict() for acc in accounts.values()]


@app.post("/api/accounts", status_code=201)
async def create_account(req: AccountCreateRequest):
    acc_id = "acc_" + uuid.uuid4().hex[:8]
    acc = AccountState(id=acc_id, name=req.name, config=req.config)
    accounts[acc_id] = acc
    ws_clients[acc_id] = set()
    _drain_tasks[acc_id] = asyncio.create_task(log_drain_loop(acc_id))
    save_accounts()
    return acc.to_dict()


@app.get("/api/accounts/{account_id}")
async def get_account_info(account_id: str):
    return get_account(account_id).to_dict()


@app.put("/api/accounts/{account_id}")
async def update_account(account_id: str, req: AccountUpdateRequest):
    acc = get_account(account_id)
    if req.name is not None:
        acc.name = req.name
    if req.config is not None:
        acc.config = req.config
        # 如果正在运行，动态同步可热更新的字段
        if acc._miner is not None:
            cfg = acc._miner.config
            try:
                v = int(req.config.get("reconnect_delay_seconds", cfg.reconnect_delay_seconds))
                if v > 0:
                    cfg.reconnect_delay_seconds = v
            except (ValueError, TypeError):
                pass
            try:
                v = int(req.config.get("task_query_interval_seconds", cfg.task_query_interval_seconds))
                if v > 0:
                    cfg.task_query_interval_seconds = v
            except (ValueError, TypeError):
                pass
            cfg.notify_on_task_complete = bool(req.config.get("notify_on_task_complete", cfg.notify_on_task_complete))
            new_task_ids = parse_task_ids(str(req.config.get("task_ids", "")))
            if new_task_ids != cfg.task_ids:
                cfg.task_ids = new_task_ids
            new_cookie = str(req.config.get("cookie", "")).strip()
            if new_cookie and new_cookie != cfg.cookie:
                acc._miner.update_cookie(new_cookie)
    save_accounts()
    return acc.to_dict()


@app.delete("/api/accounts/{account_id}", status_code=204)
async def delete_account(account_id: str):
    acc = get_account(account_id)
    if acc.status == "running":
        stop_account(account_id)
    if account_id in _drain_tasks:
        _drain_tasks.pop(account_id).cancel()
    del accounts[account_id]
    ws_clients.pop(account_id, None)
    save_accounts()


# ═══════════════════════════════════════════════════════════════════════════════
# REST API — 挂机控制
# ═══════════════════════════════════════════════════════════════════════════════

@app.post("/api/accounts/{account_id}/start")
async def api_start(account_id: str):
    acc = get_account(account_id)

    if acc.status == "running":
        return {"ok": False, "message": "已在运行中"}

    if acc.status == "stopping" or (acc._thread and acc._thread.is_alive()):
        return {"ok": False, "message": "正在停止中，请稍候再试"}

    if not _main_loop:
        raise HTTPException(500, "事件循环未就绪")

    start_account(account_id, _main_loop)
    await broadcast_status(account_id)
    return {"ok": True}


@app.post("/api/accounts/{account_id}/stop")
async def api_stop(account_id: str):
    acc = get_account(account_id)
    stop_account(account_id)
    # acc.status = "stopped"
    await broadcast_status(account_id)
    return {"ok": True}


@app.post("/api/start_all")
async def api_start_all():
    if not _main_loop:
        raise HTTPException(500, "事件循环未就绪")
    started = []
    for acc in accounts.values():
        if acc.status != "running":
            try:
                _build_miner_config(acc.config).validate()
                start_account(acc.id, _main_loop)
                await broadcast_status(acc.id)
                started.append(acc.id)
            except Exception:
                pass
    return {"started": started}


@app.post("/api/stop_all")
async def api_stop_all():
    for acc in accounts.values():
        if acc.status == "running":
            stop_account(acc.id)
            acc.status = "stopped"
            await broadcast_status(acc.id)
    return {"ok": True}


# ═══════════════════════════════════════════════════════════════════════════════
# REST API — 任务 & 奖励
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/accounts/{account_id}/tasks")
async def api_get_tasks(account_id: str):
    acc = get_account(account_id)
    cookie = acc.config.get("cookie", "").strip()
    task_ids_raw = str(acc.config.get("task_ids", "")).strip()
    if not cookie:
        raise HTTPException(400, "Cookie 未配置")
    if not task_ids_raw:
        return {"text": "未配置任务 ID", "tasks": []}

    task_ids = parse_task_ids(task_ids_raw)

    async def _query():
        client = BilibiliClient(cookie)
        try:
            return await client.get_task_progress(task_ids)
        finally:
            await client.close()

    try:
        progresses = await asyncio.wait_for(_query(), timeout=20)
        text = format_task_progress(progresses)
        tasks = [
            {
                "task_id": t.task_id,
                "task_name": t.task_name,
                "cur_value": t.cur_value,
                "limit_value": t.limit_value,
                "status": t.status,
                "is_completed": t.is_completed,
            }
            for t in progresses
        ]
        return {"text": text, "tasks": tasks}
    except asyncio.TimeoutError:
        raise HTTPException(504, "查询超时")
    except Exception as exc:
        raise HTTPException(500, str(exc))


@app.post("/api/accounts/{account_id}/claim_rewards")
async def api_claim_rewards(account_id: str):
    acc = get_account(account_id)
    cookie = acc.config.get("cookie", "").strip()
    task_ids_raw = str(acc.config.get("task_ids", "")).strip()
    if not cookie:
        raise HTTPException(400, "Cookie 未配置")
    if not task_ids_raw:
        raise HTTPException(400, "任务 ID 未配置")

    task_ids = parse_task_ids(task_ids_raw)

    async def _claim():
        client = BilibiliClient(cookie)
        try:
            return await client.receive_all_mission_rewards(task_ids)
        finally:
            await client.close()

    try:
        results = await asyncio.wait_for(_claim(), timeout=60)
        text = format_reward_claim_results(results)
        return {"text": text}
    except asyncio.TimeoutError:
        raise HTTPException(504, "领取超时")
    except Exception as exc:
        raise HTTPException(500, str(exc))


# ═══════════════════════════════════════════════════════════════════════════════
# REST API — 日志
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/accounts/{account_id}/logs")
async def api_get_logs(account_id: str, limit: int = 500):
    acc = get_account(account_id)
    entries = acc.logs[-limit:]
    return [{"time": e.time, "level": e.level, "msg": e.msg} for e in entries]


@app.delete("/api/accounts/{account_id}/logs", status_code=204)
async def api_clear_logs(account_id: str):
    acc = get_account(account_id)
    acc.logs.clear()


# ═══════════════════════════════════════════════════════════════════════════════
# WebSocket — 实时日志 & 状态推送
# ═══════════════════════════════════════════════════════════════════════════════

@app.websocket("/ws/{account_id}")
async def websocket_endpoint(websocket: WebSocket, account_id: str):
    if account_id not in accounts:
        await websocket.close(code=4004, reason="账户不存在")
        return

    await websocket.accept()

    with ws_clients_lock:
        ws_clients[account_id].add(websocket)

    acc = accounts[account_id]

    # 发送当前状态快照
    await websocket.send_text(json.dumps({
        "type": "init",
        "account_id": account_id,
        "status": acc.status,
        "uid": acc.uid,
        "uname": acc.uname,
        "logs": [{"time": e.time, "level": e.level, "msg": e.msg} for e in acc.logs[-200:]],
    }))

    try:
        while True:
            # 保持连接存活，客户端可发送 ping
            data = await asyncio.wait_for(websocket.receive_text(), timeout=30)
            if data == "ping":
                await websocket.send_text(json.dumps({"type": "pong"}))
    except (WebSocketDisconnect, asyncio.TimeoutError):
        pass
    finally:
        with ws_clients_lock:
            ws_clients.get(account_id, set()).discard(websocket)


# ═══════════════════════════════════════════════════════════════════════════════
# 入口
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    uvicorn.run(
        "server:app",
        host="0.0.0.0",
        port=8765,
        reload=False,
        log_level="info",
    )