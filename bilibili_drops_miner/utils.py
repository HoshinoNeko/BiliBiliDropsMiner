from __future__ import annotations

import re

COOKIE_PATTERN = re.compile(r"\s*([^=;\s]+)\s*=\s*([^;]*)")


def parse_room_ids(raw: str) -> list[int]:
    room_ids: list[int] = []
    for token in raw.replace("\n", ",").split(","):
        cleaned = token.strip()
        if not cleaned:
            continue
        if not cleaned.isdigit():
            raise ValueError(f"房间号格式错误: {cleaned}")
        room_id = int(cleaned)
        if room_id <= 0:
            raise ValueError(f"房间号必须大于 0: {cleaned}")
        room_ids.append(room_id)
    return room_ids


def parse_task_ids(raw: str) -> list[str]:
    # 1. 尝试从粘贴的 URL/参数中提取 task_ids 的值
    #    匹配 ? 或 & 之后的 task_ids=...（直到下一个 & 或结束）
    match = re.search(r'(?:[?&])task_ids=([^&]+)', raw)
    if match:
        raw = match.group(1)  # 只保留逗号分隔的 ID 列表部分
    # 2. 原有逻辑：统一分隔符并逐项清洗
    task_ids: list[str] = []
    for token in raw.replace("\n", ",").split(","):
        cleaned = token.strip()
        if not cleaned:
            continue
        # 3. 二次防护：如果 token 还包含 &，截断取前半部分
        if '&' in cleaned:
            cleaned = cleaned.split('&', 1)[0].strip()
        if cleaned:
            task_ids.append(cleaned)
    return task_ids


def parse_cookie(cookie_text: str) -> dict[str, str]:
    cookie_map: dict[str, str] = {}
    for key, value in COOKIE_PATTERN.findall(cookie_text):
        cookie_map[key] = value
    return cookie_map


def get_cookie_value(cookie_text: str, key: str) -> str:
    return parse_cookie(cookie_text).get(key, "")


def join_cookie(cookie_map: dict[str, str]) -> str:
    return "; ".join(f"{key}={value}" for key, value in cookie_map.items())
