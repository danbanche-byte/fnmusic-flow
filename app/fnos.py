from __future__ import annotations

import asyncio
import os
import json
import sqlite3
import re
import threading
import time
from pathlib import Path
from typing import Any

import httpx

from .track_match import (choose_library_entry, choose_match, filename_key,
                          library_relative_path, title_search_terms)

try:
    from opencc import OpenCC
    _TO_SIMPLIFIED = OpenCC('t2s')
    _TO_TRADITIONAL = OpenCC('s2t')
except ImportError:
    _TO_SIMPLIFIED = None
    _TO_TRADITIONAL = None


def _token_path() -> Path:
    return Path(os.getenv("FNOS_TOKEN_FILE", "/config/fnos-token"))


def _account_path() -> Path:
    return Path(os.getenv("FNOS_ACCOUNT_FILE", "/config/fnos-account.json"))


def _music_db_path() -> Path:
    return Path(os.getenv("FNOS_MUSIC_DB", "/var/lib/fnos-music-db/music.db"))


def _account_lock_path() -> Path:
    return Path(os.getenv("FNOS_ACCOUNT_LOCK_FILE", "/config/fnos-account-lock"))


def load_account_lock() -> str:
    """用户在界面明确选择绑定的账号（账号锁）。空 = 未锁定。"""
    try:
        return _account_lock_path().read_text(encoding="utf-8").strip().lower()
    except OSError:
        return ""


def save_account_lock(account: str | None) -> str:
    """持久化账号锁：后续 token 自愈只恢复该账号。传空清除锁。"""
    path = _account_lock_path()
    value = str(account or "").strip().lower()
    if not value:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
        return ""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(value, encoding="utf-8")
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)
    return value


def load_fnos_token() -> str:
    configured = os.getenv("FNOS_TOKEN", "").strip()
    if configured:
        return configured
    try:
        return _token_path().read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def save_fnos_token(token: str) -> None:
    value = token.strip()
    if not value:
        raise ValueError("飞牛音乐 token 不能为空")
    path = _token_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(value, encoding="utf-8")
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)


def save_fnos_account(account: dict[str, Any] | None) -> None:
    if not isinstance(account, dict):
        return
    value = {key: account.get(key) for key in ("guid", "name", "username") if account.get(key)}
    if not value:
        return
    path = _account_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)


def _preferred_account() -> dict[str, Any]:
    try:
        value = json.loads(_account_path().read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError, TypeError):
        return {}


def _allowed_accounts() -> list[str]:
    """本实例**只允许**使用的飞牛账号白名单（小写）。

    优先级：账号锁文件（用户在界面明确选择的账号，2026-10-01 社区版 1.3.0 新增）
    > 环境变量 `FNOS_ALLOWED_ACCOUNTS`（逗号分隔）。两者都空 = 不限制，
    自动绑定本机「可写」账号（论坛单用户场景点开即用）。

    背景（2026-09-28 账号串号修复）：token 缺失时从飞牛 music.db 按 `id desc`
    捞「任意可写账号」会悄悄借用机主凭据——多账号家庭 NAS 必须有白名单，
    账号锁就是界面化的白名单。
    """
    lock = load_account_lock()
    if lock:
        return [lock]
    raw = os.getenv("FNOS_ALLOWED_ACCOUNTS", "").strip()
    if not raw or raw == "*":
        return []
    return [item.strip().lower() for item in raw.split(",") if item.strip()]


def _music_db_tokens(limit: int = 30, *, writable_only: bool = False,
                     admins_only: bool | None = None) -> list[str]:
    """从飞牛 music.db 里捞可用的登录 token。

    2026-09-28 修复（第二轮，根因版）：
    飞牛音乐对「写歌单」的判定键**不是 role，而是 `user.shared_library_access_mode`**。
    实测（同一台 NAS、同一时刻、同一个歌单）：

        账号A  role=admin   access=all    -> POST /playlist/add-track  code:0   ✅
        账号B  role=member  access=all    -> POST /playlist/add-track  code:0   ✅
        账号C  role=member  access=none   -> POST /playlist/add-track  code:100004 forbidden ❌

    `账号B` 与 `账号C` 同为 member，唯一差别就是 access_mode —— 所以真正的门槛是
    `access_mode='all'`，admin 只是恰好都是 all 而已。上一轮按 `role='admin'`
    过滤虽然"能用"，但会让朋友的实例去借用机主的凭据，既越权又难看。

    现在按 `access_mode='all'` 过滤：既能选中普通成员账号，也能选中管理员，
    并且会**排除** access_mode='none' 的自动建号（`created_by='oauth'`，
    表现为「歌单建出来了但永远是空的」+ `forbidden`）。
    `admins_only` 仅作为旧调用点的兼容别名保留。

    2026-09-28 修复（第三轮，账号白名单）：
    增加 `FNOS_ALLOWED_ACCOUNTS` 白名单过滤 —— 朋友实例设为受信账号名，
    从此**只**会捞该账号自己的 token，绝不会再串到机主。
    白名单为空时保持原行为（主实例不受影响）。
    """
    if admins_only is not None:
        writable_only = bool(admins_only)
    path = _music_db_path()
    if not path.exists():
        return []
    allowed = _allowed_accounts()
    # 白名单占位符：账号名匹配 user.name（大小写不敏感）
    allowed_clause = ""
    allowed_params: tuple = ()
    if allowed:
        allowed_clause = " and lower(u.name) in (%s) " % ",".join("?" for _ in allowed)
        allowed_params = tuple(allowed)
    try:
        uri = f"file:{path.as_posix()}?mode=ro"
        with sqlite3.connect(uri, uri=True, timeout=3) as connection:
            if writable_only:
                # 优先未过期；advisory：expired_at 为文本时间戳，直接字符串比较即可
                rows = connection.execute(
                    "select ut.token from user_token ut "
                    "join user u on u.id = ut.user_id "
                    "where ut.token is not null and ut.token <> '' "
                    "  and u.status = 'active' "
                    "  and ifnull(u.shared_library_access_mode,'') = 'all' "
                    + allowed_clause +
                    "order by (ut.expired_at is null or ut.expired_at > datetime('now')) desc, "
                    "         ut.id desc limit ?",
                    allowed_params + (limit,),
                ).fetchall()
                if not rows and not allowed:
                    # 退一步：管理员天然可写。仅在没有任何 access=all 账号时兜底，
                    # 避免把功能彻底卡死。（白名单模式下**不做**此兜底 ——
                    # 否则会退化成「借用管理员账号」，正是本次要修的问题。）
                    rows = connection.execute(
                        "select ut.token from user_token ut "
                        "join user u on u.id = ut.user_id "
                        "where ut.token is not null and ut.token <> '' "
                        "  and u.status = 'active' and u.role = 'admin' "
                        "order by ut.id desc limit ?",
                        (limit,),
                    ).fetchall()
            else:
                rows = connection.execute(
                    "select ut.token from user_token ut "
                    "join user u on u.id = ut.user_id "
                    "where ut.token is not null and ut.token <> '' "
                    + allowed_clause +
                    "order by ut.id desc limit ?",
                    allowed_params + (limit,),
                ).fetchall()
    except (sqlite3.Error, OSError):
        return []
    return list(dict.fromkeys(str(row[0]).strip() for row in rows if row and str(row[0]).strip()))


def _music_db_names_of_token(token: str) -> list[str]:
    """反查一个 token 属于哪些账号（小写）。用于账号白名单校验。

    返回空列表 = 查不到（token 可能已失效或不在库中）。
    """
    token = (token or "").strip()
    path = _music_db_path()
    if not token or not path.exists():
        return []
    try:
        uri = f"file:{path.as_posix()}?mode=ro"
        with sqlite3.connect(uri, uri=True, timeout=3) as connection:
            rows = connection.execute(
                "select lower(u.name) from user_token ut "
                "join user u on u.id = ut.user_id where ut.token=?",
                (token,),
            ).fetchall()
    except (sqlite3.Error, OSError):
        return []
    return list(dict.fromkeys(str(row[0]).strip() for row in rows if row and str(row[0]).strip()))


def _music_db_access_mode_of_token(token: str, guid: str = "") -> str:
    """用 token（或 guid）反查飞牛 user 表的 shared_library_access_mode。

    这是**唯一权威**的写权限判定来源 —— 飞牛的 /user/me 不返回该字段。
    返回 'all' / 'none' / ''（查询失败）。
    """
    path = _music_db_path()
    if not path.exists() or (not token and not guid):
        return ""
    try:
        uri = f"file:{path.as_posix()}?mode=ro"
        with sqlite3.connect(uri, uri=True, timeout=3) as connection:
            row = None
            if guid:
                row = connection.execute(
                    "select shared_library_access_mode from user where guid=? limit 1",
                    (guid,),
                ).fetchone()
            if row is None and token:
                row = connection.execute(
                    "select u.shared_library_access_mode from user_token ut "
                    "join user u on u.id = ut.user_id where ut.token=? limit 1",
                    (token,),
                ).fetchone()
            return str(row[0] or "").strip().lower() if row else ""
    except (sqlite3.Error, OSError):
        return ""


def _account_is_writable(account: dict) -> bool:
    """判断某个飞牛账号是否**能写歌单**（add-track / remove-track）。

    2026-09-28 实测结论（同一台 NAS、同一时刻、同一歌单）：
        账号A  role=admin   access_mode=all    -> code:0   ✅
        账号B  role=member  access_mode=all    -> code:0   ✅
        账号C  role=member  access_mode=none   -> 100004 forbidden ❌

    判定键是 `shared_library_access_mode='all'`，与 role 无关。

    ⚠️ 关键：**飞牛的 `/user/me` 不返回该字段**，所以不能只看入参 dict。
    这里以**飞牛库 user 表为权威来源**（按 guid 反查），
    只有查不到时才退回调用方传入的字段 / role，最后仍无结论则**乐观放行**
    （返回 True），把真正的拒绝权交给写接口自身的 403/100004
    —— 那里有 _request() 的换 token 自愈兜底。
    否则会把完全可写（access=all）的账号误判成不可写，反而卡死自愈逻辑。
    """
    mode = str(account.get("sharedLibraryAccessMode")
               or account.get("shared_library_access_mode") or "").lower()
    if not mode:
        mode = _music_db_access_mode_of_token("", str(account.get("guid") or ""))
    if mode == "all":
        return True
    if mode == "none":
        return False
    return True


# 兼容旧名（历史调用点）
_account_can_write = _account_is_writable


def _rows(value: Any) -> list[dict]:
    if isinstance(value, list):
        return [row for row in value if isinstance(row, dict)]
    if isinstance(value, dict):
        for key in ("list", "items", "tracks", "data"):
            nested = value.get(key)
            if isinstance(nested, (list, dict)):
                rows = _rows(nested)
                if rows:
                    return rows
    return []


# ---------------------------------------------------------------------------
# 按「真实文件路径」直接定位 track guid
# ---------------------------------------------------------------------------
# 背景：飞牛的 /search/track 是**按标题**检索的。当某个音频的元数据还没被刮削时，
# track.title 会停留在字面值 'track'，于是无论用什么关键词都搜不到它 —— 文件明明
# 在库里、能被飞牛播放，我们的推送却永远报「没有唯一匹配」。这就是「巅峰榜·热歌」
# 里那首「思念若是一首诗」卡住的根因。
#
# 解法：绕开检索，直接读飞牛自己的 music.db（容器已只读挂载），用
# audio_file.path 反查 track.guid。本地文件与飞牛索引的是同一批真实文件，
# 路径只差一个挂载根前缀，剥掉前缀后完全一致 —— 这是比标题更硬的等价关系。
_music_index_lock = threading.Lock()
_music_index_cache: dict[str, Any] = {"at": 0.0, "by_stem": {}, "size": 0}


def _music_index_ttl() -> float:
    try:
        return max(0.0, float(os.getenv("FNOS_MUSIC_INDEX_TTL", "120")))
    except ValueError:
        return 120.0


def _load_music_index() -> dict[str, list[dict]]:
    """构建 ``文件名主干归一化键 -> [{path, guid, deleted}]`` 索引（带 TTL 缓存）。

    music.db 有两万多条音频，一次全表读约几十毫秒。一次推送可能涉及几百首歌，
    所以缓存住避免重复读；TTL 到期后自动重建，保证新入库的歌也能被看到。
    """
    now = time.monotonic()
    with _music_index_lock:
        if _music_index_cache["by_stem"] and now - float(_music_index_cache["at"]) < _music_index_ttl():
            return _music_index_cache["by_stem"]
        path = _music_db_path()
        index: dict[str, list[dict]] = {}
        if path.exists():
            try:
                uri = f"file:{path.as_posix()}?mode=ro"
                with sqlite3.connect(uri, uri=True, timeout=5) as connection:
                    cursor = connection.execute(
                        """select af.path, af.is_physical_file_deleted, t.guid
                           from audio_file af
                           join track t on t.audio_file_id = af.id
                           where af.path is not null and af.path <> ''
                             and t.guid is not null and t.guid <> ''"""
                    )
                    for fnos_path, deleted, guid in cursor:
                        stem = filename_key(fnos_path)
                        if not stem:
                            continue
                        index.setdefault(stem, []).append({
                            "path": str(fnos_path),
                            "guid": str(guid),
                            "deleted": bool(deleted),
                        })
            except (sqlite3.Error, OSError):
                # 读不到就退回搜索匹配，绝不让索引问题影响推送本身。
                return _music_index_cache["by_stem"] or {}
        _music_index_cache["at"] = now
        _music_index_cache["by_stem"] = index
        _music_index_cache["size"] = sum(len(items) for items in index.values())
        return index


def lookup_track_guid_by_library_path(file_path: object) -> str | None:
    """用本地已下载文件的路径，直接在飞牛 music.db 里取 track guid。

    命中返回 guid；路径无法唯一确定时返回 None，交回给原有的搜索匹配。
    """
    stem = filename_key(file_path)
    if not stem:
        return None
    entries = _load_music_index().get(stem)
    if not entries:
        return None
    return choose_library_entry(file_path, entries)


def dead_fnos_guids(guids) -> set[str]:
    """查询飞牛 music.db，返回其中已被标记 is_physical_file_deleted=1 的 guid。

    背景（2026-09-18 事故）：推送同时触发的全库扫描与文件落位/晋升并发时，
    飞牛会把「文件其实还在」的索引行判死；歌单里的 guid 仍指向死行，
    端上显示「歌曲文件不存在」，而推送却报成功。推送后据此如实核验。
    读不到库时静默返回空集 —— 绝不让核验失败影响推送本身。
    """
    values = list(dict.fromkeys(str(g).strip() for g in guids or [] if str(g).strip()))
    if not values:
        return set()
    path = _music_db_path()
    if not path.exists():
        return set()
    try:
        uri = f"file:{path.as_posix()}?mode=ro"
        dead: set[str] = set()
        with sqlite3.connect(uri, uri=True, timeout=5) as connection:
            for index in range(0, len(values), 500):
                chunk = values[index:index + 500]
                query = (
                    "select t.guid from track t "
                    "join audio_file af on af.id = t.audio_file_id "
                    f"where t.guid in ({','.join('?' * len(chunk))}) "
                    "and af.is_physical_file_deleted = 1"
                )
                for row in connection.execute(query, chunk):
                    dead.add(str(row[0]))
        return dead
    except (sqlite3.Error, OSError):
        return set()


_MISSING_WORDS = ("不存在", "not found", "no such", "resource not found", "无效", "invalid")
_AUTH_WORDS = ("token", "unauthorized", "登录", "session", "会话", "socket", "connect", "timeout")


def _looks_missing_resource(detail: str) -> bool:
    """判断错误信息是否表示「目标歌单在飞牛侧已被删除」。

    必须与鉴权/连接类错误区分开：歌单没了可以重建，而会话失效时重建歌单
    会白白多出一个重名歌单。所以先排除鉴权/连接噪声，再看缺失特征。
    """
    text = str(detail or "").strip().casefold()
    if not text:
        return False
    if any(word.casefold() in text for word in _AUTH_WORDS):
        return False
    return any(word.casefold() in text for word in _MISSING_WORDS)


class FnosAdapter:
    """Adapter for the local trim.music Unix-socket API on fnOS."""

    def __init__(self, base_url: str | None = None, token: str | None = None):
        self.base_url = (base_url or "http://localhost/music/api/v1").rstrip("/")
        candidate = token.strip() if token and token.strip() else load_fnos_token()
        # 2026-09-28（账号白名单）：若磁盘上的 token 不属于本实例允许的账号，
        # 直接丢弃 —— 否则会带着机主的凭据到处跑。
        self.token = "" if candidate and not self._token_allowed(candidate) else candidate
        self.socket_path = os.getenv("FNOS_MUSIC_SOCKET", "/var/run/trim_music.socket")

    @staticmethod
    def _token_allowed(token: str) -> bool:
        """token 是否属于白名单账号（白名单为空 = 不限制，恒 True）。"""
        allowed = _allowed_accounts()
        if not allowed:
            return True
        names = _music_db_names_of_token(token)
        return any(name in allowed for name in names)

    async def _request(self, method: str, path: str, **kwargs):
        if self.token and not self._token_allowed(self.token):
            # 串号 token 现身：丢弃并重新走白名单兜底
            self.token = ""
        if not self.token:
            # 2026-09-28：自动兜底时优先取 **可写** 账号 token。
            # 实测门槛是 user.shared_library_access_mode='all'（而非 role='admin'）：
            # access_mode='none' 的自动建号会让 add-track 拿到 100004 forbidden。
            tokens = _music_db_tokens(1, writable_only=True) or _music_db_tokens(1)
            if tokens:
                self.token = tokens[0]
        if not self.token:
            raise RuntimeError("飞牛音乐没有可用登录会话，请先登录飞牛音乐")
        headers = kwargs.pop("headers", {})
        headers.setdefault("Authorization", self.token)
        transport = httpx.AsyncHTTPTransport(uds=self.socket_path) if self.socket_path else None
        async with httpx.AsyncClient(transport=transport, timeout=30, follow_redirects=True) as client:
            response = await client.request(method, f"{self.base_url}/{path.lstrip('/')}", headers=headers, **kwargs)
            if response.status_code in (401, 403):
                detail = response.text[:300]
                if "INVALID TOKEN" in detail.upper() or "TOKEN EXPIRED" in detail.upper() or response.status_code == 401:
                    refreshed = ""
                    try:
                        from .fnos_session import refresh_fnos_token

                        refreshed = await refresh_fnos_token()
                    except Exception:
                        pass
                    if not refreshed:
                        preferred = _preferred_account()
                        # 2026-09-28：先按「已绑定账号」找，找不到再退一步按可写账号找。
                        candidates = _music_db_tokens()
                        ordered: list[str] = []
                        for candidate in candidates:
                            if candidate != self.token and candidate not in ordered:
                                ordered.append(candidate)
                        # 把「可写」token（access_mode='all'）提前，确保优先被验证采用
                        for writable_token in _music_db_tokens(writable_only=True):
                            if writable_token in ordered:
                                ordered.remove(writable_token)
                            if writable_token != self.token:
                                ordered.insert(0, writable_token)
                        for candidate in ordered:
                            if candidate == self.token:
                                continue
                            check = await client.get(
                                f"{self.base_url}/user/me",
                                headers={"Authorization": candidate},
                            )
                            try:
                                account_result = check.json()
                            except Exception:
                                continue
                            account = account_result.get("data") if isinstance(account_result, dict) else None
                            if check.status_code >= 400 or not isinstance(account_result, dict) or account_result.get("code") != 0 or not isinstance(account, dict):
                                continue
                            is_writable = bool(_account_is_writable(account))
                            binds_to_preferred = (
                                (not preferred.get("guid") or account.get("guid") == preferred.get("guid"))
                                and (not preferred.get("name") or account.get("name") == preferred.get("name"))
                            )
                            # 「可写」账号优先无条件采用；否则仍需匹配已绑定账号
                            if not (is_writable or binds_to_preferred):
                                continue
                            refreshed = candidate
                            save_fnos_account(account)
                            break
                    if refreshed and refreshed != self.token:
                        self.token = refreshed
                        save_fnos_token(refreshed)
                        retry_headers = dict(headers)
                        retry_headers["Authorization"] = refreshed
                        response = await client.request(method, f"{self.base_url}/{path.lstrip('/')}", headers=retry_headers, **kwargs)
                elif response.status_code == 403:
                    # 2026-09-28：飞牛对**不可写**账号的写操作返回 403 + "forbidden"
                    # （实测 trigger 是 user.shared_library_access_mode='none'，
                    #  不是 role）。HTTP 403 并非 token 失效，旧逻辑不会自愈。这里对
                    # **写操作**（POST/PUT/PATCH/DELETE）做一次「换可写 token 重试」：
                    # 只要本机存在 access_mode='all' 的账号，当前 token 就该被替换，
                    # 否则用户会看到「歌单建好了但一首歌都加不进去」。
                    if method.upper() in {"POST", "PUT", "PATCH", "DELETE"}:
                        for candidate in _music_db_tokens(writable_only=True):
                            if candidate == self.token:
                                continue
                            check = await client.get(f"{self.base_url}/user/me", headers={"Authorization": candidate})
                            try:
                                account_result = check.json()
                            except Exception:
                                continue
                            account = account_result.get("data") if isinstance(account_result, dict) else None
                            if check.status_code >= 400 or not isinstance(account_result, dict) or account_result.get("code") != 0 or not isinstance(account, dict):
                                continue
                            if not _account_is_writable(account):
                                continue
                            self.token = candidate
                            save_fnos_token(candidate)
                            save_fnos_account(account)
                            retry_headers = dict(headers)
                            retry_headers["Authorization"] = candidate
                            response = await client.request(method, f"{self.base_url}/{path.lstrip('/')}", headers=retry_headers, **kwargs)
                            break
        try:
            result = response.json()
        except Exception as exc:
            raise RuntimeError(f"飞牛音乐返回非 JSON: HTTP {response.status_code}") from exc
        if response.status_code >= 400 or not isinstance(result, dict) or result.get("code") != 0:
            raise RuntimeError(str(result.get("msg") or result.get("message") or f"HTTP {response.status_code}"))
        return result

    async def probe(self):
        if not os.path.exists(self.socket_path):
            return {"status": "socket_missing", "socket": self.socket_path}
        if not self.token:
            return {"status": "token_missing", "socket": self.socket_path}
        try:
            response = await self._request("GET", "/user/me")
            account = response.get("data")
            save_fnos_account(account)
            # 2026-09-28：把「当前绑定账号能否写歌单」显式暴露出来。
            # ⚠️ /user/me 不含 sharedLibraryAccessMode，无法直接判定；
            # 权威来源是飞牛库 user 表，用当前 token 反查其 access_mode。
            bound_guid = str((account or {}).get("guid") or "")
            access_mode = _music_db_access_mode_of_token(self.token, bound_guid)
            writable = access_mode != "none" if access_mode else None
            payload = {"status": "ready", "account": account, "socket": self.socket_path,
                       "automatic_token": bool(_music_db_tokens(1)),
                       "shared_library_access_mode": access_mode,
                       "writable_token_available": bool(_music_db_tokens(1, writable_only=True))}
            if writable is not None:
                payload["can_write_playlist"] = writable
            if writable is False:
                # 绑到了 access_mode='none' 的自动建号：歌单能建但加不进歌，
                # 提前报出来，免得又被误读成接口故障。
                payload["warning"] = (
                    "当前绑定的飞牛账号 shared_library_access_mode='none'，"
                    "无法把歌曲加入歌单（add-track 会返回 100004 forbidden）。"
                    "请在飞牛音乐里把该账号的共享库访问权限设为「全部」，"
                    "或改绑一个有权限的账号。"
                )
            return payload
        except Exception as exc:
            detail = str(exc)[:200]
            status = "expired" if any(word in detail.upper() for word in ("INVALID TOKEN", "UNAUTHORIZED", "TOKEN EXPIRED")) else "error"
            return {"status": status, "detail": detail, "socket": self.socket_path}

    async def account_candidates(self, probe_alive: bool = True) -> list[dict]:
        """列出本机飞牛账号候选（社区版 1.3.0 账号选择器数据源）。

        从飞牛库 user × user_token 聚合：账号名 / 角色 / 共享库访问模式 / token 数；
        probe_alive=True 时对每个账号最新 token 实测 /user/me（每账号最多测 2 个），
        给出 token_alive 与 writable，供界面多账号家庭 NAS 选择绑定。
        """
        path = _music_db_path()
        if not path.exists():
            return []
        try:
            uri = f"file:{path.as_posix()}?mode=ro"
            with sqlite3.connect(uri, uri=True, timeout=3) as connection:
                rows = connection.execute(
                    "select u.name, u.guid, u.role, ifnull(u.shared_library_access_mode,'') as access_mode, "
                    "       ut.token "
                    "from user u join user_token ut on ut.user_id = u.id "
                    "where u.status = 'active' and ut.token is not null and ut.token <> '' "
                    "order by u.name, ut.id desc"
                ).fetchall()
        except (sqlite3.Error, OSError):
            return []
        accounts: dict[str, dict] = {}
        for name, guid, role, access_mode, token in rows:
            key = str(name or "").strip().lower()
            if not key:
                continue
            item = accounts.setdefault(key, {
                "name": str(name).strip(), "guid": str(guid or ""), "role": str(role or ""),
                "access_mode": str(access_mode or ""), "tokens": []})
            if len(item["tokens"]) < 3:
                item["tokens"].append(str(token))
        result = []
        for item in accounts.values():
            alive = None
            if probe_alive:
                alive = False
                for token in item["tokens"][:2]:
                    if await self._raw_user_me(token):
                        alive = True
                        break
            result.append({
                "name": item["name"], "guid": item["guid"], "role": item["role"],
                "access_mode": item["access_mode"],
                "writable": item["access_mode"] == "all",
                "token_count": len(item["tokens"]),
                "token_alive": alive if probe_alive else None,
            })
        result.sort(key=lambda row: (not row["writable"], not bool(row["token_alive"]), row["name"]))
        return result

    async def _raw_user_me(self, token: str) -> dict | None:
        """用指定 token 直连 socket 实测 /user/me；code=0 返回账号 dict，否则 None。"""
        if not token or not os.path.exists(self.socket_path):
            return None
        transport = httpx.AsyncHTTPTransport(uds=self.socket_path) if self.socket_path else None
        try:
            async with httpx.AsyncClient(transport=transport, timeout=8, follow_redirects=True) as client:
                response = await client.get(f"{self.base_url}/user/me", headers={"Authorization": token})
            payload = response.json()
        except Exception:
            return None
        account = payload.get("data") if isinstance(payload, dict) else None
        if response.status_code >= 400 or not isinstance(payload, dict) or payload.get("code") != 0 or not isinstance(account, dict):
            return None
        return account

    async def self_heal_token(self, preferred_account: str | None = None) -> dict:
        """周期性 token 探活 + 自愈（1.0.10 新增；1.3.0 支持指定账号）。

        背景（2026-09-29 第 4 次复发定案）：
            磁盘持久化的 token，其在飞牛库 `user_token` 表中的**行会被物理删除**
            （用户在飞牛音乐侧重新登录 / 换设备登录 → 旧会话行被清除）——**不是过期**。
            而 `_request()` 里的 401 兜底重取只在**真的发生请求**时才触发；
            若实例长时间空闲（没有推送/同步请求），token 就一直烂在磁盘上，
            用户下次点开就是「登录失败」，日志里还零 auth 报错。
            → 本方法提供**主动探活**，由 scheduler 低频调用，补齐这个缺口。

        行为：
            1. 先跑一次 `probe()`。健康则直接返回，不做任何写操作。
            2. 不健康（token_missing / expired / error / 磁盘 token 已不在库中）时，
               从飞牛库按**白名单 + 可写**捞候选 token，逐个用 `/user/me` 实测，
               取**第一个真正可用**的（不盲信 `order by id desc`）。
            3. 实测通过后 `save_fnos_token` + `save_fnos_account` 原子写回，
               再复测一次确认读通道可用。

        返回 dict：{'status': 'healthy'|'healed'|'unrecoverable'|'no_token',
                    'account': str|None, 'detail': str}
        """
        before = await self.probe()
        if before.get("status") == "ready":
            account = (before.get("account") or {}).get("name")
            return {"status": "healthy", "account": account, "detail": ""}

        # 候选来源：白名单/账号锁（_allowed_accounts）已由 _music_db_tokens 内部生效，
        # 优先「可写」账号（access_mode='all'），保证修好后写通道也可用。
        candidates: list[str] = []
        for token in list(_music_db_tokens(writable_only=True)) + list(_music_db_tokens()):
            if token and token not in candidates:
                candidates.append(token)

        if preferred_account:
            preferred = str(preferred_account).strip().lower()
            candidates = [token for token in candidates
                          if preferred in [n.lower() for n in _music_db_names_of_token(token)]]
            if not candidates:
                return {"status": "unrecoverable", "account": None,
                        "detail": f"没有账号 {preferred} 的可用 token（可能已在飞牛音乐侧登出，请重新登录后再试）"}

        if not candidates:
            return {"status": "no_token", "account": None,
                    "detail": "飞牛库中没有符合白名单的可用 token（用户可能从未在该实例账号下登录过飞牛音乐）"}

        transport = httpx.AsyncHTTPTransport(uds=self.socket_path) if self.socket_path else None
        async with httpx.AsyncClient(transport=transport, timeout=20, follow_redirects=True) as client:
            for candidate in candidates:
                try:
                    check = await client.get(f"{self.base_url}/user/me",
                                             headers={"Authorization": candidate})
                    payload = check.json()
                except Exception:
                    continue
                account = payload.get("data") if isinstance(payload, dict) else None
                if (check.status_code >= 400 or not isinstance(payload, dict)
                        or payload.get("code") != 0 or not isinstance(account, dict)):
                    continue
                if not _account_is_writable(account):
                    # 不可写账号（access_mode='none'）：能读不能写，跳过继续找
                    continue
                # 实测可用 → 原子写回
                self.token = candidate
                save_fnos_token(candidate)
                save_fnos_account(account)
                return {"status": "healed", "account": account.get("name"),
                        "detail": f"从 {len(candidates)} 个候选中选中 {str(account.get('name'))}"
                                  f"（原状态 {before.get('status')}）"}

        return {"status": "unrecoverable", "account": None,
                "detail": f"{len(candidates)} 个候选 token 全部实测失败（原状态 {before.get('status')}）"}

    async def playlists(self) -> list[dict]:
        return _rows((await self._request("GET", "/playlist/list")).get("data"))

    async def search_tracks(self, keyword: str) -> list[dict]:
        return _rows((await self._request("GET", "/search/track", params={"q": keyword})).get("data"))

    async def playlist_tracks(self, guid: str) -> list[dict]:
        result: list[dict] = []
        for page in range(1, 1001):
            data = await self._request("GET", "/track/playlist-detail/list", params={"playlistGUID": guid, "page": page, "pageSize": 50})
            rows = _rows(data.get("data"))
            result.extend(rows)
            if len(rows) < 50:
                return result
        raise RuntimeError("飞牛歌单超过分页上限")

    async def _find_or_create_playlist(self, title: str, description: str) -> dict:
        exact = [row for row in await self.playlists() if str(row.get("name") or "") == title]
        if len(exact) > 1:
            # 2026-09-20 修复：同名多条此前直接抛异常，会把整条推送管线卡死在 502，
            # 用户只能手动进飞牛删重名 —— 而「重复创建一模一样的歌单」事故留下的
            # 重名恰恰是删不完的。改为复用最早创建的一条继续增量更新，把重复清单
            # 透传给调用方记录（不自动删除，避免误删用户手动挑中的那一份）。
            return {**exact[0], "_duplicates": [row.get("guid") for row in exact[1:]]}
        if exact:
            return exact[0]
        created = await self._request("POST", "/playlist/create", json={"name": title, "visibility": 1, "description": description})
        data = created.get("data")
        guid = data.get("guid") if isinstance(data, dict) else None
        if not guid:
            exact = [row for row in await self.playlists() if str(row.get("name") or "") == title]
            if len(exact) != 1:
                raise RuntimeError("飞牛歌单创建后无法唯一回读")
            return {**exact[0], "_created": True}
        return {"guid": guid, "name": title, "_created": True}

    async def _ensure_playlist_name(self, guid: str, title: str) -> dict:
        """复用已绑定歌单时把飞牛侧歌单名刷成来源最新名。

        雷达/专属歌单在源平台会改标题（「今天从《XX》听起|私人雷达」天天换歌），
        此前复用路径只同步曲目、从不碰名字 —— `_sync_cover` 也只在封面真正
        上传时才顺带 `/playlist/edit`，封面 URL 没变或上传被跳过时名字就永远
        停在首次推送那天的旧名。名字一致时不发请求；不带 coverId 的 edit
        只改名字，不动封面。

        2026-09-25 修复（BUG-002 标题部分）：旧实现要求 /playlist/list 里能看到
        目标 GUID 才改名，列表波动（实测时有时无，且服务端忽略分页参数）时
        静默 ``return False``，改名被无声跳过。现在：

        - 已绑定 GUID 本身就是身份，改名**不以列表可见性为前置条件**；
        - 列表可见且名字一致 → 跳过（省一次请求）；
        - 列表不可见或读取失败 → 仍然发起编辑，之后尝试回读确认；
        - 回读不到 → 如实标 ``unverified``，绝不再静默返回 False。

        返回 ``{status, name, before, requested, error, reason}``，
        status ∈ updated / unchanged / unverified / failed / skipped。
        """
        title = str(title or "").strip()
        if not title:
            return {"status": "skipped", "reason": "empty_title"}

        async def _visible_name() -> str | None:
            try:
                rows = await self.playlists()
            except Exception:
                return None
            current = next((row for row in rows if str(row.get("guid") or "") == guid), None)
            return str(current.get("name") or "") if current else None

        before = await _visible_name()
        if before is not None and before == title:
            return {"status": "unchanged", "name": before}
        try:
            await self._request("POST", "/playlist/edit", json={"guid": guid, "name": title})
        except Exception as exc:
            return {"status": "failed", "error": f"改名请求失败: {str(exc)[:200]}",
                    "before": before, "requested": title}
        # 回读确认；列表波动时可能读不到 —— 如实标 unverified（编辑请求已被
        # 服务端接受），不算成功也不算失败。
        after = await _visible_name()
        if after is None:
            return {"status": "unverified", "requested": title, "before": before,
                    "reason": "playlist_list_unavailable"}
        if after == title:
            return {"status": "updated", "before": before, "name": after}
        return {"status": "failed", "error": f"改名后回读不一致: {after!r}",
                "before": before, "requested": title}

    async def _match_track(self, track: dict) -> dict | None:
        # 第一优先：拿本地已下载文件的**真实路径**去飞牛库里直取 guid。
        #
        # track['file_path'] 是我们自己下载落盘的真实文件，飞牛索引的就是同一批文件，
        # 剥掉挂载根前缀后路径完全一致。这条路不依赖标题/歌手标签，因此
        # 能治「文件已下载、飞牛搜不到」——包括飞牛还没刮削元数据（title 仍是 'track'）、
        # 以及第三方音源改写了标签这两类最棘手的失配。
        file_path = str(track.get("file_path") or "").strip()
        if file_path:
            direct_guid = lookup_track_guid_by_library_path(file_path)
            if direct_guid:
                return {
                    "guid": direct_guid,
                    "title": track.get("title"),
                    "name": track.get("title"),
                    "matched_by": "library_path",
                }

        # 回退：按标题/歌手检索。老库或文件尚未入库时走这条。
        #
        # Older QQ imports may contain mojibake in the database fields while
        # the managed filename and its embedded tags remain correct. Try the
        # stored metadata first, then derive a clean title/artist from the
        # downloaded filename.
        sources = [track]
        if file_path:
            name = Path(file_path).name.rsplit('.', 1)[0]
            parts = re.split(r"\s+-\s+", name, maxsplit=1)
            if parts and parts[0].strip():
                derived = dict(track)
                derived["title"] = parts[0].strip()
                if len(parts) > 1 and parts[1].strip():
                    derived["artist"] = re.sub(r"\s+_\s+", " / ", parts[1].strip())
                sources.append(derived)
        for source in sources:
            matched = await self._match_track_source(source, expected_filename=file_path or None)
            if matched:
                return matched
        return None

    async def _match_track_source(self, track: dict, expected_filename: str | None = None) -> dict | None:
        title = str(track.get("title") or "").strip()
        if not title:
            return None
        raw_terms = title_search_terms(title)
        queries: list[str] = []
        for term in raw_terms:
            queries.append(term)
            if _TO_SIMPLIFIED:
                queries.append(_TO_SIMPLIFIED.convert(term))
            if _TO_TRADITIONAL:
                queries.append(_TO_TRADITIONAL.convert(term))
        # 本地文件名的整段主干也拿去搜一次：飞牛搜索命中文件名/路径时，
        # 这条 query 能把「标题对不上、但文件确实在库里」的歌捞出来。
        if expected_filename:
            stem = Path(str(expected_filename).replace("\\", "/")).name.rsplit('.', 1)[0].strip()
            if stem and stem != title:
                queries.append(stem)
        queries = list(dict.fromkeys(item.strip() for item in queries if item.strip()))
        base_title = queries[0] if queries else title
        artists = [part.strip() for part in re.split(r"\s*(?:/|&|,|，|、|;|；)\s*", str(track.get("artist") or "")) if part.strip()]
        artist_terms = list(artists)
        if _TO_SIMPLIFIED:
            artist_terms.extend(_TO_SIMPLIFIED.convert(artist) for artist in artists)
        if _TO_TRADITIONAL:
            artist_terms.extend(_TO_TRADITIONAL.convert(artist) for artist in artists)
        queries.extend(f"{artist} {base_title}" for artist in artist_terms[:4])
        album = str(track.get("album") or "").strip()
        if album:
            queries.append(album)
        rows: list[dict] = []
        seen: set[str] = set()
        for query in dict.fromkeys(queries[:12]):
            result = await self.search_tracks(query)
            for row in result:
                key = str(row.get("guid") or "")
                if key and key not in seen:
                    seen.add(key)
                    rows.append(row)
            matched = choose_match(track, rows, expected_filename=expected_filename)
            if matched:
                return matched
        return choose_match(track, rows, expected_filename=expected_filename)

    async def _sync_cover(self, guid: str, cover_url: str | None, title: str) -> dict:
        """上传并把封面绑定到飞牛歌单。

        2026-09-25 修复（BUG-002 封面部分）：旧实现在上传响应里解析不到
        coverId/id/guid 时静默返回 None —— 封面实际没换，整次推送仍报成功。
        现在返回结构化结果 ``{status, cover_id, error, response, before_fields}``，
        status ∈ skipped / set / unverified / failed；解析失败时记录响应片段
        供诊断（真实响应结构以部署后的写入验收实测为准，拿到后在这里补字段）。
        """
        if not cover_url:
            return {"status": "skipped", "reason": "no_source_cover"}
        try:
            async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
                image = await client.get(cover_url)
            image.raise_for_status()
        except Exception as exc:
            return {"status": "failed", "error": f"封面下载失败: {str(exc)[:200]}"}
        content_type = image.headers.get("content-type", "image/jpeg").split(";", 1)[0]
        extension = ".png" if "png" in content_type else ".webp" if "webp" in content_type else ".jpg"
        try:
            uploaded = await self._request(
                "POST",
                "/static/cover/playlist",
                files={"file": (f"cover{extension}", image.content, content_type)},
            )
        except Exception as exc:
            return {"status": "failed", "error": f"封面上传失败: {str(exc)[:200]}"}
        data = uploaded.get("data")

        def _pick(node: dict) -> str | None:
            # 飞牛真实响应结构未实测（写入验收待批准）；先按已知字段 + 常见命名
            # 逐个尝试，全部落空时记录响应片段，绝不静默当作成功。
            for key in ("coverId", "coverGUID", "cover_guid", "id", "guid"):
                value = node.get(key)
                if value:
                    return str(value)
            return None

        cover_id = None
        if isinstance(data, dict):
            cover_id = _pick(data)
            if not cover_id and isinstance(data.get("data"), dict):
                cover_id = _pick(data["data"])
        elif isinstance(data, (str, int)):
            cover_id = str(data)
        if not cover_id:
            return {"status": "unverified",
                    "error": "封面上传响应中未解析到封面 ID（封面文件已上传，但未绑定到歌单）",
                    "response": str(data)[:200]}
        try:
            # Keep the existing name when the server rejects optional description fields.
            await self._request("POST", "/playlist/edit", json={"guid": guid, "name": title, "coverId": cover_id})
        except Exception as exc:
            return {"status": "failed", "cover_id": cover_id,
                    "error": f"封面上传成功但绑定到歌单失败: {str(exc)[:200]}"}
        return {"status": "set", "cover_id": cover_id}

    async def create_or_update_playlist(
        self,
        title: str,
        description: str = "",
        cover_url: str | None = None,
        tracks: list[dict] | None = None,
        *,
        fnos_guid: str | None = None,
        sync_cover: bool = True,
        mirror: bool = False,
        progress_cb=None,
    ):
        """把一份来源歌单同步到飞牛音乐。

        mirror=False（默认，用于普通歌单）：只做增量追加，不删除飞牛歌单里已有的曲目，
        这样用户在飞牛侧手动添加的曲子不会被清掉。
        mirror=True（用于日推 / 排行榜）：严格镜像 —— 不在最新列表里的曲目会被移除，
        避免日推歌单只累积、永不更新。
        progress_cb(done, total, stage)：可选进度回调（社区版 1.3.2），匹配阶段逐首
        回调 stage='matching'，加歌完成回调 stage='indexing'；回调异常一律忽略。
        """
        # A saved fnOS GUID is the stable identity of a pushed playlist.  Never
        # fall back to name matching when one is supplied: a renamed target or
        # a duplicate title must not create a second playlist.
        reused_existing_playlist = bool(str(fnos_guid or "").strip())
        created_playlist = False
        relinked = False
        if reused_existing_playlist:
            guid = str(fnos_guid).strip()
            try:
                current_rows = await self.playlist_tracks(guid)
            except Exception as exc:
                detail = str(exc)
                if title.strip() and _looks_missing_resource(detail):
                    # 绑定的飞牛歌单已不存在（用户在飞牛侧删了它、或应用重装后 guid 失效）。
                    # 这在「取消推送」之后是常态。与其把推送永久卡死在
                    # 「绑定的飞牛歌单不存在或无法读取」，不如按标题重新建一个同名歌单并
                    # 回写新 guid —— 用户重新点一次推送，歌单就该回来。
                    playlist = await self._find_or_create_playlist(title, description)
                    created_playlist = bool(playlist.pop("_created", False))
                    current_rows = None
                    relinked = True
                else:
                    raise RuntimeError(f"绑定的飞牛歌单不存在或无法读取: {exc}") from exc
            else:
                playlist = {"guid": guid, "name": title}
        else:
            playlist = await self._find_or_create_playlist(title, description)
            created_playlist = bool(playlist.pop("_created", False))
            current_rows = None
        guid = str(playlist.get("guid") or "")
        if not guid:
            raise RuntimeError("飞牛歌单缺少 guid")
        # 2026-09-20 修复：复用已绑定歌单时，来源歌单的标题可能在源平台已经变了
        # （雷达歌单每天换标题、专属歌单换推荐语）。此前名字只有封面同步顺带刷新，
        # 封面没传成功就永远停在旧名 —— 这里显式把名字刷成来源最新名。
        # 2026-09-25（BUG-002）：标题同步返回结构化结果，不再静默跳过。
        if reused_existing_playlist and not relinked and not created_playlist:
            title_sync = await self._ensure_playlist_name(guid, str(title or ""))
        else:
            title_sync = {"status": "skipped", "reason": "playlist_created_or_relinked"}
        renamed = title_sync.get("status") == "updated"
        current = {str(row.get("guid")) for row in (current_rows if current_rows is not None else await self.playlist_tracks(guid)) if row.get("guid")}
        source_tracks = list(tracks or [])
        semaphore = asyncio.Semaphore(max(1, int(os.getenv("FNOS_MATCH_CONCURRENCY", "8"))))

        async def limited_match(track: dict):
            async with semaphore:
                result = await self._match_track(track)
            matched_count["done"] += 1
            if progress_cb:
                try:
                    progress_cb(matched_count["done"], total_tracks, "matching")
                except Exception:
                    pass
            return result

        matched_count = {"done": 0}
        total_tracks = len(source_tracks)
        rows = await asyncio.gather(*(limited_match(track) for track in source_tracks))
        matched, missing, item_results = [], [], []
        for track, row in zip(source_tracks, rows):
            if row and row.get("guid"):
                track_guid = str(row["guid"])
                matched.append(track_guid)
                item_results.append({
                    "platform": track.get("platform"), "song_id": track.get("song_id"),
                    "position": track.get("position"), "title": track.get("title"),
                    "artist": track.get("artist"), "fnos_track_guid": track_guid,
                })
            else:
                detail = {"platform": track.get("platform"), "song_id": track.get("song_id"),
                          "position": track.get("position"), "title": track.get("title"),
                          "artist": track.get("artist"), "reason": "飞牛曲库中没有唯一匹配的歌曲"}
                missing.append(detail)
                item_results.append({**detail, "fnos_track_guid": None})
        desired = list(dict.fromkeys(matched))
        desired_set = set(desired)
        add = [item for item in desired if item not in current]
        for index in range(0, len(add), 50):
            await self._request("POST", "/playlist/add-track", json={"guid": guid, "trackGUIDs": add[index:index + 50]})
        if progress_cb and source_tracks:
            try:
                progress_cb(total_tracks, total_tracks, "indexing")
            except Exception:
                pass

        # 严格镜像：移除已经不在最新列表里的曲目。
        # 安全护栏 —— 源列表为空、或本次一首都没匹配上时，一律不动已有歌单：
        # 这种情形几乎都是上游接口异常 / 登录失效 / 曲库索引未就绪，
        # 此时删歌会把用户歌单清空，代价远大于「本轮没更新」。
        remove: list[str] = []
        removal_skipped: str | None = None
        if mirror:
            if not source_tracks:
                removal_skipped = "来源歌单为空，已跳过移除"
            elif not desired:
                removal_skipped = "本轮未匹配到任何曲目，已跳过移除（疑似曲库索引或上游异常）"
            else:
                remove = [item for item in current if item not in desired_set]
                for index in range(0, len(remove), 50):
                    await self._request("POST", "/playlist/remove-track",
                                        json={"guid": guid, "trackGUIDs": remove[index:index + 50]})

        actual_rows = await self.playlist_tracks(guid)
        actual = {str(row.get("guid")) for row in actual_rows if row.get("guid")}
        for item in item_results:
            item["in_fnos_playlist"] = bool(item.get("fnos_track_guid") in actual)
        verified = len(set(desired) & actual)
        # 2026-09-18 事故修复：已入单但索引被判死（is_physical_file_deleted=1）的
        # guid 不能算「验证通过」。先立即请求一次全库扫描（文件稳定时通常一次即恢复，
        # 与飞牛自身扫描等效），恢复不了就如实从 verified 中剔除并标注，
        # 让推送管线走重试补录，而不是假报「全部推送成功」。
        stale_index: set[str] = set()
        try:
            stale_index = dead_fnos_guids(desired)
        except Exception:
            stale_index = set()
        if stale_index:
            try:
                await self.refresh_library()
            except Exception:
                pass
            stale_index = dead_fnos_guids(stale_index)
            verified = len({g for g in set(desired) & actual if g not in stale_index})
            for item in item_results:
                if item.get("fnos_track_guid") in stale_index:
                    item["in_fnos_playlist"] = False
                    item["reason"] = "飞牛索引标记文件缺失（索引竞态），已请求重新扫描并将自动补录"
        # 「总数」按去重后的曲目算：飞牛歌单本身不接受同一首歌重复入单，
        # 而来源歌单（尤其是 QQ/网易云的榜单）经常有重复曲目。
        # 用原始条数当分母会凭空多出 run_missing，让用户看到「少了几首」的假象。
        source_keys = {(str(track.get('platform') or ''), str(track.get('song_id') or ''))
                       for track in source_tracks}
        source_keys = {key for key in source_keys if key[1]}
        unique_total = len(source_keys) if source_keys else len(source_tracks)
        cover_sync = await self._sync_cover(guid, cover_url, title) if sync_cover else {"status": "skipped", "reason": "disabled"}
        cover_id = cover_sync.get("cover_id")
        return {"guid": guid, "total": unique_total, "items_total": len(source_tracks),
                "matched": len(desired), "verified": verified,
                "playlist_count": len(actual), "added": len(add), "removed": len(remove),
                "stale_index": len(stale_index),
                "already_present": len(set(desired) & current),
                "mirror": bool(mirror), "removal_skipped": removal_skipped,
                "missing": missing, "coverId": cover_id,
                "title_sync": title_sync, "cover_sync": cover_sync,
                "item_results": item_results,
                "reused_existing_playlist": reused_existing_playlist,
                "created_playlist": created_playlist,
                "relinked": relinked,
                "renamed": renamed,
                "duplicates": playlist.get("_duplicates") or []}

    async def refresh_library(self):
        """触发飞牛曲库重扫（尽力而为，绝不毒化调用方流程）。

        2026-09-28 修复（R10-2）：`/shared-library/scan-all` 是飞牛**管理员专属**
        接口 —— member 账号调用会拿到
        `100003 forbidden, admin only`。而本方法在推送链路里（新增曲目后）和
        下载链路里（落盘后刷新索引）都会被调用，一旦抛错就会把整次推送
        标记成 failed —— 症状正是「歌单推了一半，last_error=forbidden, admin only」。

        这一步只是「请飞牛尽快重新索引新文件」的提示，飞牛自身的定期扫描
        也会补上索引。因此对权限类拒绝**静默降级**（返回 skipped），
        其余错误照常抛出。
        """
        try:
            return await self._request("POST", "/shared-library/scan-all")
        except RuntimeError as exc:
            text = str(exc).lower()
            if "admin" in text or "forbidden" in text or "100003" in text:
                return {"status": "skipped", "reason": "admin_only_endpoint"}
            raise

    async def delete_playlist(self, guid: str) -> dict:
        """删除飞牛音乐里的歌单（连同其中的曲目）。

        飞牛的 playlist/delete 只删歌单记录，不动音乐库里的音频文件，
        因此这个操作对曲库是安全的、可重建的 —— 正是「取消推送」该有的收尾动作。
        已不存在的歌单按成功处理，保证取消操作可以重复执行。
        """
        target = str(guid or "").strip()
        if not target:
            return {"deleted": False, "reason": "guid_missing"}
        try:
            await self._request("POST", "/playlist/delete", json={"guid": target})
            return {"deleted": True, "guid": target}
        except RuntimeError as exc:
            detail = str(exc)
            if any(word in detail for word in ("不存在", "not found", "NOT FOUND", "invalid")):
                return {"deleted": False, "guid": target, "reason": "already_missing"}
            raise

    async def favorites(self):
        songs = []
        for page in range(1, 1001):
            data = await self._request("GET", "/favorite-track/list", params={"page": page, "size": 50})
            rows = _rows(data.get("data"))
            songs.extend(rows)
            if len(rows) < 50:
                break
        return {"songs": songs}

    async def play_history(self) -> list[dict]:
        rows: list[dict] = []
        for page in range(1, 101):
            data = await self._request('GET', '/play-history/list', params={'page': page, 'size': 50})
            batch = _rows(data.get('data'))
            rows.extend(batch)
            if len(batch) < 50:
                break
        return rows
