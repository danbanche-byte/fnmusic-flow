from __future__ import annotations

import asyncio
import json
import os
import secrets
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from fnos.client import FnosClient


_REFRESH_LOCK = asyncio.Lock()
_PENDING_LOCK = asyncio.Lock()
_PENDING_LOGINS: dict[str, Any] = {}


def _client_class():
    try:
        from fnos.client import FnosClient
    except ImportError as exc:
        raise RuntimeError("飞牛自动续期组件未安装，请更新 Docker 镜像") from exc
    return FnosClient


def _session_path() -> Path:
    return Path(os.getenv("FNOS_SESSION_FILE", "/config/fnos-session.json"))


def _endpoint() -> str:
    return os.getenv("FNOS_LOGIN_ENDPOINT", "127.0.0.1:5666").strip()


def load_fnos_session() -> dict[str, Any]:
    try:
        value = json.loads(_session_path().read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return {}
    return value if isinstance(value, dict) else {}


def save_fnos_session(value: dict[str, Any]) -> None:
    required = ("token", "long_token", "secret")
    if any(not str(value.get(key) or "").strip() for key in required):
        raise ValueError("飞牛长期会话信息不完整")
    path = _session_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)


def clear_fnos_session() -> None:
    _session_path().unlink(missing_ok=True)


def fnos_session_status() -> dict[str, Any]:
    value = load_fnos_session()
    return {
        "configured": bool(value.get("long_token") and value.get("secret")),
        "username": value.get("username"),
        "updated_at": value.get("updated_at"),
        "endpoint": value.get("endpoint") or _endpoint(),
    }


async def _close(client: Any) -> None:
    try:
        await client.close()
    except Exception:
        pass


def _final_credentials(client: Any, result: dict[str, Any], username: str, endpoint: str) -> dict[str, Any]:
    token = str(result.get("token") or client.token or "").strip()
    long_token = str(result.get("longToken") or client.long_token or "").strip()
    secret = str(client.get_decrypted_secret() or "").strip()
    if not token or not long_token or not secret:
        raise RuntimeError("飞牛登录成功，但没有返回可续期的长期会话")
    return {
        "token": token,
        "long_token": long_token,
        "secret": secret,
        "username": username,
        "endpoint": endpoint,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


async def login_fnos(username: str, password: str) -> dict[str, Any]:
    username = username.strip()
    if not username or not password:
        raise ValueError("飞牛账号和密码不能为空")
    endpoint = _endpoint()
    client = _client_class()()
    try:
        await client.connect(endpoint, timeout=8)
        result = await client.login(
            username,
            password,
            timeout=15,
            stay=True,
            device_type="Browser",
            device_name="fnmusic-flow",
        )
        if result.get("twofaRequired"):
            challenge_id = secrets.token_urlsafe(24)
            async with _PENDING_LOCK:
                old = list(_PENDING_LOGINS.values())
                _PENDING_LOGINS.clear()
                _PENDING_LOGINS[challenge_id] = client
            for pending in old:
                await _close(pending)
            return {"status": "twofa_required", "challenge_id": challenge_id}
        if result.get("twofaSetupRequired"):
            raise RuntimeError("该账号需要先在飞牛系统中完成两步验证绑定")
        if result.get("result") != "succ":
            raise RuntimeError(str(result.get("msg") or result.get("errmsg") or "飞牛账号登录失败"))
        credentials = _final_credentials(client, result, username, endpoint)
        save_fnos_session(credentials)
        from .fnos import save_fnos_token

        save_fnos_token(credentials["token"])
        return {"status": "ready", "username": username, "renewable": True}
    except Exception:
        await _close(client)
        raise
    finally:
        if not any(value is client for value in _PENDING_LOGINS.values()):
            await _close(client)


async def complete_fnos_twofa(challenge_id: str, code: str, trust_device: bool = True) -> dict[str, Any]:
    async with _PENDING_LOCK:
        client = _PENDING_LOGINS.pop(challenge_id, None)
    if not client:
        raise ValueError("登录验证已过期，请重新输入飞牛账号和密码")
    username = str((client.twofa_pending or {}).get("username") or "")
    try:
        result = await client.submit_twofa_code(code.strip(), trust_device=trust_device, timeout=15)
        if result.get("result") != "succ":
            raise RuntimeError(str(result.get("msg") or result.get("errmsg") or "两步验证码错误"))
        credentials = _final_credentials(client, result, username, _endpoint())
        save_fnos_session(credentials)
        from .fnos import save_fnos_token

        save_fnos_token(credentials["token"])
        return {"status": "ready", "username": username, "renewable": True}
    finally:
        await _close(client)


async def refresh_fnos_token() -> str:
    async with _REFRESH_LOCK:
        credentials = load_fnos_session()
        token = str(credentials.get("token") or "").strip()
        long_token = str(credentials.get("long_token") or "").strip()
        secret = str(credentials.get("secret") or "").strip()
        if not token or not long_token or not secret:
            raise RuntimeError("未配置可自动续期的飞牛长期会话")
        client = _client_class()()
        try:
            await client.connect(str(credentials.get("endpoint") or _endpoint()), timeout=8)
            result = await client.login_via_token(token, long_token, secret, timeout=15)
            refreshed = str(result.get("token") or client.token or "").strip()
            if not refreshed or (result.get("result") == "fail" and result.get("errno") not in (0, None)):
                raise RuntimeError(str(result.get("msg") or result.get("errmsg") or "飞牛长期会话续期失败"))
            credentials["token"] = refreshed
            credentials["updated_at"] = datetime.now(timezone.utc).isoformat()
            save_fnos_session(credentials)
            from .fnos import save_fnos_token

            save_fnos_token(refreshed)
            return refreshed
        finally:
            await _close(client)
