import asyncio
import base64
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
import httpx
from fastapi import Header, HTTPException, Request
from .app import app, db, guard, music_root
from .phase23 import init_phase23
from .recommendations import RecommendationProvider
from .fnos import FnosAdapter, save_fnos_token, load_account_lock, save_account_lock, _allowed_accounts
from .fnos_session import complete_fnos_twofa, fnos_session_status, login_fnos, refresh_fnos_token
from .playlist import identify
from .platforms import NeteaseConnector, QQMusicConnector, PlatformError
from .scheduler import SyncScheduler
from .for_you import (run_rotation, pool_items, pool_item, pool_status,
                      dislike_item, restore_item, blacklist_rows)
from .favorite_sync import sync_favorites
from .models import DEFAULT_QUALITY  # 2026-09-29：入库显式带音质，不依赖（老库仍是320k的）表默认值
from .library import annotate_items
from .artwork_repair import run_artwork_repair
from .daily_cache import (is_cache_path, promote_cached_track, record_play as record_cache_play,
                          summary as cache_summary)

init_phase23(db)

SUPPORTED_PROVIDERS = {'netease', 'qq'}
POCKETTUNE_API = os.getenv('POCKETTUNE_API_URL', 'http://127.0.0.1:3000/api').rstrip('/')
_QR_CHECK_CACHE: dict[tuple[str, str], tuple[float, dict]] = {}
_QR_CHECK_LOCKS: dict[str, asyncio.Lock] = {}

# ---------------------------------------------------------------------------
# 进程内 GET 响应缓存
# ---------------------------------------------------------------------------
# 上游一次完整拉取（我的歌单 + 专属 50 张 + 雷达 7 张）约 0.9 秒，而用户在同一
# 页面反复切换/重进会重复触发；缓存 60 秒可把重复访问降到 0 次上游请求。排行榜
# 同理（网易云 toplist 单次 1.3 秒）、广场 0.24 秒、日推 0.33 秒。
# TTL 均可用环境变量调整，设 0 即关闭。
#
# 放在模块顶部是刻意的：这些名字被下面多个路由引用，定义在使用点之前才不容易
# 在后续重排代码时引入 NameError。
_PLAYLIST_CACHE: dict[str, dict] = {}
_PLAYLIST_CACHE_TTL = float(os.getenv('PLAYLIST_CACHE_SECONDS', '60'))
_CHARTS_CACHE: dict[str, dict] = {}
_CHARTS_CACHE_TTL = float(os.getenv('CHARTS_CACHE_SECONDS', '600'))
# 歌单广场（/api/plaza）内容以「天」为单位变化，缓存 5 分钟不会有感知差异，
# 但能让「发现」页来回切换做到瞬时。
_PLAZA_CACHE: dict[str, dict] = {}
_PLAZA_CACHE_TTL = float(os.getenv('PLAZA_CACHE_SECONDS', '300'))
# 每日推荐（/api/platform/{provider}/daily）一天只变一次，缓存 2 分钟既保证
# 「刚推完就能看到」的时效，又避免切页重复打上游。
# 注意：只缓存**上游真实返回**的结果，登录失效时的兜底快照不写入，否则会把失效
# 状态一直粘住。
_DAILY_LIVE_CACHE: dict[str, dict] = {}
_DAILY_LIVE_CACHE_TTL = float(os.getenv('DAILY_LIVE_CACHE_SECONDS', '120'))


def _ttl_cache_get(store: dict, key: str, ttl: float) -> dict | None:
    """取缓存副本；命中时补上 source / cached_at / cache_age_seconds / stale。"""
    if ttl <= 0:
        return None
    hit = store.get(key)
    if not hit or time.time() - hit['at'] > ttl:
        return None
    payload = dict(hit['payload'])
    payload['source'] = 'memory'
    payload['cached_at'] = datetime.fromtimestamp(hit['at'], timezone.utc).isoformat()
    payload['cache_age_seconds'] = round(time.time() - hit['at'], 2)
    payload['stale'] = False
    return payload


def _ttl_cache_put(store: dict, key: str, payload: dict, ttl: float) -> None:
    if ttl > 0:
        store[key] = {'at': time.time(), 'payload': payload}


@app.get('/api/artwork/audit')
async def artwork_audit(scope: str = 'managed', x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    if scope not in {'managed', 'all'}:
        raise HTTPException(400, '封面扫描范围不正确')
    managed_filter = (" where exists(select 1 from tracks where tracks.file_path=library_files.path)"
                      if scope == 'managed' else '')
    counts = {row['artwork_status']: int(row['count']) for row in db.fetchall(
        'select artwork_status,count(*) count from library_files' + managed_filter + ' group by artwork_status'
    )}
    latest = db.fetchone('select * from artwork_jobs order by id desc limit 1')
    library_count = db.fetchone('select count(*) count from library_files') or {'count': 0}
    return {
        'total': sum(counts.values()), 'embedded': counts.get('embedded', 0),
        'missing': counts.get('missing', 0), 'unknown': counts.get('unknown', 0),
        'failed': counts.get('failed', 0), 'library_total': int(library_count['count']), 'scope': scope,
        'latest_job': latest,
    }


@app.post('/api/artwork/repair')
async def artwork_repair(payload: dict | None = None, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    running = db.fetchone("select * from artwork_jobs where status in ('pending','running') order by id desc limit 1")
    if running:
        return {'reused': True, 'job': running}
    limit_value = (payload or {}).get('limit')
    limit = min(100000, max(1, int(limit_value))) if limit_value else None
    managed_only = str((payload or {}).get('scope') or 'managed') != 'all'
    only_status = (payload or {}).get('only_status')
    if only_status not in (None, 'missing', 'unknown', 'failed'):
        raise HTTPException(400, '封面状态筛选值不正确')
    db.execute("insert into artwork_jobs(status) values('pending')")
    job = db.fetchone('select * from artwork_jobs order by id desc limit 1')
    task = asyncio.create_task(run_artwork_repair(db, int(job['id']), limit, managed_only, only_status))
    tasks = getattr(app.state, 'artwork_tasks', set())
    tasks.add(task)
    task.add_done_callback(tasks.discard)
    app.state.artwork_tasks = tasks
    return {'reused': False, 'job': job}


@app.get('/api/artwork/jobs/{job_id}')
async def artwork_job(job_id: int, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    job = db.fetchone('select * from artwork_jobs where id=?', (job_id,))
    if not job:
        raise HTTPException(404, '封面修复任务不存在')
    job['failures'] = db.fetchall(
        "select path,title,artist,artwork_error from library_files where artwork_status='failed' order by artwork_checked_at desc limit 100"
    )
    return job


def _qr_cached(provider: str, key: str) -> dict | None:
    cached = _QR_CHECK_CACHE.get((provider, key))
    if cached and time.monotonic() - cached[0] < 2.0:
        return dict(cached[1])
    return None


def _qr_store(provider: str, key: str, data: dict) -> None:
    if len(_QR_CHECK_CACHE) >= 64:
        _QR_CHECK_CACHE.clear()
    _QR_CHECK_CACHE[(provider, key)] = (time.monotonic(), dict(data))


def _qr_lock(provider: str) -> asyncio.Lock:
    return _QR_CHECK_LOCKS.setdefault(provider, asyncio.Lock())


async def _qq_wechat_qr_proxy() -> dict | None:
    """Optionally use a host-provided QQ WeChat QR generator.

    Some PocketTune builds receive an HTML AppID error from WeChat while the
    fnOS-installed build on the same NAS still has a valid QR flow.  Keep this
    as an explicit, deployment-only fallback: the public Docker image contains
    no NAS address and remains fully usable when no proxy is configured.
    """
    proxy = os.getenv('QQ_WECHAT_QR_PROXY_URL', '').strip().rstrip('/')
    if not proxy:
        return None
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get(f'{proxy}/api/accounts/qq/qr/key', params={'login_type': 'wx'})
            data = await _upstream_json(response, 'qq_wx_qr_proxy')
        if isinstance(data.get('content'), str) and data['content'].startswith('data:image/') and data.get('key'):
            return data
    except (HTTPException, httpx.HTTPError):
        return None
    return None


async def _upstream_json(response: httpx.Response, operation: str) -> dict:
    """Read a bundled PocketTune response without leaking HTML into the UI.

    The embedded services sometimes return a plain-text/HTML error page.  Calling
    ``response.json()`` directly made the browser report ``Unexpected token I``.
    Keep the API contract JSON-shaped and include only a short, safe diagnostic.
    """
    try:
        value = response.json()
    except (ValueError, TypeError) as exc:
        snippet = ' '.join(response.text.split())[:180]
        raise HTTPException(502, f'{operation}: upstream_non_json'
                            + (f' ({snippet})' if snippet else '')) from exc
    if not isinstance(value, dict):
        raise HTTPException(502, f'{operation}: upstream_invalid_json')
    if response.status_code >= 400:
        message = value.get('message') or value.get('msg') or value.get('error') or value.get('detail')
        suffix = f': {str(message)[:160]}' if message else ''
        raise HTTPException(502, f'{operation}: upstream_http_{response.status_code}{suffix}')
    return value


def _netease_qr_payload(value: dict, operation: str) -> dict:
    """Normalize Netease API variants while retaining the original fields."""
    data = value.get('data')
    if operation == 'key' and isinstance(data, str):
        value = {**value, 'data': {'unikey': data, 'key': data}}
    elif operation == 'create' and isinstance(data, str):
        value = {**value, 'data': {'qrimg': data}}
    if operation == 'key' and isinstance(value.get('data'), dict):
        nested = value['data']
        if nested.get('unikey') and not nested.get('key'):
            value = {**value, 'data': {**nested, 'key': nested['unikey']}}
    return value

@app.get('/api/integrated/status')
async def integrated_status(x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    return {
        'pockettune_bundled': True,
        'pockettune_api': POCKETTUNE_API,
        'providers': {'netease': {'login': 'qr_or_cookie', 'playlist': True, 'daily': True},
                      'qq': {'login': 'qr_or_cookie', 'playlist': True, 'daily': True}},
        'external_dependency': '飞牛音乐 API（目标媒体库）',
    }

@app.post('/api/sync/run')
async def run_sync(x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    return {'results': await SyncScheduler(db).run_once()}

@app.get('/api/sync/settings')
async def sync_settings(x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    return db.fetchall('select * from sync_settings order by name')


@app.get('/api/subscriptions')
async def subscriptions(x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    rows = db.fetchall(
        '''select s.id,s.provider,s.playlist_id,s.kind,s.enabled,s.interval_minutes,s.schedule_time,s.next_run_at,
                  s.last_run_at,s.last_status,s.last_error,s.target_title,p.title,c.cover_url
           from subscriptions s left join playlists p on p.provider=s.provider and p.playlist_id=s.playlist_id
           left join playlist_covers c on c.playlist_row_id=p.id order by s.id desc'''
    )
    # The time selected in the UI is always Beijing time.  Keep this explicit
    # in the response so clients do not reinterpret the stored UTC timestamp
    # as local time and show a misleading schedule.
    for row in rows:
        row['schedule_timezone'] = 'Asia/Shanghai'
    return rows


@app.patch('/api/subscriptions/{subscription_id}')
async def update_subscription(subscription_id: int, payload: dict, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    row = db.fetchone('select * from subscriptions where id=?', (subscription_id,))
    if not row:
        raise HTTPException(404, '订阅不存在')
    enabled = 1 if bool(payload.get('enabled', row['enabled'])) else 0
    # 间隔 0 = 不循环（只在每日锚点跑一次）。下限保留 5 分钟，防止误填打爆上游。
    try:
        interval = int(payload.get('interval_minutes', row.get('interval_minutes') or 0))
    except (TypeError, ValueError) as exc:
        raise HTTPException(400, '检查间隔必须是整数分钟') from exc
    if interval < 0 or interval > 525600:
        raise HTTPException(400, '检查间隔必须在 0（不循环）到 365 天之间')
    if 0 < interval < 5:
        raise HTTPException(400, '检查间隔不能小于 5 分钟')
    target_title = str(payload.get('target_title', row.get('target_title') or '')).strip() or None
    # 注意：payload.get 的默认值只在「键不存在」时生效，显式传 null 会拿到 None，
    # 经 str() 变成字面量 'None' 并撞上 HH:MM 校验。这里统一先判空再转字符串，
    # 让 None / '' / 缺省 三种写法都表示「清空锚点」。
    raw_anchor = payload.get('schedule_time', row.get('schedule_time'))
    schedule_time = str(raw_anchor).strip() if raw_anchor else None
    if schedule_time and _parse_hhmm(schedule_time) is None:
        raise HTTPException(400, '每日更新时刻必须是 HH:MM')
    # 关闭自动更新，或「不循环且没有每日锚点」时，一并落 enabled=0：调度器把
    # next_run_at is null 当成已到期，保留 enabled=1 会每 tick 反复触发。
    plan_next = _next_subscription_run(interval, schedule_time)
    if not enabled or plan_next is None:
        enabled, next_run = 0, None
    else:
        next_run = plan_next
    db.execute(
        '''update subscriptions set enabled=?,interval_minutes=?,schedule_time=?,target_title=?,next_run_at=?,
           last_status=case when ?=1 and last_status in ('paused','cancelled') then 'pending' when ?=0 then 'paused' else last_status end
           where id=?''',
        (enabled, interval, schedule_time, target_title, next_run, enabled, enabled, subscription_id),
    )
    return db.fetchone('select * from subscriptions where id=?', (subscription_id,))


@app.post('/api/subscriptions/{subscription_id}/run')
async def run_subscription(subscription_id: int, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    if not db.fetchone('select id from subscriptions where id=?', (subscription_id,)):
        raise HTTPException(404, '订阅不存在')
    result = await SyncScheduler(db).sync_subscriptions(subscription_id, force=True)
    return result


@app.delete('/api/subscriptions/{subscription_id}')
async def delete_subscription(subscription_id: int, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    subscription = db.fetchone('select * from subscriptions where id=?', (subscription_id,))
    if not subscription:
        raise HTTPException(404, '订阅不存在')
    target = db.fetchone(
        'select id from push_targets where provider=? and playlist_id=? and kind=?',
        (subscription['provider'], subscription['playlist_id'], subscription['kind']),
    )
    if target:
        return await cancel_push_target(int(target['id']), x_fn_token)
    db.execute(
        "update subscriptions set enabled=0,last_status='cancelled',next_run_at=null,last_error=null where id=?",
        (subscription_id,),
    )
    return {'status': 'cancelled', 'id': subscription_id,
            'message': '已取消后续自动推送；该规则没有关联的飞牛歌单'}


def _subscription_interval(kind: str, value: object = None) -> int:
    """返回检查间隔（分钟）；``0`` 表示「不循环」。

    注意这里**不能**再用 ``max(30, …)`` 收口：用户需要显式的 0 来表达
    「不循环」（只在每日锚点跑一次），而 30/60 分钟这类短周期现在由前端
    直接提供。下限保留 5 分钟，防止误填导致上游被高频打。
    """
    defaults = {'daily': 1440, 'chart': 10080, 'playlist': 1440, 'radar': 1440}
    try:
        interval = int(value) if value is not None else defaults.get(kind, 1440)
    except (TypeError, ValueError):
        interval = defaults.get(kind, 1440)
    if interval <= 0:
        return 0
    return min(525600, max(5, interval))


_BEIJING = timezone(timedelta(hours=8))
_MINUTES_PER_DAY = 1440


def _parse_hhmm(value: object) -> tuple[int, int] | None:
    try:
        hour, minute = (int(x) for x in str(value).split(':', 1))
    except (TypeError, ValueError):
        return None
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None
    return hour, minute


def _next_subscription_run(interval_minutes: int, schedule_time: str | None = None,
                           now: datetime | None = None) -> str | None:
    """两段式调度的下次执行时间：每日锚点 + 检查间隔，可组合、可单用。

    这是用户要求的核心语义（以日推为例：「每天 6 点更新一次，然后后面每小时
    检查一次状态，有更新就继续更新」）：

    ==================  ==========================================
    配置                 行为
    ==================  ==========================================
    只设锚点             每天该时刻跑一次（interval=0，即「不循环」）
    只设间隔             从现在起每 N 分钟跑一次
    锚点 + 间隔          每天锚点首跑，之后自锚点起每 N 分钟检查一次；
                         跨天则回到次日锚点重新开始
    都不设               返回 None，调用方据此关闭自动更新
    ==================  ==========================================

    间隔 ≥ 1 天时不做「当天重置」，而是把锚点对齐到该周期（例如每周定时）。
    时间一律按北京时间解释，存储为 UTC。
    """
    # 归一到 UTC：结果字符串必须与输入时刻的时区表示无关，否则传入带 +08:00
    # 的时刻会输出北京时间字面量，被调度器当成 UTC 读回后整体偏移 8 小时。
    moment = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    local = moment.astimezone(_BEIJING)
    anchor = _parse_hhmm(schedule_time) if schedule_time else None
    interval = int(interval_minutes or 0)

    if anchor:
        hour, minute = anchor
        today_anchor = local.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if interval <= 0:
            candidate = today_anchor if today_anchor > local else today_anchor + timedelta(days=1)
        elif interval >= _MINUTES_PER_DAY:
            candidate = (today_anchor if today_anchor > local
                         else today_anchor + timedelta(minutes=interval))
        elif local < today_anchor:
            # 还没走到今天锚点：先等锚点跑第一次
            candidate = today_anchor
        else:
            elapsed_minutes = (local - today_anchor).total_seconds() / 60
            steps = int(elapsed_minutes // interval) + 1
            candidate = today_anchor + timedelta(minutes=steps * interval)
            # 超出当天则回到次日锚点，保证「每天锚点重新开始」
            if candidate.date() != today_anchor.date():
                candidate = today_anchor + timedelta(days=1)
        return candidate.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

    if interval > 0:
        return (moment + timedelta(minutes=interval)).astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

    return None

# 2026-09-25 修复（BUG-001）：订阅 upsert 改为补丁语义。
# _UNSET 表示「请求未提供该字段」——更新已有订阅时保留原值，不再被
# 「订阅更新 / 重新推送」按钮隐式重置（曾把 06:00+60 分钟的雷达订阅覆盖成
# 无锚点+1440 分钟，导致平台刷新后整张歌单要等到次日才同步）。
_UNSET = object()

# 雷达首次订阅的产品默认值（2026-09-25 用户拍板：每天 06:00 起检查，
# 平台未刷新时每 60 分钟重试）。
_RADAR_DEFAULT_INTERVAL_MINUTES = 60
_RADAR_DEFAULT_SCHEDULE_TIME = '06:00'


def _upsert_subscription(provider: str, playlist_id: str, kind: str,
                         interval_minutes=_UNSET, target_title=_UNSET,
                         schedule_time=_UNSET) -> dict:
    """写入或更新订阅规则（两段式：每日锚点 + 检查间隔，补丁语义）。

    只有调用方明确提供的字段才覆盖：``_UNSET``（默认）在更新时保留库中原值，
    在新建时落 kind 默认值（雷达 = 06:00 + 60 分钟；其余 = 无锚点 +
    ``_subscription_interval(kind)`` 默认间隔）。显式传 ``None`` 的
    schedule_time 仍表示「清空锚点」；显式传间隔则按边界收口。

    ``interval_minutes=0`` 且未设每日锚点 = 「不自动更新」，此时必须落
    ``enabled=0``：调度器把 ``next_run_at is null`` 视作「已到期」，若仍保留
    ``enabled=1`` 会在每个 tick 被反复触发。
    """
    existing_row = db.fetchone(
        '''select id from subscriptions where provider=? and playlist_id=?
           order by case when enabled=1 then 0 else 1 end, id desc limit 1''',
        (provider, playlist_id),
    )
    existing = db.fetchone('select * from subscriptions where id=?', (existing_row['id'],)) if existing_row else None

    if interval_minutes is _UNSET:
        if existing:
            try:
                interval = int(existing.get('interval_minutes') or 0)
            except (TypeError, ValueError):
                interval = _subscription_interval(kind, None)
        elif kind == 'radar':
            interval = _RADAR_DEFAULT_INTERVAL_MINUTES
        else:
            interval = _subscription_interval(kind, None)
    else:
        interval = _subscription_interval(kind, interval_minutes)
    if schedule_time is _UNSET:
        if existing:
            anchor = existing.get('schedule_time')
        else:
            anchor = _RADAR_DEFAULT_SCHEDULE_TIME if kind == 'radar' else None
    else:
        anchor = (schedule_time or '').strip() or None
        if anchor and _parse_hhmm(anchor) is None:
            anchor = None
    next_run = _next_subscription_run(interval, anchor)
    enabled = 1 if next_run else 0
    if target_title is _UNSET:
        title = str((existing or {}).get('target_title') or '') or None
    else:
        title = str(target_title or '') or None
    if existing:
        # UNIQUE(provider,playlist_id,kind)：改 kind 前先给占位的旧行让位。
        conflict = db.fetchone(
            'select id from subscriptions where provider=? and playlist_id=? and kind=? and id<>?',
            (provider, playlist_id, kind, existing['id']),
        )
        if conflict:
            db.execute('update subscriptions set kind=? where id=?', (f'legacy_{conflict["id"]}', conflict['id']))
        db.execute(
            '''update subscriptions set kind=?,enabled=?,interval_minutes=?,schedule_time=?,
               next_run_at=?,last_run_at=CURRENT_TIMESTAMP,last_status='success',
               last_error=null,target_title=? where id=?''',
            (kind, enabled, interval, anchor, next_run, title, existing['id']),
        )
    else:
        db.execute(
            '''insert into subscriptions(provider,playlist_id,kind,enabled,interval_minutes,schedule_time,next_run_at,last_run_at,last_status,last_error,target_title)
               values(?,?,?,?,?,?,?,CURRENT_TIMESTAMP,'success',null,?)''',
            (provider, playlist_id, kind, enabled, interval, anchor, next_run, title),
        )
    return db.fetchone('select * from subscriptions where provider=? and playlist_id=? and kind=?',
                       (provider, playlist_id, kind))


RETENTION_MODES = {'permanent', '1d', '7d', 'custom'}


def _sql_now() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')


def _run_now() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat(sep=' ', timespec='milliseconds')


def _parse_client_time(value: object, field_name: str) -> datetime | None:
    text = str(value or '').strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace('Z', '+00:00'))
    except ValueError as exc:
        raise HTTPException(400, f'{field_name}格式不正确') from exc
    if parsed.tzinfo is None:
        # Browser datetime-local values contain "T" and use the NAS timezone.
        # SQLite timestamps contain a space and are stored as UTC.
        zone = timezone(timedelta(hours=8)) if 'T' in text else timezone.utc
        parsed = parsed.replace(tzinfo=zone)
    return parsed.astimezone(timezone.utc)


def _sql_time(value: datetime | None) -> str | None:
    return value.strftime('%Y-%m-%d %H:%M:%S') if value else None


def _stored_time(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc) if value else None


def _retention_expiry(mode: str, custom_value: object = None, *, start: datetime | None = None) -> str | None:
    if mode not in RETENTION_MODES:
        raise HTTPException(400, '有效期仅支持常驻、1 天、7 天或自定义')
    now = start or datetime.now(timezone.utc)
    if mode == 'permanent':
        return None
    if mode == '1d':
        return _sql_time(now + timedelta(days=1))
    if mode == '7d':
        return _sql_time(now + timedelta(days=7))
    custom = _parse_client_time(custom_value, '自定义到期时间')
    if custom is None or custom <= now:
        raise HTTPException(400, '自定义到期时间必须晚于当前时间')
    return _sql_time(custom)


def _push_history(target_id: int, action: str, status: str, detail: object = None) -> None:
    text = None
    if detail is not None:
        text = detail if isinstance(detail, str) else json.dumps(detail, ensure_ascii=False, separators=(',', ':'))
    db.execute(
        'insert into push_history(target_id,action,status,detail) values(?,?,?,?)',
        (target_id, action, status, (text or '')[:2000] or None),
    )


def _push_mode(kind: object, playlist_id: object) -> str:
    value = str(kind or '').strip()
    identity = str(playlist_id or '').strip()
    return 'mirror' if value in {'daily', 'chart', 'radar'} or identity == 'daily' or identity.startswith('chart:') else 'append'


_RADAR_PLAYLIST_IDS = {'3136952023', '8402996200', '5320167908', '5327906368',
                       '5362359247', '5300458264', '5341776086'}


def _canonical_kind(kind: object, playlist_id: object, title: object = '') -> str:
    """把歌单分类收敛到唯一稳定值，供 push_targets / subscriptions 做身份键。

    2026-09-20 修复：同一张网易云雷达歌单会因推送入口不同（专属歌单卡片、
    雷达卡片、个性化广场）被记成 'playlist' / 'radar' 两种 kind，而
    push_targets / subscriptions 都以 (provider, playlist_id, kind) 为唯一键
    —— kind 分裂导致同源歌单出现两条目标，各自按名字找飞牛歌单，名字一变
    （雷达歌单天天换标题）就重复新建。这里统一判定：
    daily / chart 按身份键优先；固定雷达 ID 清单或标题含「雷达」归 radar；
    其余（含 exclusive —— 专属歌单在飞牛侧并无独立形态）归 playlist。
    """
    value = str(kind or '').strip()
    identity = str(playlist_id or '').strip()
    name = str(title or '')
    if identity == 'daily' or value == 'daily':
        return 'daily'
    if identity.startswith('chart:') or value == 'chart':
        return 'chart'
    if value == 'radar' or identity in _RADAR_PLAYLIST_IDS or '雷达' in name:
        return 'radar'
    return 'playlist'


def _find_target_for_source(provider: str, playlist_id: str, kind: str) -> dict | None:
    """按来源身份找推送目标，kind 只作首选过滤而非硬条件。

    2026-09-20 修复「重复创建一模一样的歌单」：旧逻辑严格按
    (provider, playlist_id, kind) 匹配，历史数据里同一张雷达歌单存了
    'playlist' 与 'radar' 两条目标，新入口匹配不到就当全新歌单推 ——
    按（当时）标题找飞牛歌单找不到就再建一个。现在先按精确键找，
    找不到就回退到同源任意 kind 的目标（优先 active 且已绑定飞牛歌单的），
    让调用方直接收养既有绑定，从根上消灭重复创建。
    """
    exact = db.fetchone(
        '''select * from push_targets where provider=? and playlist_id=? and kind=?
           order by case status when 'active' then 0 when 'scheduled' then 1 else 2 end, id desc''',
        (provider, playlist_id, kind),
    )
    if exact and (exact.get('fnos_guid') or exact.get('status') == 'active'):
        return exact
    # 精确键命中但既没绑定也不活跃（如取消后残留的空壳行）时，同源里若有
    # 已绑定飞牛歌单的目标，优先收养那一行 —— 绑定比 kind 更珍贵。
    adopted = db.fetchone(
        '''select * from push_targets where provider=? and playlist_id=?
           order by case status when 'active' then 0 when 'scheduled' then 1 else 2 end,
                  case when fnos_guid is not null then 0 else 1 end, id desc''',
        (provider, playlist_id),
    )
    if adopted:
        return adopted
    return exact


def _bound_fnos_guid(provider: str, playlist_id: str) -> str | None:
    """取同源任意目标上已绑定的飞牛歌单 guid（kind 不敏感）。

    push_targets 以 (provider, playlist_id, kind) 为历史唯一键，kind 分裂的
    老数据可能把唯一的飞牛绑定挂在另一条目标上。推送前先扫一眼，能捞回
    绑定就绝不走按标题匹配 —— 标题匹配正是重复建单的源头。
    """
    row = db.fetchone(
        '''select fnos_guid from push_targets where provider=? and playlist_id=?
           and fnos_guid is not null and fnos_guid<>'' order by id desc limit 1''',
        (provider, playlist_id),
    )
    return (row or {}).get('fnos_guid') or None


def _attach_track_paths(items: list[dict]) -> list[dict]:
    paths = {
        (str(row['platform']), str(row['song_id'])): row['file_path']
        for row in db.fetchall("select platform,song_id,file_path from tracks where file_path is not null and file_path<>''")
    }
    for item in items:
        key = (str(item.get('platform') or ''), str(item.get('song_id') or ''))
        if not item.get('file_path') and paths.get(key):
            item['file_path'] = paths[key]
    return items


def _record_push_item_status(target_id: int, playlist: dict, result: dict) -> None:
    result_by_key = {
        (str(item.get('platform') or ''), str(item.get('song_id') or '')): item
        for item in result.get('item_results') or []
    }
    rows = []
    for item in playlist.get('items') or []:
        platform, song_id = str(item.get('platform') or ''), str(item.get('song_id') or '')
        pushed = result_by_key.get((platform, song_id), {})
        track = db.fetchone('select id,file_path,status from tracks where platform=? and song_id=?', (platform, song_id)) or {}
        task = db.fetchone('select status,error from tasks where track_id=? order by id desc limit 1', (track.get('id'),)) if track.get('id') else None
        fnos_track_guid = pushed.get('fnos_track_guid')
        if pushed.get('in_fnos_playlist'):
            status, reason = 'pushed', None
        elif fnos_track_guid:
            status, reason = 'ready_to_append', '飞牛曲库已识别，等待补录到歌单'
        elif track.get('file_path'):
            status, reason = 'fnos_match_failed', '文件已下载，但飞牛曲库没有找到唯一匹配；可能仍在索引或歌曲标签存在差异'
        elif task and task.get('status') == 'failed_final':
            status, reason = 'download_failed', task.get('error') or '下载失败'
        elif task and task.get('status') in {'pending', 'matching', 'downloading'}:
            status, reason = 'downloading', {'pending': '等待下载', 'matching': '正在匹配音源', 'downloading': '正在下载'}[task['status']]
        else:
            status, reason = 'not_downloaded', '本地没有文件，且当前没有下载任务'
        rows.append((target_id, platform, song_id, int(item.get('position') or 0),
                     str(item.get('title') or ''), str(item.get('artist') or ''), status,
                     str(reason or '') or None, fnos_track_guid))
    with db.transaction() as conn:
        conn.execute('delete from push_item_status where target_id=?', (target_id,))
        conn.executemany(
            '''insert into push_item_status(target_id,platform,song_id,position,title,artist,status,reason,fnos_track_guid)
               values(?,?,?,?,?,?,?,?,?)''', rows,
        )
        conn.commit()


def _upsert_push_target(playlist: dict, kind: str, title: str, result: dict, options: dict) -> dict:
    provider = str(playlist['provider'])
    playlist_id = str(playlist['playlist_id'])
    retention = str(options.get('retention_mode') or 'permanent')
    expires_at = _retention_expiry(retention, options.get('expires_at'))
    guid = str(result.get('guid') or '').strip() or None
    # 2026-09-20 修复「重复创建一模一样的歌单」：旧实现以 (provider, playlist_id, kind)
    # 为唯一键，同一来源歌单经不同入口推送会因 kind 分裂（playlist/radar）插入
    # 第二条目标，之后各自按标题找飞牛歌单、名字一变就再建一个新歌单。
    # 现在以 (provider, playlist_id) 为准：已有目标一律原地归一（kind 收敛为
    # canonical 值、fnos_guid 保留既有绑定），不再产生同源第二条。
    existing_row = db.fetchone(
        '''select id from push_targets where provider=? and playlist_id=?
           order by case status when 'active' then 0 when 'scheduled' then 1 else 2 end,
                  case when fnos_guid is not null then 0 else 1 end, id desc limit 1''',
        (provider, playlist_id),
    )
    if existing_row:
        # UNIQUE(provider,playlist_id,kind)：收养的行要改成 canonical kind 时，
        # 同源占着该 kind 的旧行先让位（标记 legacy），避免唯一键冲突。
        conflict = db.fetchone(
            'select id from push_targets where provider=? and playlist_id=? and kind=? and id<>?',
            (provider, playlist_id, kind, existing_row['id']),
        )
        if conflict:
            db.execute('update push_targets set kind=? where id=?', (f'legacy_{conflict["id"]}', conflict['id']))
        db.execute(
            '''update push_targets set playlist_row_id=?,kind=?,target_title=?,
               fnos_guid=coalesce(?,fnos_guid),status='active',retention_mode=?,expires_at=?,
               scheduled_push_at=null,scheduled_auto_sync=null,scheduled_interval_minutes=null,
               last_pushed_at=CURRENT_TIMESTAMP,last_error=null,updated_at=CURRENT_TIMESTAMP where id=?''',
            (playlist.get('id'), kind, title, guid, retention, expires_at, existing_row['id']),
        )
    else:
        db.execute(
            '''insert into push_targets(provider,playlist_id,playlist_row_id,kind,target_title,fnos_guid,status,
                      retention_mode,expires_at,scheduled_push_at,last_pushed_at,last_error,updated_at)
               values(?,?,?,?,?,?,'active',?,?,null,CURRENT_TIMESTAMP,null,CURRENT_TIMESTAMP)''',
            (provider, playlist_id, playlist.get('id'), kind, title, guid, retention, expires_at),
        )
    target = db.fetchone(
        'select * from push_targets where provider=? and playlist_id=? and kind=?',
        (provider, playlist_id, kind),
    )
    started_at = str(options.get('_run_started_at') or _run_now())
    # 2026-09-29（1.0.7）：total 改为按 (platform, song_id) 去重计数，与
    # scheduler.process_push_runs / adapter.verified 口径一致 —— 源歌单自带
    # 重复曲目时，旧口径 total=60 vs verified=58（去重 guid 数）永远差 2，
    # completed 判定永假，快照假 running/missing（Queen 实测）。
    _items = playlist.get('items') or []
    total = len({(str(i.get('platform')), str(i.get('song_id'))) for i in _items}) \
        or int(result.get('total') or len(_items))
    matched = min(total, int(result.get('verified', result.get('matched', 0)) or 0))
    completed = matched >= total
    queue = options.get('_queue') or {}
    stage = 'completed' if completed else ('downloading' if int(queue.get('tasks_created', 0)) + int(queue.get('active_reused', 0)) else 'indexing')
    completed_at = _run_now() if completed else None
    duration = ((datetime.now(timezone.utc) - _stored_time(started_at)).total_seconds()
                if completed and _stored_time(started_at) else None)
    db.execute(
        '''update push_targets set run_started_at=?,run_completed_at=?,run_status=?,run_stage=?,
           run_total=?,run_matched=?,run_missing=?,run_duration_seconds=?,run_last_checked_at=CURRENT_TIMESTAMP,
           run_scan_requested_at=null,run_attempts=0 where id=?''',
        (started_at, completed_at, 'completed' if completed else 'running', stage,
         total, matched, max(0, total - matched), duration, target['id']),
    )
    target = db.fetchone('select * from push_targets where id=?', (target['id'],))
    _record_push_item_status(target['id'], playlist, result)
    # 2026-09-25（BUG-002/A2）：歌曲、标题、封面是三个独立同步对象，
    # 历史里分项记录各自状态（updated/unchanged/unverified/failed/skipped）。
    _push_history(target['id'], str(options.get('_action') or 'push'), 'success' if completed else 'running', {
        'matched': result.get('matched', 0), 'added': result.get('added', 0),
        'verified': matched, 'total': total, 'stage': stage,
        'retention_mode': retention, 'expires_at': expires_at,
        'title_updated': (result.get('title_sync') or {}).get('status'),
        'cover_updated': (result.get('cover_sync') or {}).get('status'),
    })
    return target


def _target_with_details(target_id: int) -> dict | None:
    return db.fetchone(
        '''select t.*,s.id subscription_id,s.enabled auto_sync,s.interval_minutes,s.schedule_time,
                  s.next_run_at,
                  s.last_run_at sync_last_run_at,s.last_status sync_status,p.title source_title,c.cover_url
           from push_targets t
           left join subscriptions s on s.provider=t.provider and s.playlist_id=t.playlist_id and s.kind=t.kind
           left join playlists p on p.id=t.playlist_row_id
           left join playlist_covers c on c.playlist_row_id=t.playlist_row_id
           where t.id=?''',
        (target_id,),
    )


def _saved_target_playlist(target: dict) -> dict:
    playlist = None
    if target.get('playlist_row_id'):
        playlist = db.fetchone(
            'select p.*,c.cover_url,c.description,c.owner from playlists p left join playlist_covers c on c.playlist_row_id=p.id where p.id=?',
            (target['playlist_row_id'],),
        )
    if not playlist:
        playlist = db.fetchone(
            'select p.*,c.cover_url,c.description,c.owner from playlists p left join playlist_covers c on c.playlist_row_id=p.id where p.provider=? and p.playlist_id=?',
            (target['provider'], target['playlist_id']),
        )
    if not playlist:
        raise HTTPException(409, '本地没有该歌单快照，请先重新打开来源歌单')
    playlist['items'] = _annotate_library(db.fetchall(
        'select position,platform,song_id,title,artist,album,duration_ms,cover_url from playlist_items where playlist_id=? order by position',
        (playlist['id'],),
    ))
    _attach_track_paths(playlist['items'])
    return playlist


async def _execute_push_target(target_id: int, action: str = 'repush') -> dict:
    target = _target_with_details(target_id)
    if not target:
        raise HTTPException(404, '推送记录不存在')
    playlist = _saved_target_playlist(target)
    started_at = _run_now()
    db.execute(
        """update push_targets set run_started_at=?,run_completed_at=null,run_status='running',
           run_stage='matching',run_total=?,run_matched=0,run_missing=?,run_duration_seconds=null,
           run_scan_requested_at=null,run_attempts=0,last_error=null,updated_at=CURRENT_TIMESTAMP where id=?""",
        (started_at, len(playlist.get('items') or []), len(playlist.get('items') or []), target_id),
    )
    try:
        # 2026-09-20 修复：重推/补录时标题优先取快照最新名（雷达歌单天天换标题），
        # 旧实现的固定 target_title 会把飞牛歌单名永远钉在首次推送那天的旧名；
        # fnos_guid 兜底扫同源任意目标，kind 分裂的老数据也能继承既有绑定。
        push_title = (str(playlist.get('title') or '').strip()
                      or str(target.get('target_title') or '').strip())
        result = await FnosAdapter(os.getenv('FNOS_API_URL'), os.getenv('FNOS_TOKEN')).create_or_update_playlist(
            push_title, playlist.get('description', ''), playlist.get('cover_url'), playlist.get('items', []),
            fnos_guid=target.get('fnos_guid') or _bound_fnos_guid(target['provider'], str(target['playlist_id'])),
            mirror=_push_mode(target.get('kind'), target.get('playlist_id')) == 'mirror',
        )
    except Exception as exc:
        error = str(exc)[:300]
        db.execute(
            "update push_targets set last_error=?,updated_at=CURRENT_TIMESTAMP where id=?",
            (error, target_id),
        )
        _push_history(target_id, action, 'failed', error)
        raise HTTPException(502, f'飞牛歌单同步失败: {error}') from exc
    expires_at = _retention_expiry(str(target.get('retention_mode') or 'permanent'), target.get('expires_at'))
    scheduled_auto_sync = target.get('scheduled_auto_sync')
    scheduled_interval = target.get('scheduled_interval_minutes')
    queue = await _enqueue_playlist_items(playlist.get('items', []), 4)
    _record_push_item_status(target_id, playlist, result)
    # 2026-09-29（1.0.7）：total 与 _upsert_push_target / scheduler 统一为去重口径
    # （见 _upsert_push_target 内注释）。
    _items = playlist.get('items') or []
    total = len({(str(i.get('platform')), str(i.get('song_id'))) for i in _items}) \
        or int(result.get('total') or len(_items))
    matched = min(total, int(result.get('verified', result.get('matched', 0)) or 0))
    completed = matched >= total
    stage = 'completed' if completed else ('downloading' if queue['tasks_created'] + queue['active_reused'] else 'indexing')
    completed_at = _run_now() if completed else None
    duration = ((datetime.now(timezone.utc) - _stored_time(started_at)).total_seconds() if completed else None)
    db.execute(
        '''update push_targets set fnos_guid=coalesce(?,fnos_guid),status='active',expires_at=?,
           scheduled_push_at=null,scheduled_auto_sync=null,scheduled_interval_minutes=null,
           last_pushed_at=CURRENT_TIMESTAMP,last_error=null,run_completed_at=?,run_status=?,run_stage=?,
           run_total=?,run_matched=?,run_missing=?,run_duration_seconds=?,run_last_checked_at=CURRENT_TIMESTAMP,
           target_title=case when ?<>'' then ? else target_title end,
           updated_at=CURRENT_TIMESTAMP where id=?''',
        (str(result.get('guid') or '').strip() or None, expires_at, completed_at,
         'completed' if completed else 'running', stage, total, matched, max(0, total - matched), duration,
         push_title, push_title, target_id),
    )
    if scheduled_auto_sync is not None:
        if scheduled_auto_sync:
            # 2026-09-25 修复（BUG-001）：预约推送完成后的订阅回写同样走补丁
            # 语义 —— 预约时未设置间隔就保留原值，且不再隐式清空每日锚点。
            _upsert_subscription(
                target['provider'], target['playlist_id'], target['kind'],
                _subscription_interval(target['kind'], scheduled_interval)
                if scheduled_interval is not None else _UNSET,
                target['target_title'],
            )
        else:
            db.execute(
                "update subscriptions set enabled=0,last_status='paused' where provider=? and playlist_id=? and kind=?",
                (target['provider'], target['playlist_id'], target['kind']),
            )
    _push_history(target_id, action, 'success' if completed else 'running',
                  {'matched': result.get('matched', 0), 'verified': matched, 'total': total,
                   'stage': stage, 'added': result.get('added', 0), 'expires_at': expires_at,
                   'title_updated': (result.get('title_sync') or {}).get('status'),
                   'cover_updated': (result.get('cover_sync') or {}).get('status')})
    return {'status': 'completed' if completed else 'running', 'target': _target_with_details(target_id), 'result': result, 'queue': queue}

def _provider_name(provider: str) -> str:
    provider = provider.lower().strip()
    if provider not in SUPPORTED_PROVIDERS:
        raise HTTPException(400, '仅支持 netease 或 qq')
    return provider

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _netease_identity(data: object) -> tuple[str | None, dict]:
    root = data.get('data') if isinstance(data, dict) and isinstance(data.get('data'), dict) else data
    root = root if isinstance(root, dict) else {}
    profile = root.get('profile') if isinstance(root.get('profile'), dict) else {}
    account = root.get('account') if isinstance(root.get('account'), dict) else {}
    uid = profile.get('userId') or profile.get('id') or account.get('id') or account.get('userId')
    return (str(uid) if uid else None), (profile or account)


def _is_netease_auth_cookie(cookie: str) -> bool:
    keys = {piece.strip().partition('=')[0] for piece in cookie.split(';') if '=' in piece}
    return bool(keys & {'MUSIC_U', 'MUSIC_A'})

@app.get('/api/accounts')
async def accounts(x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    return db.fetchall('select provider,display_name,status,last_checked_at,error,updated_at from accounts order by provider')

@app.post('/api/accounts/{provider}/connect')
async def connect_account(provider: str, payload: dict, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token); provider = _provider_name(provider)
    # 换账号/换 Cookie 后必须立刻丢弃旧账号的歌单缓存，否则最长 60 秒内
    # 用户仍会看到上一个账号的歌单。
    _playlist_cache_clear(provider)
    endpoint = str(payload.get('endpoint') or '').strip()
    cookie = str(payload.get('cookie') or '').strip()
    if provider == 'netease' and not endpoint:
        endpoint = f'{POCKETTUNE_API}/netease'
    status, error = ('configured', None)
    if cookie and provider == 'netease':
        try:
            uid, _ = _netease_identity(await NeteaseConnector(cookie).call('login/status', {'timestamp': int(time.time() * 1000)}))
            status = 'connected' if uid else 'expired'
            error = None if uid else 'netease_session_invalid'
        except Exception as exc:
            status, error = 'error', str(exc)[:200]
    elif cookie and provider == 'qq':
        connector = QQMusicConnector(cookie)
        cookies = connector._cookies()
        valid = bool(connector.account_id() and (cookies.get('qm_keyst') or cookies.get('qqmusic_key')))
        status, error = ('connected', None) if valid else ('expired', 'qq_session_invalid')
    if provider == 'qq' and not endpoint:
        endpoint = 'builtin://qqmusic'
    db.execute('insert or replace into accounts(provider,display_name,endpoint,cookie,status,error,updated_at) values(?,?,?,?,?,?,CURRENT_TIMESTAMP)',
               (provider, payload.get('display_name') or provider, endpoint, cookie or None, status, error))
    return {'provider': provider, 'status': status, 'error': error, 'login_required': status != 'connected'}

@app.get('/api/accounts/netease/qr/key')
async def netease_qr_key(x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    async with httpx.AsyncClient(timeout=20) as client:
        response = await client.get(f'{POCKETTUNE_API}/netease/login/qr/key', params={'noCookie':'true','timestamp':int(datetime.now().timestamp()*1000)})
        return _netease_qr_payload(await _upstream_json(response, 'netease_qr_key'), 'key')

@app.get('/api/accounts/netease/qr/create')
async def netease_qr_create(key: str, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    async with httpx.AsyncClient(timeout=20) as client:
        response = await client.get(f'{POCKETTUNE_API}/netease/login/qr/create', params={'key':key,'qrimg':'true','noCookie':'true','timestamp':int(datetime.now().timestamp()*1000)})
        return _netease_qr_payload(await _upstream_json(response, 'netease_qr_create'), 'create')

@app.get('/api/accounts/netease/qr/check')
async def netease_qr_check(key: str, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    if cached := _qr_cached('netease', key):
        return cached
    async with _qr_lock('netease'):
        if cached := _qr_cached('netease', key):
            return cached
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get(f'{POCKETTUNE_API}/netease/login/qr/check', params={'key':key,'noCookie':'false','timestamp':int(datetime.now().timestamp()*1000)})
            data = await _upstream_json(response, 'netease_qr_check')
        code = data.get('code') or ((data.get('data') or {}).get('code') if isinstance(data.get('data'), dict) else None)
        cookie = str(data.get('cookie') or '')
        if code == 803 and not cookie:
            cookie = '; '.join(f'{name}={value}' for name, value in response.cookies.items())
        if code == 803 and _is_netease_auth_cookie(cookie):
            data['cookie'] = cookie
            db.execute('insert or replace into accounts(provider,display_name,endpoint,cookie,status,updated_at) values(?,?,?,?,?,CURRENT_TIMESTAMP)',
                       ('netease','网易云音乐',f'{POCKETTUNE_API}/netease',cookie,'connected'))
        else:
            data.pop('cookie', None)
        _qr_store('netease', key, data)
        return data

@app.get('/api/accounts/qq/qr/key')
async def qq_qr_key(login_type: str = 'qq', x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    if login_type not in {'qq', 'wx'}:
        raise HTTPException(400, 'login_type 仅支持 qq 或 wx')
    async with httpx.AsyncClient(timeout=20) as client:
        response = await client.get(f'{POCKETTUNE_API}/qqmusic/login/qr/key', params={'type': login_type})
        data = await _upstream_json(response, f'qq_{login_type}_qr_key')
        # An expired WeChat Open Platform AppID can come back as HTTP 200 with
        # an HTML error page embedded in ``content``.  Older versions tried to
        # detect that page by looking for the words ``appid`` and ``error`` in
        # the decoded first 512 bytes.  That is unsafe: a perfectly valid JPEG
        # is binary data and may contain those byte sequences by chance, which
        # made the Docker build reject a usable QR code (502) while the fnOS
        # build accepted it.  Only reject an unambiguous HTML payload now.
        if login_type == 'wx' and isinstance(data.get('content'), str):
            content = data['content']
            if content.startswith('data:image/'):
                encoded = content.split(',', 1)[1] if ',' in content else ''
                try:
                    marker = base64.b64decode(encoded, validate=False)[:1024].lstrip().lower()
                except (ValueError, TypeError):
                    marker = b''
                # Do not inspect arbitrary text inside an image.  An HTML
                # response starts with one of these markers (or is explicitly
                # labelled as HTML by the data URL), whereas JPEG/PNG/WebP
                # payloads do not.
                is_html = (marker.startswith(b'<!doctype html') or
                           marker.startswith(b'<html') or
                           marker.startswith(b'<head') or
                           marker.startswith(b'<body') or
                           marker.startswith(b'<?xml'))
                if is_html or content.lower().startswith('data:text/html'):
                    fallback = await _qq_wechat_qr_proxy()
                    if fallback:
                        return fallback
                    raise HTTPException(502, 'qq_wechat_appid_invalid: QQ 音乐上游微信登录 AppID 已失效，请使用 QQ 扫码或配置新的有效 AppID；可在 Docker 环境配置 QQ_WECHAT_QR_PROXY_URL 指向可用的同机 QQ 登录服务')
        return data

@app.get('/api/accounts/qq/qr/check')
async def qq_qr_check(key: str, login_type: str = 'qq', x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    if login_type not in {'qq', 'wx'}:
        raise HTTPException(400, 'login_type 仅支持 qq 或 wx')
    if cached := _qr_cached('qq', key):
        return cached
    async with _qr_lock('qq'):
        if cached := _qr_cached('qq', key):
            return cached
        async with httpx.AsyncClient(timeout=45) as client:
            response = await client.get(f'{POCKETTUNE_API}/qqmusic/login/qr/check', params={'key': key, 'type': login_type})
            data = await _upstream_json(response, f'qq_{login_type}_qr_check')
        if data.get('status') == 4:
            async with httpx.AsyncClient(timeout=15) as client:
                session = await client.get(f'{POCKETTUNE_API}/qqmusic/session')
                if session.is_success:
                    try:
                        session_data = session.json()
                    except (ValueError, TypeError):
                        session_data = {}
                    cookies = session_data.get('cookies') or {}
                    cookie = '; '.join(f'{k}={v}' for k,v in cookies.items() if v)
                    db.execute("insert or replace into accounts(provider,display_name,endpoint,cookie,status,error,updated_at) values(?,?,?,?,?,?,CURRENT_TIMESTAMP)", ('qq','QQ 音乐','builtin://qqmusic',cookie,'connected',None))
        _qr_store('qq', key, data)
        return data

def _normalize_tracks(data: object, provider: str) -> list[dict]:
    if isinstance(data, dict):
        rows = data.get('songs') or data.get('dailySongs') or data.get('playlist') or data.get('data') or []
        if isinstance(rows, dict):
            rows = rows.get('songs') or rows.get('dailySongs') or rows.get('data') or []
    else: rows = data
    result = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict): continue
        song_id = row.get('id') or row.get('songmid') or row.get('mid') or row.get('hash')
        title = row.get('name') or row.get('title') or ''
        artists = row.get('ar') or row.get('artists') or []
        artist = row.get('artist') or row.get('singer') or ' / '.join(x.get('name','') for x in artists if isinstance(x,dict))
        album = row.get('album') or row.get('al') or {}
        cover_url = (album.get('picUrl') or album.get('cover_url')) if isinstance(album, dict) else row.get('cover_url')
        if isinstance(album, dict): album = album.get('name','')
        if song_id and title: result.append({'platform':provider,'song_id':str(song_id),'title':str(title),'artist':str(artist or ''),'album':str(album or ''),'duration_ms':row.get('duration') or row.get('dt'),'cover_url':cover_url})
    return result


def _cache_daily(provider: str, items: list[dict]) -> None:
    with db.transaction() as conn:
        conn.execute('delete from daily_recommendations where provider=?', (provider,))
        conn.executemany(
            '''insert into daily_recommendations(provider,song_id,title,artist,album,duration_ms,cover_url,fetched_at)
               values(?,?,?,?,?,?,?,CURRENT_TIMESTAMP)''',
            ((provider, str(item['song_id']), item['title'], item.get('artist', ''),
              item.get('album', ''), item.get('duration_ms'), item.get('cover_url')) for item in items),
        )
        conn.commit()


def _cached_daily(provider: str) -> list[dict]:
    return db.fetchall(
        '''select provider as platform,song_id,title,artist,album,duration_ms,cover_url,fetched_at
           from daily_recommendations where provider=? order by rowid''',
        (provider,),
    )


def _daily_snapshot_meta(provider: str) -> dict:
    row = db.fetchone(
        'select max(fetched_at) fetched_at,count(*) item_count from daily_recommendations where provider=?',
        (provider,),
    ) or {}
    return {'fetched_at': row.get('fetched_at'), 'item_count': int(row.get('item_count') or 0)}


def _daily_cover(items: list[dict]) -> str | None:
    """Use the first real album cover as the stable daily playlist artwork."""
    for item in items:
        value = item.get('cover_url')
        if value:
            return str(value).replace('http:', 'https:')
    return None


@app.get('/api/cache/daily')
async def daily_cache_status(x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    return cache_summary(db)


@app.post('/api/cache/daily/play')
async def daily_cache_play(payload: dict, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    provider = str(payload.get('platform') or payload.get('provider') or '').strip()
    song_id = str(payload.get('song_id') or payload.get('id') or '').strip()
    if not provider or not song_id:
        raise HTTPException(400, '缺少歌曲平台或歌曲 ID')
    return record_cache_play(db, music_root, provider, song_id)

@app.get('/api/platform/{provider}/daily')
async def platform_daily(provider: str, refresh: int = 0, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token); provider = _provider_name(provider)
    account = db.fetchone('select * from accounts where provider=?', (provider,))
    if not account or account.get('status') != 'connected' or not account.get('cookie'):
        # Keep the last successful daily snapshot visible when a provider
        # session expires.  Returning a hard 401 made a valid historical daily
        # playlist look as if it had disappeared from the UI.
        if provider == 'netease':
            cached = _cached_daily(provider)
            if cached:
                return {'provider': provider, 'kind': 'daily', 'items': cached,
                        'source': 'cache', 'stale': True, 'login_required': True,
                        **_daily_snapshot_meta(provider),
                        'message': '网易云登录已失效，当前显示上次成功同步的每日推荐；重新扫码后可刷新'}
        raise HTTPException(401, '账号尚未连接或登录已失效')
    # 只有在「账号有效」之后才查/写这份缓存：上面那个分支返回的是登录失效兜底，
    # 不能被缓存复用，否则用户重新登录后仍会看到失效提示。
    if refresh:
        _DAILY_LIVE_CACHE.pop(provider, None)
    else:
        cached_live = _ttl_cache_get(_DAILY_LIVE_CACHE, provider, _DAILY_LIVE_CACHE_TTL)
        if cached_live:
            return cached_live
    try:
        data = await (NeteaseConnector((account or {}).get('cookie')).daily() if provider == 'netease' else QQMusicConnector((account or {}).get('cookie')).daily())
    except PlatformError as exc:
        cached = _cached_daily(provider)
        if cached:
            return {'provider': provider, 'kind': 'daily', 'items': cached, 'source': 'cache', 'stale': True,
                    **_daily_snapshot_meta(provider)}
        raise HTTPException(502, str(exc)) from exc
    items = _normalize_tracks(data, provider) if provider == 'netease' else data
    _cache_daily(provider, items)
    payload = {'provider': provider, 'kind': 'daily', 'items': items, 'source': 'live', 'stale': False,
               **_daily_snapshot_meta(provider)}
    _ttl_cache_put(_DAILY_LIVE_CACHE, provider, payload, _DAILY_LIVE_CACHE_TTL)
    return payload

@app.get('/api/platform/{provider}/search')
async def platform_search(provider: str, keyword: str, page: int = 1, page_size: int = 20, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token); provider = _provider_name(provider)
    account = db.fetchone('select * from accounts where provider=?', (provider,))
    try:
        items = await (QQMusicConnector((account or {}).get('cookie')).search(keyword, page, page_size) if provider == 'qq' else NeteaseConnector((account or {}).get('cookie')).search(keyword, page_size))
        return {'provider': provider, 'items': items}
    except PlatformError as exc: raise HTTPException(502, str(exc)) from exc

@app.get('/api/platform/netease/playlists/{uid}')
async def netease_playlists(uid: str, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token); account = db.fetchone('select * from accounts where provider=?', ('netease',))
    try: return await NeteaseConnector((account or {}).get('cookie')).playlists(uid)
    except PlatformError as exc: raise HTTPException(502, str(exc)) from exc

def _cached_netease_playlists() -> dict:
    rows = db.fetchall(
        '''select p.provider,p.playlist_id,p.title,p.source_url,p.kind,
                  p.last_sync_at,c.cover_url,c.description,c.owner,
                  (select count(*) from playlist_items i where i.playlist_id=p.id) item_count
           from playlists p left join playlist_covers c on c.playlist_row_id=p.id
           where p.provider=? order by p.last_sync_at desc''',
        ('netease',),
    )
    for row in rows:
        title = str(row.get('title') or '')
        owner = str(row.get('owner') or '')
        # Older databases do not have a playlist kind column.  Keep enough
        # metadata for the UI to distinguish the private recommendation card
        # from ordinary user/collected playlists when we are serving a stale
        # snapshot after a 301/401.
        if row.get('kind') == 'radar' or ('雷达' in title and row.get('kind') not in (None, 'playlist')):
            row['kind'] = 'radar'
        elif row.get('kind') == 'exclusive' or any(mark in f'{title} {owner}' for mark in ('私人雷达', '专属歌单', '私人推荐')):
            row['kind'] = 'exclusive'
    radar_rows = [row for row in rows if row.get('kind') == 'radar']
    exclusive_rows = [row for row in rows if row.get('kind') == 'exclusive']
    personal_rows = [row for row in rows if row.get('kind') not in ('exclusive', 'radar')]
    return {'provider': 'netease', 'items': personal_rows, 'exclusive_items': exclusive_rows,
            'radar_items': radar_rows,
            'sections': {'my': personal_rows, 'exclusive': exclusive_rows, 'radar': radar_rows}, 'source': 'cache',
            'stale': True, 'login_required': True,
            'message': '网易云登录已失效，当前显示上次成功同步的歌单；重新扫码后可刷新'}

def _playlist_cache_get(provider: str) -> dict | None:
    return _ttl_cache_get(_PLAYLIST_CACHE, provider, _PLAYLIST_CACHE_TTL)


def _playlist_cache_put(provider: str, payload: dict) -> None:
    _ttl_cache_put(_PLAYLIST_CACHE, provider, payload, _PLAYLIST_CACHE_TTL)


def _playlist_cache_clear(provider: str | None = None) -> None:
    if provider is None:
        _PLAYLIST_CACHE.clear()
    else:
        _PLAYLIST_CACHE.pop(provider, None)


@app.get('/api/platform/{provider}/playlists')
async def platform_playlists(provider: str, refresh: int = 0,
                             x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token); provider = _provider_name(provider)
    account = db.fetchone('select * from accounts where provider=?', (provider,))
    if not account or account.get('status') != 'connected' or not account.get('cookie'):
        if provider == 'netease':
            return _cached_netease_playlists()
        raise HTTPException(401, '账号尚未连接')
    # 命中进程内缓存直接返回：重复切换页面不再打上游。
    # refresh=1 用于用户主动点「刷新」时强制穿透缓存。
    if refresh:
        _playlist_cache_clear(provider)
    cached_hit = _playlist_cache_get(provider)
    if cached_hit is not None:
        return cached_hit
    try:
        if provider == 'qq':
            mine = await QQMusicConnector(account['cookie']).playlists()
            payload = {'provider':'qq', 'items':mine, 'exclusive_items': [],
                       'sections': {'my': mine, 'exclusive': []},
                       'source':'live','stale':False}
            _playlist_cache_put('qq', payload)
            return payload
        connector = NeteaseConnector(account['cookie'])

        # 三路并发：①登录态 → 我的歌单 ②专属歌单 ③雷达歌单。
        # 原实现全部串行，实测 3.15 秒，其中仅 6 张雷达逐个等待就占 2.37 秒。
        # ①内部两步有真实依赖（user/playlist 需要 login/status 的 uid）故串行，
        # 但①与②③互不依赖；②③各自内部也已并发。并发后总耗时 ≈ 0.7 秒。
        async def my_playlists() -> tuple[str | None, list[dict]]:
            status = await connector.call('login/status', {'timestamp': int(time.time() * 1000)})
            uid_value, _ = _netease_identity(status)
            if not uid_value:
                return None, []
            data = await connector.playlists(str(uid_value))
            rows = data.get('playlist') if isinstance(data, dict) else []
            collected: list[dict] = []
            for row in rows if isinstance(rows, list) else []:
                if not isinstance(row, dict) or not row.get('id'):
                    continue
                creator = row.get('creator') or {}
                collected.append({'provider':'netease','playlist_id':str(row['id']),
                                  'title':row.get('name') or '',
                                  'description':row.get('description') or '',
                                  'cover_url':row.get('coverImgUrl'),
                                  'owner':creator.get('nickname','') if isinstance(creator, dict) else '',
                                  'item_count':row.get('trackCount') or 0})
            return str(uid_value), collected

        async def exclusive_cards() -> list[dict]:
            # 首选 personalized（实测 50 张，与 PocketTune 首页「专属歌单」同源）；
            # 不可用时退回 recommend/resource（实测仅 7 张）以免整个分区空掉。
            # 捕获全部异常而非只捕 PlatformError：单路失败不应拖垮其它分区。
            try:
                cards = await connector.recommend_playlists()
            except Exception:
                cards = []
            if cards:
                return cards
            try:
                return await connector.exclusive_playlists()
            except Exception:
                return []

        async def radar_cards() -> list[dict]:
            try:
                return await connector.radar_playlists()
            except Exception:
                return []

        (uid, items), exclusive, radar = await asyncio.gather(
            my_playlists(), exclusive_cards(), radar_cards(),
        )
        if not uid:
            db.execute("update accounts set status='expired',error='netease_session_invalid',updated_at=CURRENT_TIMESTAMP where provider='netease'")
            # Keep the last successful snapshot visible while the user signs
            # in again.  Treating a short-lived upstream 401 as an empty
            # account made all personal playlists appear to disappear.
            return _cached_netease_playlists()
        # Some PocketTune/Netease builds include the private recommendation
        # cards in ``user/playlist`` as well as ``recommend/resource``.  Keep
        # those cards exclusively in the plaza's dedicated section so a
        # user's actual created/collected playlists stay in the dual-platform
        # area without a duplicate card.
        exclusive_markers = ('私人雷达', '专属歌单', '私人推荐')
        personal_items = []
        embedded_exclusive = []
        for item in items:
            title = str(item.get('title') or '')
            owner = str(item.get('owner') or '')
            if item.get('kind') == 'exclusive' or any(mark in f'{title} {owner}' for mark in exclusive_markers):
                item['kind'] = 'exclusive'
                embedded_exclusive.append(item)
            else:
                personal_items.append(item)
        # 网易云的「专属歌单/私人雷达」不在 user/playlist 返回值中，而是由
        # personalized（旧实现用 recommend/resource）单独提供。作为独立分区
        # 返回，不能混入「我的歌单」。上面三路已在 asyncio.gather 中并发拿到。
        exclusive_items = list(embedded_exclusive)
        seen = {str(item.get('playlist_id')) for item in personal_items + exclusive_items}
        for item in exclusive:
            playlist_id = str(item.get('playlist_id') or '')
            if playlist_id and playlist_id not in seen:
                exclusive_items.append(item)
                seen.add(playlist_id)
        radar_items: list[dict] = []
        for item in radar:
            playlist_id = str(item.get('playlist_id') or '')
            if playlist_id and playlist_id not in seen:
                radar_items.append(item)
                seen.add(playlist_id)
        # 账号把"私人雷达"收藏后 user/playlist 也会返回它，旧分类逻辑把标题含
        # "私人雷达"的卡片归入专属歌单；现在统一迁移到雷达分区，避免同一卡片
        # 因分区不同而"消失"。
        radar_ids = {str(s['playlist_id']) for s in getattr(connector, 'RADAR_PLAYLISTS', [])}
        if radar_ids:
            moved = [x for x in exclusive_items if str(x.get('playlist_id')) in radar_ids]
            if moved:
                exclusive_items = [x for x in exclusive_items if str(x.get('playlist_id')) not in radar_ids]
                in_radar = {str(x.get('playlist_id')) for x in radar_items}
                for item in moved:
                    if str(item.get('playlist_id')) not in in_radar:
                        radar_items.append({**item, 'kind': 'radar', 'daily_update': True})
                        seen.add(str(item.get('playlist_id')))
        # recommend/resource 在短时间内失败时仍保留已成功推送/打开过的
        # 专属歌单，避免用户看到“歌单消失”。只补不存在的缓存项。
        cached = _cached_netease_playlists().get('exclusive_items') or []
        for item in cached:
            if item.get('kind') != 'exclusive':
                continue
            playlist_id = str(item.get('playlist_id') or '')
            if playlist_id and playlist_id not in seen:
                exclusive_items.append(item)
                seen.add(playlist_id)
        cached_radar = _cached_netease_playlists().get('radar_items') or []
        for item in cached_radar:
            if item.get('kind') != 'radar':
                continue
            playlist_id = str(item.get('playlist_id') or '')
            if playlist_id and playlist_id not in seen:
                radar_items.append(item)
                seen.add(playlist_id)
        payload = {'provider':'netease','items':personal_items,'exclusive_items':exclusive_items,
                   'radar_items':radar_items,
                   'sections': {'my': personal_items, 'exclusive': exclusive_items, 'radar': radar_items},
                   'source':'live','stale':False}
        _playlist_cache_put('netease', payload)
        return payload
    except PlatformError as exc:
        # PocketTune can return a JSON 301/302 or an HTTP 401 for the same
        # expired-cookie condition.  Serve the local snapshot in both cases.
        detail = str(exc)
        if provider == 'netease' and any(mark in detail.lower() for mark in ('401', '301', '302', 'session_invalid', 'api_301', 'api_302')):
            db.execute("update accounts set status='expired',error='netease_session_invalid',updated_at=CURRENT_TIMESTAMP where provider='netease'")
            return _cached_netease_playlists()
        raise HTTPException(502, detail) from exc

@app.get('/api/plaza')
async def playlist_plaza(provider: str = 'netease', category: str = '', offset: int = 0,
                         limit: int = 30, sort: str = 'hot', refresh: int = 0,
                         x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token); provider = _provider_name(provider); account = db.fetchone('select * from accounts where provider=?', (provider,))
    if sort not in {'hot', 'latest'}:
        raise HTTPException(400, '排序只支持 hot 或 latest')
    page_size = min(max(1, limit), 100)
    page_offset = max(0, offset)
    cache_key = f'{provider}|{category}|{page_offset}|{page_size}|{sort}'
    if refresh:
        _PLAZA_CACHE.pop(cache_key, None)
    else:
        cached = _ttl_cache_get(_PLAZA_CACHE, cache_key, _PLAZA_CACHE_TTL)
        if cached:
            return cached
    try:
        connector = NeteaseConnector((account or {}).get('cookie')) if provider == 'netease' else QQMusicConnector((account or {}).get('cookie'))
        items = await connector.plaza(category, page_offset, page_size, sort)
        # 歌单广场只返回公开发现内容，与登录账号私有歌单分区。
        for item in items:
            if isinstance(item, dict):
                item.setdefault('scope', 'public')
        payload = {'provider': provider, 'scope': 'public', 'items': items,
                   'offset': page_offset, 'limit': page_size,
                   'has_more': len(items) == page_size}
        _ttl_cache_put(_PLAZA_CACHE, cache_key, payload, _PLAZA_CACHE_TTL)
        return payload
    except PlatformError as exc: raise HTTPException(502, str(exc)) from exc


@app.get('/api/playlists/search')
async def search_playlists(keyword: str, provider: str = 'all', page: int = 1,
                           page_size: int = 24, sort: str = 'relevance',
                           x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    keyword = keyword.strip()
    if not keyword:
        raise HTTPException(400, '请输入歌单名称或创建者')
    if len(keyword) > 100:
        raise HTTPException(400, '搜索关键词过长')
    if provider not in {'all', *SUPPORTED_PROVIDERS}:
        raise HTTPException(400, 'provider 仅支持 all、netease 或 qq')
    if sort not in {'relevance', 'hot', 'latest'}:
        raise HTTPException(400, 'sort 仅支持 relevance、hot 或 latest')
    page = max(1, page)
    page_size = min(50, max(1, page_size))
    providers = ['netease', 'qq'] if provider == 'all' else [provider]

    async def search_one(name: str) -> dict:
        account = db.fetchone('select * from accounts where provider=?', (name,))
        connector = (NeteaseConnector((account or {}).get('cookie')) if name == 'netease'
                     else QQMusicConnector((account or {}).get('cookie')))
        result = await connector.search_playlists(keyword, page, page_size)
        return {'provider': name, **result}

    responses = await asyncio.gather(*(search_one(name) for name in providers), return_exceptions=True)
    successful: list[dict] = []
    errors: list[dict] = []
    for name, result in zip(providers, responses):
        if isinstance(result, Exception):
            errors.append({'provider': name, 'message': str(result)})
        else:
            successful.append(result)
    if not successful:
        message = '; '.join(f"{x['provider']}: {x['message']}" for x in errors)
        raise HTTPException(502, message or '两个平台均未返回搜索结果')

    if provider == 'all' and sort == 'relevance':
        item_lists = [list(result.get('items') or []) for result in successful]
        items = [row for index in range(max((len(rows) for rows in item_lists), default=0))
                 for rows in item_lists if index < len(rows) for row in [rows[index]]]
    else:
        items = [row for result in successful for row in result.get('items') or []]
        if sort == 'hot':
            items.sort(key=lambda row: int(row.get('play_count') or 0), reverse=True)
        elif sort == 'latest':
            items.sort(key=lambda row: str(row.get('update_time') or ''), reverse=True)

    seen: set[tuple[str, str]] = set()
    items = [row for row in items
             if not ((key := (str(row.get('provider')), str(row.get('playlist_id')))) in seen
                     or seen.add(key))]
    return {
        'keyword': keyword, 'provider': provider, 'page': page,
        'page_size': page_size, 'sort': sort,
        'scope': 'public',
        'total': sum(int(result.get('total') or 0) for result in successful),
        'has_more': any(bool(result.get('has_more')) for result in successful),
        'items': items, 'sources': [result['provider'] for result in successful],
        'errors': errors,
    }


@app.get('/api/plaza/categories')
async def playlist_plaza_categories(provider: str = 'netease', x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token); provider = _provider_name(provider); account = db.fetchone('select * from accounts where provider=?', (provider,))
    try:
        connector = NeteaseConnector((account or {}).get('cookie')) if provider == 'netease' else QQMusicConnector((account or {}).get('cookie'))
        return {'provider': provider, 'groups': await connector.categories()}
    except PlatformError as exc: raise HTTPException(502, str(exc)) from exc


@app.get('/api/charts')
async def charts(provider: str = 'netease', refresh: int = 0, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token); provider = _provider_name(provider); account = db.fetchone('select * from accounts where provider=?', (provider,))
    # 排行榜变化很慢（周更），但网易云 toplist 单次拉取就要 1.3 秒，而前端
    # 每次加载首页都会取一次。缓存 10 分钟后首屏不再等这个接口。
    if refresh:
        _CHARTS_CACHE.pop(provider, None)
    cached_hit = _ttl_cache_get(_CHARTS_CACHE, provider, _CHARTS_CACHE_TTL)
    if cached_hit is not None:
        return cached_hit
    try:
        connector = NeteaseConnector((account or {}).get('cookie')) if provider == 'netease' else QQMusicConnector((account or {}).get('cookie'))
        items = await connector.charts()
        for item in items:
            if isinstance(item, dict):
                item.setdefault('scope', 'platform')
        payload = {'provider': provider, 'scope': 'platform', 'items': items,
                   'source': 'live', 'stale': False}
        _ttl_cache_put(_CHARTS_CACHE, provider, payload, _CHARTS_CACHE_TTL)
        return payload
    except PlatformError as exc:
        raise HTTPException(502, str(exc)) from exc


@app.get('/api/charts/{provider}/{chart_id}')
async def chart_detail(provider: str, chart_id: str, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token); provider = _provider_name(provider); account = db.fetchone('select * from accounts where provider=?', (provider,))
    try:
        connector = NeteaseConnector((account or {}).get('cookie')) if provider == 'netease' else QQMusicConnector((account or {}).get('cookie'))
        raw = await connector.chart(chart_id)
        result = _playlist_payload(raw, provider, f'chart:{chart_id}') if provider == 'netease' else raw
        result['playlist_id'] = f'chart:{chart_id}'
        return await _save_playlist(result, {'source_url': f'chart:{provider}:{chart_id}'})
    except PlatformError as exc:
        raise HTTPException(502, str(exc)) from exc


# ---------------------------------------------------------------------------
# 「为我推荐」：口味画像筛选的 50 池 / 每日 15 换血公开歌单推荐。
# 实现见 app/for_you.py；画像文件是私密运行时配置，任何响应不返回其内容。
# ---------------------------------------------------------------------------

_FOR_YOU_TASKS: set[asyncio.Task] = set()


def _for_you_start_rotation(trigger: str) -> dict:
    """防并发重入：进行中的轮换直接复用其运行状态，不再叠加新任务。"""
    running = db.fetchone("select id,started_at from for_you_runs where status='running' order by id desc limit 1")
    if running:
        started = _stored_time(running.get('started_at'))
        if started and (datetime.now(timezone.utc) - started).total_seconds() < 3600:
            return {'started': False, 'reason': 'already_running', 'run_id': int(running['id'])}
    task = asyncio.create_task(run_rotation(db, trigger=trigger))
    _FOR_YOU_TASKS.add(task)
    task.add_done_callback(_FOR_YOU_TASKS.discard)
    return {'started': True, 'trigger': trigger}


@app.get('/api/for-you')
async def for_you_list(x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    return {'items': pool_items(db), 'status': pool_status(db), 'scope': 'public'}


@app.post('/api/for-you/refresh')
async def for_you_refresh(x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    """手动触发一轮换血（与每日调度共享当日 15 个换入预算；不清池、不绕过冷却）。"""
    guard(x_fn_token)
    result = _for_you_start_rotation('manual')
    status = pool_status(db)
    status['refresh'] = result
    return status


@app.get('/api/for-you/blacklist')
async def for_you_blacklist(x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    return blacklist_rows(db)


@app.post('/api/for-you/blacklist/{provider}/{playlist_id}/restore')
async def for_you_blacklist_restore(provider: str, playlist_id: str,
                                    x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    if provider not in SUPPORTED_PROVIDERS:
        raise HTTPException(400, '仅支持 netease 或 qq')
    try:
        return restore_item(db, provider, playlist_id)
    except LookupError as exc:
        raise HTTPException(404, '该歌单不在黑名单中') from exc


@app.get('/api/for-you/{item_id}')
async def for_you_detail(item_id: int, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    row = pool_item(db, item_id)
    if not row:
        raise HTTPException(404, '推荐条目不存在或已被移出推荐池')
    row['status'] = pool_status(db)
    return row


@app.post('/api/for-you/{item_id}/feedback')
async def for_you_feedback(item_id: int, payload: dict,
                           x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    """action=dislike：移出推荐池并加入黑名单（可在 /api/for-you/blacklist 恢复）。"""
    guard(x_fn_token)
    action = str((payload or {}).get('action') or '')
    if action != 'dislike':
        raise HTTPException(400, 'action 仅支持 dislike（恢复走 blacklist/restore）')
    try:
        result = dislike_item(db, item_id)
    except LookupError as exc:
        raise HTTPException(404, '推荐条目不存在或已被移出推荐池') from exc
    result['status'] = pool_status(db)
    return result


@app.post('/api/accounts/{provider}/disconnect')
async def disconnect_account(provider: str, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token); provider = _provider_name(provider)
    _playlist_cache_clear(provider)
    db.execute("update accounts set cookie=null,status='disconnected',error=null,updated_at=CURRENT_TIMESTAMP where provider=?", (provider,))
    return {'provider': provider, 'status': 'disconnected'}

@app.post('/api/accounts/{provider}/check')
async def check_account(provider: str, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token); provider = _provider_name(provider)
    row = db.fetchone('select * from accounts where provider=?', (provider,))
    if not row or not row.get('endpoint') or not row.get('cookie'):
        raise HTTPException(404, '账号尚未连接')
    status, error = 'connected', None
    try:
        if provider == 'netease':
            data = await NeteaseConnector(row['cookie']).call('login/status', {'timestamp': int(datetime.now().timestamp() * 1000)})
            uid, profile = _netease_identity(data)
            if not uid:
                status, error = 'expired', 'netease_session_invalid'
            db.execute('update accounts set status=?,error=?,last_checked_at=?,updated_at=CURRENT_TIMESTAMP where provider=?', (status,error,_now(),provider))
            return {'provider': provider, 'status': status, 'error': error, 'profile': profile if uid else None}
        connector = QQMusicConnector(row['cookie'])
        cookies = connector._cookies()
        uin = connector.account_id()
        if not uin or not (cookies.get('qm_keyst') or cookies.get('qqmusic_key')):
            status, error = 'expired', 'qq_session_invalid'
        db.execute('update accounts set status=?,error=?,last_checked_at=?,updated_at=CURRENT_TIMESTAMP where provider=?', (status,error,_now(),provider))
        return {'provider': provider, 'status': status, 'error': error, 'profile': {'uin': uin} if status == 'connected' else None}
    except Exception as exc:
        status, error = 'error', str(exc)[:200]
    db.execute('update accounts set status=?,error=?,last_checked_at=?,updated_at=CURRENT_TIMESTAMP where provider=?', (status,error,_now(),provider))
    return {'provider': provider, 'status': status, 'error': error}

def _playlist_payload(data: object, provider: str, playlist_id: str) -> dict:
    if not isinstance(data, dict): data = {'songs': data}
    meta = data.get('playlist') if isinstance(data.get('playlist'), dict) else data
    rows = data.get('songs') or data.get('tracks') or (meta.get('tracks') if isinstance(meta, dict) else None) or []
    items = []
    for index, row in enumerate(rows if isinstance(rows, list) else []):
        if not isinstance(row, dict): continue
        song_id = row.get('id') or row.get('songmid') or row.get('mid') or row.get('hash')
        title = row.get('name') or row.get('title') or ''
        artists = row.get('ar') or row.get('artists') or []
        artist = row.get('artist') or row.get('singer') or row.get('author') or ' / '.join(str(x.get('name','')) for x in artists if isinstance(x, dict))
        album_value = row.get('album') or row.get('al') or ''
        cover_url = ((album_value.get('picUrl') or album_value.get('cover_url')) if isinstance(album_value, dict)
                     else row.get('cover_url') or row.get('picUrl') or row.get('picurl'))
        if isinstance(album_value, dict): album_value = album_value.get('name','')
        duration = row.get('duration') or row.get('interval') or row.get('dt')
        if duration and int(duration) < 10000: duration = int(duration) * 1000
        if song_id and title:
            items.append({'position': index, 'platform': provider, 'song_id': str(song_id), 'title': str(title),
                          'artist': str(artist or ''), 'album': str(album_value or ''), 'duration_ms': duration,
                          'cover_url': cover_url})
    creator = meta.get('creator') or meta.get('owner') or ''
    if isinstance(creator, dict):
        creator = creator.get('nickname') or creator.get('name') or creator.get('userName') or ''
    cover_url = (meta.get('cover') or meta.get('cover_url') or meta.get('coverImgUrl')
                 or meta.get('picUrl') or meta.get('picurl'))
    return {'provider': provider, 'playlist_id': str(playlist_id), 'title': str(meta.get('name') or meta.get('title') or playlist_id),
            'description': str(meta.get('description') or ''), 'cover_url': cover_url,
            'owner': str(creator), 'items': items}


def _annotate_library(items: list[dict]) -> list[dict]:
    return annotate_items(db, items)


def _enqueue_playlist_items_sync(items: list[dict], priority: int = 4, task_type: str = 'playlist') -> dict:
    """整单入队（同步版）：单事务批量完成全部读写，曲库匹配走内存索引。
    此前每首歌要开 3-4 次独立 SQLite 连接并逐条 stat NAS 文件，大歌单会卡住整个应用。"""
    created = active_reused = library_reused = 0
    cache_promote_ids: set[int] = set()
    with db.transaction() as conn:
        for item in items:
            conn.execute(
                'insert or ignore into tracks(platform,song_id,title,artist,album,duration_ms,quality,cover_url) values(?,?,?,?,?,?,?,?)',
                (item['platform'], item['song_id'], item['title'], item.get('artist', ''),
                 item.get('album', ''), item.get('duration_ms'), DEFAULT_QUALITY, item.get('cover_url')),
            )
            if item.get('cover_url'):
                conn.execute('update tracks set cover_url=coalesce(?,cover_url) where platform=? and song_id=?',
                             (item.get('cover_url'), item['platform'], item['song_id']))
            track = conn.execute(
                'select * from tracks where platform=? and song_id=?',
                (item['platform'], item['song_id']),
            ).fetchone()
            if not track:
                continue
            match = item.get('_library_file')
            if match and match.get('artwork_status') == 'embedded':
                conn.execute(
                    "update tracks set file_path=?,file_hash=?,status='archived',updated_at=CURRENT_TIMESTAMP where id=?",
                    (match['path'], match.get('file_hash'), track['id']),
                )
                if task_type != 'daily' and is_cache_path(Path(match['path']), music_root):
                    cache_promote_ids.add(int(track['id']))
                library_reused += 1
                continue
            if track['status'] == 'archived' and track['file_path'] and track['cover_status'] == 'embedded':
                if task_type != 'daily' and is_cache_path(Path(track['file_path']), music_root):
                    cache_promote_ids.add(int(track['id']))
                library_reused += 1
                continue
            existing = conn.execute(
                "select id from tasks where track_id=? and status in ('pending','matching','downloading') order by id desc limit 1",
                (track['id'],),
            ).fetchone()
            if existing:
                active_reused += 1
                continue
            conn.execute(
                'insert into tasks(track_id,task_type,priority) values(?,?,?)',
                (track['id'], task_type, priority),
            )
            created += 1
    cache_promoted = 0
    for track_id in cache_promote_ids:
        track = db.fetchone('select * from tracks where id=?', (track_id,))
        if track and promote_cached_track(db, track, music_root, f'enqueued:{task_type}'):
            cache_promoted += 1
    return {'total': len(items), 'tasks_created': created, 'active_reused': active_reused,
            'library_reused': library_reused, 'cache_promoted': cache_promoted}


async def _enqueue_playlist_items(items: list[dict], priority: int = 4, task_type: str = 'playlist') -> dict:
    annotate_items(db, items, attach_match=True)
    try:
        return await asyncio.to_thread(_enqueue_playlist_items_sync, items, priority, task_type)
    finally:
        for item in items:
            item.pop('_library_file', None)

@app.post('/api/playlists/fetch')
async def fetch_playlist(payload: dict, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token); provider = _provider_name(str(payload.get('provider') or ''))
    playlist_id = str(payload.get('playlist_id') or '').strip()
    if not playlist_id: raise HTTPException(400, 'playlist_id required')
    source_kind = str(payload.get('source_kind') or '').strip()
    account = db.fetchone('select * from accounts where provider=?', (provider,))
    endpoint = str(payload.get('endpoint') or (account or {}).get('endpoint') or '').strip()
    if playlist_id.startswith('chart:'):
        chart_id = playlist_id.partition(':')[2]
        try:
            connector = NeteaseConnector((account or {}).get('cookie')) if provider == 'netease' else QQMusicConnector((account or {}).get('cookie'))
            raw = await connector.chart(chart_id)
            result = _playlist_payload(raw, provider, playlist_id) if provider == 'netease' else raw
            result['playlist_id'] = playlist_id
            if source_kind in ('exclusive', 'netease_exclusive'): result['kind'] = 'exclusive'
            elif source_kind in ('radar', 'netease_radar'): result['kind'] = 'radar'
            return await _save_playlist(result, payload)
        except PlatformError as exc:
            raise HTTPException(502, str(exc)) from exc
    if provider == 'netease' and not payload.get('endpoint') and (not endpoint or endpoint.rstrip('/').endswith('/netease')):
        try:
            result = _playlist_payload(await NeteaseConnector((account or {}).get('cookie')).playlist(playlist_id), provider, playlist_id)
            if source_kind in ('exclusive', 'netease_exclusive'): result['kind'] = 'exclusive'
            elif source_kind in ('radar', 'netease_radar'): result['kind'] = 'radar'
            elif source_kind in ('radar', 'netease_radar'): result['kind'] = 'radar'
            return await _save_playlist(result, payload)
        except PlatformError as exc:
            raise HTTPException(502, str(exc)) from exc
    if provider == 'qq' and not payload.get('endpoint'):
        try:
            result = await QQMusicConnector((account or {}).get('cookie')).playlist(playlist_id)
            if source_kind in ('exclusive', 'netease_exclusive'): result['kind'] = 'exclusive'
            elif source_kind in ('radar', 'netease_radar'): result['kind'] = 'radar'
            elif source_kind in ('radar', 'netease_radar'): result['kind'] = 'radar'
            return await _save_playlist(result, payload)
        except PlatformError as exc:
            raise HTTPException(502, str(exc)) from exc
    if provider == 'netease' and (not endpoint or endpoint.rstrip('/').endswith('/netease')):
        endpoint = f'{POCKETTUNE_API}/netease/playlist/detail'
    if not endpoint: raise HTTPException(404, '请先连接账号或配置歌单接口')
    headers = {'User-Agent': 'fnmusic-flow/1.0'}
    if account and account.get('cookie'): headers['Cookie'] = account['cookie']
    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            params = {'kind': 'playlist', 'id': playlist_id, 'playlist_id': playlist_id}
            if provider == 'netease' and endpoint.endswith('/playlist/detail'): params = {'id': playlist_id, 's': 0, 'noCookie': 'false'}
            response = await client.get(endpoint, params=params, headers=headers)
            response.raise_for_status(); result = _playlist_payload(response.json(), provider, playlist_id)
            if source_kind in ('exclusive', 'netease_exclusive'): result['kind'] = 'exclusive'
            elif source_kind in ('radar', 'netease_radar'): result['kind'] = 'radar'
    except httpx.HTTPError as exc:
        raise HTTPException(502, f'歌单接口请求失败: {exc}') from exc
    return await _save_playlist(result, payload)

def _store_playlist(result: dict) -> int:
    """歌单快照落库（同步版）：单事务完成 upsert + 全量重建条目，替代此前逐条
    insert or replace（每条一次连接+提交，几百首歌的歌单会明显卡顿）。"""
    provider = result['provider']; playlist_id = str(result['playlist_id'])
    with db.transaction() as conn:
        conn.execute(
            '''insert into playlists(provider,playlist_id,title,source_url,kind,last_sync_at)
               values(?,?,?,?,?,CURRENT_TIMESTAMP)
               on conflict(provider,playlist_id) do update set title=excluded.title,
                 source_url=coalesce(excluded.source_url,playlists.source_url),
                  kind=case when excluded.kind in ('exclusive','daily','radar') then excluded.kind else playlists.kind end,
                 last_sync_at=CURRENT_TIMESTAMP''',
            (provider, playlist_id, result['title'], result.get('source_url'), result.get('kind') or 'playlist'),
        )
        saved = conn.execute('select id from playlists where provider=? and playlist_id=?', (provider, playlist_id)).fetchone()
        conn.execute('delete from playlist_items where playlist_id=?', (saved['id'],))
        conn.executemany(
            'insert into playlist_items(playlist_id,position,platform,song_id,title,artist,album,duration_ms,cover_url) values(?,?,?,?,?,?,?,?,?)',
            ((saved['id'], item['position'], item['platform'], item['song_id'], item['title'], item['artist'],
              item['album'], item['duration_ms'], item.get('cover_url')) for item in result['items']),
        )
        conn.execute('insert or replace into playlist_covers(playlist_row_id,cover_url,description,owner,updated_at) values(?,?,?,?,CURRENT_TIMESTAMP)',
                     (saved['id'], result.get('cover_url'), result.get('description'), result.get('owner')))
        return saved['id']


async def _save_playlist(result: dict, payload: dict) -> dict:
    if payload.get('source_url'):
        result.setdefault('source_url', payload['source_url'])
    saved_id = await asyncio.to_thread(_store_playlist, result)
    result['items'] = await asyncio.to_thread(annotate_items, db, result['items'])
    return {**result, 'id': saved_id}

@app.get('/api/playlists')
async def list_playlists(provider: str | None = None, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    if provider: provider = _provider_name(provider)
    sql = 'select p.*,c.cover_url,c.description,c.owner,(select count(*) from playlist_items i where i.playlist_id=p.id) item_count from playlists p left join playlist_covers c on c.playlist_row_id=p.id'
    return db.fetchall(sql + (' where p.provider=?' if provider else '') + ' order by p.last_sync_at desc', (provider,) if provider else ())

@app.get('/api/playlists/{playlist_row_id}')
async def playlist_detail(playlist_row_id: int, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    playlist = db.fetchone('select p.*,c.cover_url,c.description,c.owner from playlists p left join playlist_covers c on c.playlist_row_id=p.id where p.id=?', (playlist_row_id,))
    if not playlist: raise HTTPException(404, '歌单不存在')
    rows = db.fetchall('select position,platform,song_id,title,artist,album,duration_ms,cover_url from playlist_items where playlist_id=? order by position', (playlist_row_id,))
    playlist['items'] = await asyncio.to_thread(annotate_items, db, rows)
    return playlist


@app.post('/api/playlists/{playlist_row_id}/enqueue')
async def enqueue_playlist(playlist_row_id: int, payload: dict | None = None, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    playlist = db.fetchone('select id from playlists where id=?', (playlist_row_id,))
    if not playlist:
        raise HTTPException(404, '歌单不存在')
    items = db.fetchall(
        'select platform,song_id,title,artist,album,duration_ms,cover_url from playlist_items where playlist_id=? order by position',
        (playlist_row_id,),
    )
    priority = int((payload or {}).get('priority', 4))
    return {'playlist_id': playlist_row_id, **await _enqueue_playlist_items(items, priority)}

@app.post('/api/playlists/import-link')
async def import_playlist_link(payload: dict, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    url = str(payload.get('url') or '').strip(); info = identify(url)
    if not info.get('supported'): raise HTTPException(400, '暂不支持该歌单链接，或链接缺少歌单 ID')
    # Fetch metadata/items through the configured provider connector, then optionally enqueue all songs.
    result = await fetch_playlist({'provider': info['platform'], 'playlist_id': info['playlist_id'], 'source_url': url}, x_fn_token)
    if payload.get('enqueue', True):
        result.update(await _enqueue_playlist_items(result['items'], int(payload.get('priority', 4))))
    return result

@app.post('/api/providers/{provider}/config')
async def provider_config(provider: str, payload: dict, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    if not payload.get('endpoint'): raise HTTPException(400, 'endpoint required')
    db.execute('insert or replace into provider_configs(provider,endpoint,cookie,enabled) values(?,?,?,1)', (provider,payload['endpoint'],payload.get('cookie')))
    return {'provider':provider,'configured':True}

@app.post('/api/recommendations/{provider}/fetch')
async def fetch_recommendations(provider: str, payload: dict|None=None, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token); cfg=db.fetchone('select * from provider_configs where provider=? and enabled=1',(provider,))
    if not cfg: raise HTTPException(404,'provider not configured')
    rows=await RecommendationProvider(provider,cfg['endpoint'],cfg['cookie']).fetch((payload or {}).get('kind','daily')); added=0
    for row in rows:
        before=db.fetchone('select id from tracks where platform=? and song_id=?',(row['platform'],row['song_id']))
        db.execute('insert or ignore into tracks(platform,song_id,title,artist,album,duration_ms,quality,cover_url) values(?,?,?,?,?,?,?,?)',(row['platform'],row['song_id'],row['title'],row['artist'],row['album'],row['duration_ms'],DEFAULT_QUALITY,row.get('cover_url')))
        track=db.fetchone('select id from tracks where platform=? and song_id=?',(row['platform'],row['song_id']))
        if not before: db.execute("insert into tasks(track_id,task_type,priority) values(?, 'daily', 5)",(track['id'],)); added+=1
    return {'provider':provider,'count':len(rows),'added':added}

@app.get('/api/fnos/probe')
async def fnos_probe(x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token); return await FnosAdapter(os.getenv('FNOS_API_URL'),os.getenv('FNOS_TOKEN')).probe()


@app.get('/api/fnos/candidates')
async def fnos_candidates(x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    """列出本机飞牛账号候选（社区版账号选择器）：名称/角色/可写/凭据存活。

    多账号家庭 NAS 由用户明确选择绑定谁；单账号场景前端直接自动绑定。
    """
    guard(x_fn_token)
    adapter = FnosAdapter(os.getenv('FNOS_API_URL'), os.getenv('FNOS_TOKEN'))
    try:
        items = await adapter.account_candidates()
    except Exception as exc:
        raise HTTPException(502, f'读取飞牛账号候选失败: {str(exc)[:200]}') from exc
    return {'items': items, 'locked_account': load_account_lock(), 'allowed_accounts': _allowed_accounts()}


@app.post('/api/fnos/rebind')
async def fnos_rebind(payload: dict | None = None, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    """一键重绑：从飞牛库按白名单捞最新**实测可用**的 token 并写回。

    2026-09-30（1.0.10）：界面「直接绑定」正道入口。
    2026-10-01（1.3.0）：body 可传 {"account": "名称"} 指定绑定账号并落账号锁
    （/config/fnos-account-lock），后续 token 自愈只恢复该账号，多账号不串号。

    背景：磁盘 token 的行会被飞牛侧**物理删除**（用户重新登录/换设备），
    而界面过去把用户引向「借登录」（该路径在 bridge 网络下结构性不可用，
    必报 `missing Connection header`）。本端点让用户无需理解 token 细节，
    点一下即完成重绑；复用 FnosAdapter.self_heal_token() 的完整校验逻辑。
    """
    guard(x_fn_token)
    payload = payload or {}
    account = str(payload.get('account') or '').strip() or None
    adapter = FnosAdapter(os.getenv('FNOS_API_URL'), os.getenv('FNOS_TOKEN'))
    result = await adapter.self_heal_token(preferred_account=account)
    status = result.get('status')
    if status in ('healthy', 'healed'):
        locked = save_account_lock(account) if account else load_account_lock()
        probe = await FnosAdapter(os.getenv('FNOS_API_URL'), os.getenv('FNOS_TOKEN')).probe()
        return {'status': 'ready', 'action': status, 'account': probe.get('account'),
                'can_write_playlist': probe.get('can_write_playlist'), 'detail': result.get('detail'),
                'locked_account': locked}
    raise HTTPException(422, result.get('detail') or f'重绑失败（{status}）')


@app.get('/api/fnos/library-search')
async def fnos_library_search(keyword: str, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    value = str(keyword or '').strip()
    if not value:
        raise HTTPException(400, '请输入歌曲名')
    try:
        return {'items': await FnosAdapter(os.getenv('FNOS_API_URL'), os.getenv('FNOS_TOKEN')).search_tracks(value)}
    except Exception as exc:
        raise HTTPException(502, f'飞牛曲库搜索失败: {str(exc)[:200]}') from exc

@app.get('/api/fnos/session')
async def fnos_session(x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    status = fnos_session_status()
    probe = await FnosAdapter(os.getenv('FNOS_API_URL'), os.getenv('FNOS_TOKEN')).probe()
    status.update({'probe': probe.get('status'), 'account': probe.get('account'), 'detail': probe.get('detail'),
                   'automatic_token': bool(probe.get('automatic_token'))})
    return status

@app.post('/api/fnos/login')
async def fnos_login(payload: dict, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    try:
        return await login_fnos(str(payload.get('username') or ''), str(payload.get('password') or ''))
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:
        raise HTTPException(502, f'飞牛登录失败: {str(exc)[:200]}') from exc

@app.post('/api/fnos/login/2fa')
async def fnos_login_twofa(payload: dict, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    try:
        return await complete_fnos_twofa(str(payload.get('challenge_id') or ''), str(payload.get('code') or ''), bool(payload.get('trust_device', True)))
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:
        raise HTTPException(502, f'飞牛两步验证失败: {str(exc)[:200]}') from exc

@app.post('/api/fnos/session/refresh')
async def fnos_session_refresh(x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    try:
        token = await refresh_fnos_token()
        return {'status': 'ready', 'renewed': True, 'token_preview': token[:6] + '...'}
    except Exception as exc:
        raise HTTPException(422, str(exc)) from exc

@app.post('/api/fnos/config')
async def configure_fnos(payload: dict, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    token = str(payload.get('token') or '').strip()
    if not token:
        raise HTTPException(400, '飞牛音乐 token 不能为空')
    status = await FnosAdapter(os.getenv('FNOS_API_URL'), token).probe()
    if status.get('status') != 'ready':
        raise HTTPException(422, f"飞牛音乐 token 验证失败: {status.get('detail') or status.get('status')}")
    save_fnos_token(token)
    return {'status': 'ready', 'account': status.get('account')}


@app.post('/api/fnos/config/session')
async def configure_fnos_from_session(request: Request, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    candidates = [request.cookies.get(name, '').strip() for name in ('fnos-token', 'ost')]
    for token in dict.fromkeys(value for value in candidates if value):
        status = await FnosAdapter(os.getenv('FNOS_API_URL'), token).probe()
        if status.get('status') == 'ready':
            save_fnos_token(token)
            return {'status': 'ready', 'account': status.get('account'), 'source': 'fnos_session'}
    raise HTTPException(422, '当前页面没有可用的飞牛登录会话，请从飞牛桌面打开或手工粘贴 token')

@app.post('/api/fnos/playlists/{playlist_row_id}/push')
async def push_playlist_to_fnos(playlist_row_id: int, payload: dict | None = None, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    playlist = await playlist_detail(playlist_row_id, x_fn_token)
    _attach_track_paths(playlist.get('items', []))
    adapter = FnosAdapter(os.getenv('FNOS_API_URL'), os.getenv('FNOS_TOKEN'))
    run_started_at = _run_now()
    options = payload or {}
    # 2026-09-20 修复：kind 先归一（固定雷达 ID / 标题含「雷达」一律 radar，
    # exclusive 归 playlist），再按同源身份找目标 —— 找不到精确键时收养
    # 同源任意 kind 的既有目标，绑定的飞牛歌单 guid 一并继承，
    # 杜绝「换个入口推一次就多一张重复歌单」。
    kind = _canonical_kind(playlist.get('kind'), playlist['playlist_id'],
                           playlist.get('title') or (options or {}).get('title') or '')
    existing_target = _find_target_for_source(playlist['provider'], str(playlist['playlist_id']), kind)
    _items = playlist.get('items') or []
    pre_total = len({(str(i.get('platform')), str(i.get('song_id'))) for i in _items})

    # 2026-10-01（1.3.2）推送后台化：此前 HTTP 请求同步等完整推送（大歌单数分钟），
    # 期间 run_* 字段不更新、前端零进度。现在：先落一条 running 目标行（立即有
    # 「0/N 匹配中」进度），推送放后台任务执行并经 progress_cb 回写 run_matched，
    # 完成后走原 _upsert_push_target 终态化；接口立即返回。
    retention = str(options.get('retention_mode') or 'permanent')
    expires_at = _retention_expiry(retention, options.get('expires_at'))
    if existing_target:
        target_row_id = existing_target['id']
        db.execute(
            '''update push_targets set playlist_row_id=?,status='active',last_error=null,
               run_started_at=?,run_completed_at=null,run_status='running',run_stage='matching',
               run_total=?,run_matched=0,run_missing=?,run_duration_seconds=null,
               run_scan_requested_at=null,run_last_checked_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP
               where id=?''',
            (playlist.get('id'), run_started_at, pre_total, pre_total, target_row_id),
        )
    else:
        target_row_id = db.execute(
            '''insert into push_targets(provider,playlist_id,playlist_row_id,kind,target_title,fnos_guid,status,
                      retention_mode,expires_at,scheduled_push_at,last_pushed_at,last_error,updated_at)
               values(?,?,?,?,?,null,'active',?,?,null,null,null,CURRENT_TIMESTAMP)''',
            (playlist['provider'], str(playlist['playlist_id']), playlist.get('id'), kind,
             str(options.get('title') or playlist['title']), retention, expires_at),
        )
        db.execute(
            '''update push_targets set run_started_at=?,run_status='running',run_stage='matching',
               run_total=?,run_matched=0,run_missing=? where id=?''',
            (run_started_at, pre_total, pre_total, target_row_id),
        )

    subscription = None
    if options.get('auto_sync', False):
        if 'interval_minutes' in options:
            interval = _subscription_interval(kind, options.get('interval_minutes'))
        else:
            interval = _UNSET
        if 'schedule_time' in options:
            anchor = str(options.get('schedule_time') or '').strip() or None
            if anchor and _parse_hhmm(anchor) is None:
                raise HTTPException(400, '每日更新时刻必须是 HH:MM')
        else:
            anchor = _UNSET
        subscription = _upsert_subscription(
            playlist['provider'], str(playlist['playlist_id']), kind, interval,
            str(options.get('title') or playlist['title']), anchor,
        )

    last_progress = {'t': 0.0}

    def progress_cb(done: int, total: int, stage: str) -> None:
        now = time.monotonic()
        if now - last_progress['t'] < 1.0 and done < total:
            return
        last_progress['t'] = now
        try:
            db.execute(
                '''update push_targets set run_matched=?,run_total=?,run_stage=?,
                   run_last_checked_at=CURRENT_TIMESTAMP where id=?''',
                (min(int(done), max(1, int(total))), max(1, int(total)), str(stage), target_row_id),
            )
        except Exception:
            pass

    async def _run_push_job():
        try:
            result = await adapter.create_or_update_playlist(
                options.get('title') or playlist['title'], playlist.get('description', ''), playlist.get('cover_url'), playlist.get('items') or [],
                fnos_guid=(existing_target or {}).get('fnos_guid') or _bound_fnos_guid(playlist['provider'], str(playlist['playlist_id'])) or None,
                mirror=_push_mode(kind, playlist.get('playlist_id')) == 'mirror',
                progress_cb=progress_cb,
            )
        except Exception as exc:
            db.execute(
                '''update push_targets set run_status='failed',run_stage='retry_wait',run_completed_at=?,
                   last_error=?,run_last_checked_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP where id=?''',
                (_run_now(), f'飞牛歌单同步失败: {str(exc)[:260]}', target_row_id),
            )
            _push_history(target_row_id, 'push', 'failed', {'total': pre_total, 'error': str(exc)[:200]})
            return
        queue = (await _enqueue_playlist_items(playlist.get('items', []), 4)) if options.get('download_missing', True) else None
        # 2026-09-25 修复（BUG-001）：订阅字段改补丁语义，未提供的保留既有订阅值。
        final_options = {**options, '_run_started_at': run_started_at, '_queue': queue or {}}
        _upsert_push_target(playlist, kind, str(options.get('title') or playlist['title']), result, final_options)

    asyncio.create_task(_run_push_job())
    return {'status': 'running', 'playlist_id': playlist_row_id, 'push_target_id': target_row_id,
            'total': pre_total, 'subscribed': bool(subscription), 'subscription': subscription,
            'mode': 'incremental_append' if existing_target and existing_target.get('fnos_guid') else 'initial_push'}


@app.post('/api/fnos/daily/{provider}/push')
async def push_daily_to_fnos(provider: str, payload: dict | None = None, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token); provider = _provider_name(provider); options = payload or {}
    daily = await platform_daily(provider, x_fn_token)
    title = str(options.get('title') or ('网易云 · 每日推荐' if provider == 'netease' else 'QQ 音乐 · 每日推荐'))
    items = [{**item, 'position': index} for index, item in enumerate(daily.get('items') or [])]
    result = {
        'provider': provider, 'playlist_id': 'daily', 'title': title,
        'kind': 'daily', 'description': '由 fnmusic-flow 每日更新',
        'cover_url': _daily_cover(items), 'owner': 'fnmusic-flow',
        'items': items,
    }
    saved = await _save_playlist(result, {'source_url': f'daily:{provider}'})
    _attach_track_paths(saved.get('items', []))
    run_started_at = _run_now()
    try:
        existing_target = db.fetchone(
            'select * from push_targets where provider=? and playlist_id=? and kind=?',
            (provider, 'daily', 'daily'),
        )
        pushed = await FnosAdapter(os.getenv('FNOS_API_URL'), os.getenv('FNOS_TOKEN')).create_or_update_playlist(
            title, saved.get('description',''), saved.get('cover_url'), saved.get('items',[]),
            fnos_guid=(existing_target or {}).get('fnos_guid'),
            mirror=True,
        )
    except Exception as exc:
        raise HTTPException(502, f'飞牛日推同步失败: {str(exc)[:200]}') from exc
    # 2026-09-25 修复（BUG-001）：日推订阅同样只透传请求明确提供的字段；
    # 卡片未填时刻时保留原锚点（旧行为强制回落 06:00，会覆盖用户自设值）。
    subscription = None
    if options.get('auto_sync', False):
        interval = (_subscription_interval('daily', options.get('interval_minutes'))
                    if 'interval_minutes' in options else _UNSET)
        if 'schedule_time' in options:
            anchor = str(options.get('schedule_time') or '').strip() or None
            if anchor and _parse_hhmm(anchor) is None:
                raise HTTPException(400, '每日更新时刻必须是 HH:MM')
        else:
            anchor = _UNSET
        subscription = _upsert_subscription(provider, 'daily', 'daily', interval, title, anchor)
    queue = (await _enqueue_playlist_items(saved.get('items', []), 3, 'daily')) if options.get('download_missing', True) else None
    push_target = _upsert_push_target(saved, 'daily', title, pushed,
                                      {**options, '_run_started_at': run_started_at, '_queue': queue or {}})
    return {'status': push_target.get('run_status'), 'playlist_id': saved['id'], 'subscribed': bool(subscription),
            'mode': 'incremental_append' if existing_target and existing_target.get('fnos_guid') else 'initial_push',
            'reused_fnos_playlist': bool(pushed.get('reused_existing_playlist')),
            'subscription': subscription, 'push_target': push_target, 'result': pushed, 'queue': queue}


@app.get('/api/fnos/push-targets')
async def fnos_push_targets(status: str | None = None, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    # schedule_time 必须与 _target_with_details 保持一致：前端推送卡片靠它回填
    # 「每天更新时刻」，漏选会让两段式调度的锚点在 UI 上永远显示为空。
    sql = '''select t.*,s.id subscription_id,s.enabled auto_sync,s.interval_minutes,s.schedule_time,
                    s.next_run_at,
                    s.last_run_at sync_last_run_at,s.last_status sync_status,p.title source_title,c.cover_url,
                    (select count(*) from push_history h where h.target_id=t.id) history_count
             from push_targets t
             left join subscriptions s on s.provider=t.provider and s.playlist_id=t.playlist_id and s.kind=t.kind
             left join playlists p on p.id=t.playlist_row_id
             left join playlist_covers c on c.playlist_row_id=t.playlist_row_id'''
    params: tuple = ()
    if status:
        allowed = {part.strip() for part in status.split(',') if part.strip()}
        if not allowed <= {'active', 'scheduled', 'cancelled', 'expired'}:
            raise HTTPException(400, '推送状态筛选值不正确')
        placeholders = ','.join('?' for _ in allowed)
        sql += f' where t.status in ({placeholders})'
        params = tuple(sorted(allowed))
    return db.fetchall(sql + ' order by t.updated_at desc,t.id desc', params)


@app.get('/api/fnos/push-history')
async def fnos_push_history(target_id: int | None = None, limit: int = 200, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    limit = min(1000, max(1, int(limit)))
    sql = '''select h.*,t.provider,t.playlist_id,t.kind,t.target_title,t.status target_status,
                    t.retention_mode,t.expires_at,t.scheduled_push_at,t.fnos_guid
             from push_history h join push_targets t on t.id=h.target_id'''
    params: tuple = ()
    if target_id is not None:
        sql += ' where h.target_id=?'
        params = (target_id,)
    return db.fetchall(sql + f' order by h.id desc limit {limit}', params)


@app.get('/api/fnos/push-targets/{target_id}/items')
async def fnos_push_target_items(target_id: int, status: str | None = None, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    if not db.fetchone('select id from push_targets where id=?', (target_id,)):
        raise HTTPException(404, '推送记录不存在')
    allowed = {'pushed', 'ready_to_append', 'fnos_match_failed', 'download_failed', 'downloading', 'not_downloaded'}
    values = {part.strip() for part in str(status or '').split(',') if part.strip()}
    if values and not values <= allowed:
        raise HTTPException(400, '歌曲状态筛选值不正确')
    sql, params = 'select * from push_item_status where target_id=?', [target_id]
    if values:
        sql += ' and status in (' + ','.join('?' for _ in values) + ')'
        params.extend(sorted(values))
    rows = db.fetchall(sql + ' order by position', tuple(params))
    counts = {row['status']: 0 for row in rows}
    for row in rows:
        counts[row['status']] = counts.get(row['status'], 0) + 1
    return {'target_id': target_id, 'total': len(rows), 'counts': counts, 'items': rows}


@app.patch('/api/fnos/push-targets/{target_id}')
async def update_push_target(target_id: int, payload: dict, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    target = _target_with_details(target_id)
    if not target:
        raise HTTPException(404, '推送记录不存在')
    title = str(payload.get('target_title', target['target_title'])).strip()
    if not title:
        raise HTTPException(400, '飞牛歌单名称不能为空')
    retention = str(payload.get('retention_mode', target.get('retention_mode') or 'permanent'))
    custom_expiry = payload.get('expires_at') if 'expires_at' in payload else target.get('expires_at')
    expires_at = _retention_expiry(retention, custom_expiry)
    scheduled_at = target.get('scheduled_push_at')
    status_value = target['status']
    if 'scheduled_push_at' in payload:
        scheduled = _parse_client_time(payload.get('scheduled_push_at'), '预约推送时间')
        if scheduled is not None and scheduled <= datetime.now(timezone.utc):
            raise HTTPException(400, '预约推送时间必须晚于当前时间')
        if scheduled is not None and expires_at and scheduled >= _stored_time(expires_at):
            raise HTTPException(400, '到期时间必须晚于预约推送时间')
        scheduled_at = _sql_time(scheduled)
        if scheduled_at:
            status_value = 'scheduled'
        elif target['status'] == 'scheduled':
            status_value = 'active'
    scheduled_auto = target.get('scheduled_auto_sync')
    scheduled_interval = target.get('scheduled_interval_minutes')
    if 'scheduled_push_at' in payload:
        scheduled_auto = (1 if bool(payload.get('auto_sync')) else 0) if scheduled_at and 'auto_sync' in payload else None
        scheduled_interval = _subscription_interval(target['kind'], payload.get('interval_minutes')) if scheduled_at else None
    db.execute(
        '''update push_targets set target_title=?,retention_mode=?,expires_at=?,scheduled_push_at=?,status=?,
           scheduled_auto_sync=?,scheduled_interval_minutes=?,updated_at=CURRENT_TIMESTAMP
           where id=?''',
        (title, retention, expires_at, scheduled_at, status_value, scheduled_auto, scheduled_interval, target_id),
    )
    subscription = None
    enabled = payload.get('auto_sync')
    interval = payload.get('interval_minutes')
    # 两段式调度：每日锚点（schedule_time，北京时间 HH:MM）+ 检查间隔
    # （interval_minutes，0 = 不循环）。两个可以单用也可以组合。
    #
    # 这段必须与 PATCH /api/subscriptions/{id} 用同一套语义：早先的实现只认
    # auto_sync / interval_minutes，**把前端提交的 schedule_time 整个丢掉**，
    # 于是在推送卡片上设的「每天更新时刻」保存后无任何效果；同时 next_run_at
    # 用的是「now + interval」这种忽略锚点的滚动算法，锚点+间隔的组合必然算错。
    # 现在统一交给 _next_subscription_run 推导。
    raw_anchor = payload.get('schedule_time') if 'schedule_time' in payload else None
    anchor: str | None = str(raw_anchor).strip() if raw_anchor else None
    if anchor and _parse_hhmm(anchor) is None:
        raise HTTPException(400, '每日更新时刻必须是 HH:MM（北京时间）')
    if any(key in payload for key in ('auto_sync', 'interval_minutes', 'schedule_time')):
        existing = db.fetchone(
            'select * from subscriptions where provider=? and playlist_id=? and kind=?',
            (target['provider'], target['playlist_id'], target['kind']),
        )
        # 只改间隔时沿用原锚点，避免把「每天 6 点」这个前提悄悄抹掉。
        if 'schedule_time' not in payload and existing:
            anchor = existing.get('schedule_time')
        interval_value = _subscription_interval(
            target['kind'],
            interval if interval is not None else (existing or {}).get('interval_minutes'),
        )
        wants_auto = bool(enabled) if enabled is not None else bool(existing and existing.get('enabled'))
        plan_next = _next_subscription_run(interval_value, anchor)
        if not wants_auto or plan_next is None:
            # 关自动更新，或「不循环且无锚点」：next_run_at 必须留空，否则调度器
            # 会把 null 当成「已到期」而每 tick 反复触发。
            if existing:
                db.execute(
                    'update subscriptions set enabled=0,interval_minutes=?,schedule_time=?,'
                    "target_title=?,next_run_at=null,last_status='paused',last_error=null where id=?",
                    (interval_value, anchor, title, existing['id']),
                )
        elif existing:
            db.execute(
                'update subscriptions set enabled=1,interval_minutes=?,schedule_time=?,'
                'target_title=?,next_run_at=? where id=?',
                (interval_value, anchor, title, plan_next, existing['id']),
            )
        else:
            _upsert_subscription(
                target['provider'], target['playlist_id'], target['kind'],
                interval_value, title, anchor,
            )
        subscription = db.fetchone(
            'select * from subscriptions where provider=? and playlist_id=? and kind=?',
            (target['provider'], target['playlist_id'], target['kind']),
        )
    _push_history(target_id, 'schedule' if scheduled_at else 'settings', 'success', {
        'retention_mode': retention, 'expires_at': expires_at, 'scheduled_push_at': scheduled_at,
        'auto_sync': payload.get('auto_sync'), 'interval_minutes': payload.get('interval_minutes'),
        'schedule_time': anchor,
    })
    return {'target': _target_with_details(target_id), 'subscription': subscription}


@app.post('/api/fnos/push-targets/{target_id}/cancel')
async def cancel_push_target(target_id: int, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    target = _target_with_details(target_id)
    if not target:
        raise HTTPException(404, '推送记录不存在')
    deleted = {'deleted': False, 'reason': 'guid_missing'}
    if target.get('fnos_guid'):
        try:
            deleted = await FnosAdapter(os.getenv('FNOS_API_URL'), os.getenv('FNOS_TOKEN')).delete_playlist(target['fnos_guid'])
        except Exception as exc:
            error = str(exc)[:240]
            db.execute('update push_targets set last_error=?,updated_at=CURRENT_TIMESTAMP where id=?', (error, target_id))
            _push_history(target_id, 'cancel', 'failed', {'error': error, 'stage': 'delete_fnos_playlist'})
            raise HTTPException(502, f'删除飞牛音乐歌单失败，自动同步尚未取消: {error}') from exc
    db.execute(
        """update push_targets set status='cancelled',fnos_guid=null,scheduled_push_at=null,
           scheduled_auto_sync=null,scheduled_interval_minutes=null,last_error=null,
           run_status=case when run_status='running' then 'cancelled' else run_status end,
           run_stage=case when run_status='running' then 'cancelled' else run_stage end,
           run_completed_at=case when run_status='running' then CURRENT_TIMESTAMP else run_completed_at end,
           updated_at=CURRENT_TIMESTAMP where id=?""",
        (target_id,),
    )
    db.execute(
        "update subscriptions set enabled=0,last_status='cancelled',next_run_at=null where provider=? and playlist_id=? and kind=?",
        (target['provider'], target['playlist_id'], target['kind']),
    )
    _push_history(target_id, 'cancel', 'success', {
        'fnos_playlist_deleted': bool(deleted.get('deleted')),
        'deleted_guid': target.get('fnos_guid'),
        'message': '已停止同步并删除飞牛音乐歌单；来源快照保留在历史中，可随时恢复',
    })
    return {'status': 'cancelled', 'fnos_playlist_deleted': bool(deleted.get('deleted')),
            'target': _target_with_details(target_id)}


@app.post('/api/fnos/push-targets/{target_id}/repush')
async def repush_target(target_id: int, payload: dict | None = None, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    target = _target_with_details(target_id)
    if not target:
        raise HTTPException(404, '推送记录不存在')
    options = payload or {}
    if options:
        retention = str(options.get('retention_mode', target.get('retention_mode') or 'permanent'))
        custom_expiry = options.get('expires_at') if 'expires_at' in options else target.get('expires_at')
        expires_at = _retention_expiry(retention, custom_expiry)
        scheduled = _parse_client_time(options.get('scheduled_push_at'), '预约推送时间') if options.get('scheduled_push_at') else None
        if scheduled is not None:
            if scheduled <= datetime.now(timezone.utc):
                raise HTTPException(400, '预约推送时间必须晚于当前时间')
            if expires_at and scheduled >= _stored_time(expires_at):
                raise HTTPException(400, '到期时间必须晚于预约推送时间')
            db.execute(
                """update push_targets set retention_mode=?,expires_at=?,scheduled_push_at=?,scheduled_auto_sync=?,
                   scheduled_interval_minutes=?,status='scheduled',last_error=null,updated_at=CURRENT_TIMESTAMP where id=?""",
                (retention, expires_at, _sql_time(scheduled),
                 1 if bool(options.get('auto_sync')) else 0,
                 _subscription_interval(target['kind'], options.get('interval_minutes')), target_id),
            )
            _push_history(target_id, 'schedule', 'success', {'scheduled_push_at': _sql_time(scheduled), 'retention_mode': retention, 'expires_at': expires_at})
            return {'status': 'scheduled', 'target': _target_with_details(target_id)}
        db.execute(
            'update push_targets set retention_mode=?,expires_at=?,updated_at=CURRENT_TIMESTAMP where id=?',
            (retention, expires_at, target_id),
        )
    result = await _execute_push_target(target_id, 'repush')
    if 'auto_sync' in options or 'interval_minutes' in options or 'schedule_time' in options:
        # 两段式调度：每日锚点（schedule_time，北京时间 HH:MM）+ 检查间隔
        # （interval_minutes，0 表示不循环）。两者可组合也可单用。
        # 只提交 interval_minutes 时沿用库里的锚点：否则「改间隔」会把用户设好的
        # 每天时刻一起清掉，与 PATCH /api/fnos/push-targets/{id} 行为不一致。
        existing = db.fetchone(
            'select * from subscriptions where provider=? and playlist_id=? and kind=?',
            (target['provider'], target['playlist_id'], target['kind']),
        )
        if 'schedule_time' in options:
            raw_anchor = options.get('schedule_time')
            anchor = str(raw_anchor).strip() if raw_anchor else None
        else:
            anchor = (existing or {}).get('schedule_time')
        if anchor and _parse_hhmm(anchor) is None:
            raise HTTPException(400, '每日更新时刻必须是 HH:MM（北京时间）')
        interval = _subscription_interval(
            target['kind'],
            options.get('interval_minutes') if 'interval_minutes' in options
            else (existing or {}).get('interval_minutes'),
        )
        if 'auto_sync' in options:
            wants_auto = bool(options.get('auto_sync'))
        else:
            wants_auto = bool(existing and existing.get('enabled'))
        plan_next = _next_subscription_run(interval, anchor)
        if wants_auto and plan_next is not None:
            _upsert_subscription(
                target['provider'], target['playlist_id'], target['kind'],
                interval, target['target_title'], anchor,
            )
        else:
            db.execute(
                "update subscriptions set enabled=0,interval_minutes=?,schedule_time=?,"
                "last_status='paused',next_run_at=null where provider=? and playlist_id=? and kind=?",
                (interval, anchor, target['provider'], target['playlist_id'], target['kind']),
            )
        result['target'] = _target_with_details(target_id)
    return result


@app.post('/api/fnos/playlists/{playlist_row_id}/append')
async def append_playlist_to_fnos(playlist_row_id: int, payload: dict | None = None, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    """Append newly available tracks to the already-bound fnOS playlist.

    This endpoint deliberately refuses to create a target.  The caller must
    have performed the initial push first, which makes the operation safe to
    repeat after more downloads complete.
    """
    guard(x_fn_token)
    playlist = db.fetchone('select * from playlists where id=?', (playlist_row_id,))
    if not playlist:
        raise HTTPException(404, '歌单不存在')
    # 2026-09-18 修复：日推/雷达歌单的 push_target 记在 kind='daily'/'radar' 下，
    # 旧逻辑只认 chart/playlist，导致对日推歌单点「追加」必然 409。
    # 2026-09-20 起统一走 _canonical_kind，并按同源身份兜底收养目标（kind 不敏感）。
    kind = _canonical_kind(playlist['kind'], playlist['playlist_id'], playlist['title'])
    target = _find_target_for_source(playlist['provider'], str(playlist['playlist_id']), kind)
    if not target or not target.get('fnos_guid'):
        raise HTTPException(409, '该歌单还没有绑定的飞牛目标歌单，请先进行首次推送')
    # Reactivating an expired/cancelled record is intentional: the user asked
    # for a one-shot append, while the existing target identity is retained.
    db.execute("update push_targets set status='active',last_error=null,updated_at=CURRENT_TIMESTAMP where id=?", (target['id'],))
    result = await _execute_push_target(target['id'], 'append')
    result['mode'] = 'incremental_append'
    result['reused_fnos_playlist'] = True
    result['target'] = _target_with_details(target['id'])
    return result

@app.post('/api/fnos/favorites/sync')
async def sync_fnos_favorites(x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    try:
        return await sync_favorites(db, music_root)
    except Exception as exc:
        raise HTTPException(502, f'飞牛收藏读取失败: {str(exc)[:200]}') from exc
