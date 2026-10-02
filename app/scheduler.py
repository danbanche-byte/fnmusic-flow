from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone

from .platforms import NeteaseConnector, QQMusicConnector, PlatformError
from .favorite_sync import sync_favorites
from .fnos import FnosAdapter, load_fnos_token
from .daily_cache import cleanup_expired, record_fnos_history
from pathlib import Path


BEIJING = timezone(timedelta(hours=8))


def _sql_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')


def _parse_stored_moment(value: object) -> datetime | None:
    """解析存量时间字段（expires_at / scheduled_push_at）。

    兼容 `YYYY-MM-DD HH:MM:SS`、ISO `T` 分隔、带时区偏移与 `Z` 后缀；
    解析不了返回 None，调用方按"未到期"处理，避免误停误删。
    """
    text = str(value or '').strip()
    if not text:
        return None
    candidate = text[:-1] + '+00:00' if text.endswith('Z') else text
    parsed: datetime | None = None
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M', '%Y/%m/%d %H:%M:%S'):
            try:
                parsed = datetime.strptime(candidate, fmt)
                break
            except ValueError:
                continue
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _snapshot_diff(previous_row: dict | None, previous_items: list[dict],
                   result: dict, provider: str) -> tuple[bool, bool, bool]:
    """来源快照差异三分项（2026-09-25 修复 BUG-002）：歌曲 / 标题 / 封面各自判断。

    旧逻辑只比较 (platform, song_id) 序列 —— 歌曲不变、标题或封面单独变化时，
    同步被判成 changed_none 整体跳过，改名与封面更新永远不会执行。现在：

    - previous_row 为 None（库里还没有快照）→ 三项都视为有变化，走完整同步；
    - 歌曲列表为空的旧快照也视为「没有可比基线」→ 走完整同步；
    - 标题比较 playlists.title 与本次 result.title；
    - 封面比较 playlist_covers.cover_url 与本次 result.cover_url。

    返回 ``(tracks_changed, title_changed, cover_changed)``，调用方据此决定
    是否跳过整次同步、以及是否跳过下载入队（只有歌曲变化才需要下载）。
    """
    if previous_row is None:
        return True, True, True
    previous_snapshot = tuple(
        (str(item['platform']), str(item['song_id']))
        for item in previous_items
    )
    current_snapshot = tuple(
        (str(item.get('platform') or provider), str(item['song_id']))
        for item in (result.get('items') or []) if item.get('song_id')
    )
    tracks_changed = (not previous_snapshot) or current_snapshot != previous_snapshot
    title_changed = str(previous_row.get('title') or '') != str(result.get('title') or '')
    cover_changed = str(previous_row.get('cover_url') or '') != str(result.get('cover_url') or '')
    return tracks_changed, title_changed, cover_changed


def _daily_start_parts() -> tuple[int, int]:
    value = os.getenv('DAILY_FETCH_START', '06:00').strip()
    try:
        hour, minute = (int(part) for part in value.split(':', 1))
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError
        return hour, minute
    except (TypeError, ValueError):
        return 6, 0


def _daily_interval_minutes() -> int:
    try:
        return max(5, int(os.getenv('DAILY_FETCH_INTERVAL_MINUTES', '30')))
    except ValueError:
        return 30


def _next_daily_retry(now: datetime | None = None) -> str:
    local = (now or datetime.now(timezone.utc)).astimezone(BEIJING)
    interval = _daily_interval_minutes()
    minutes = local.hour * 60 + local.minute
    next_minutes = ((minutes // interval) + 1) * interval
    candidate = local.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(minutes=next_minutes)
    return _sql_utc(candidate)


def _next_daily_start(now: datetime | None = None) -> str:
    local = (now or datetime.now(timezone.utc)).astimezone(BEIJING)
    hour, minute = _daily_start_parts()
    candidate = local.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if candidate <= local:
        candidate += timedelta(days=1)
    return _sql_utc(candidate)


_MINUTES_PER_DAY = 1440


def _subscription_next_run(row: dict) -> str | None:
    """按订阅行的两段式配置算下次执行时间。

    ``interval_minutes`` 与 ``schedule_time`` 均可单独使用或组合：
    只设锚点=每天该时刻一次；只设间隔=每 N 分钟；两者都设=锚点首跑后按间隔
    检查。返回 None 表示「不循环且无锚点」，调用方应关闭该订阅。
    """
    from .full import _next_subscription_run
    try:
        interval = int(row.get('interval_minutes') or 0)
    except (TypeError, ValueError):
        interval = 0
    return _next_subscription_run(interval, row.get('schedule_time'))


def _daily_next_retry(row: dict, now: datetime | None = None) -> str:
    """日推「平台还没更新」时的下次检查时间。

    用户显式配置了每日锚点或短周期（如「每天 6 点，之后每小时检查」）时按配置
    走；未配置则沿用 DAILY_FETCH_INTERVAL_MINUTES（默认 30 分钟）的原有行为。
    """
    from .full import _next_subscription_run
    try:
        interval = int(row.get('interval_minutes') or 0)
    except (TypeError, ValueError):
        interval = 0
    if row.get('schedule_time') or 0 < interval < _MINUTES_PER_DAY:
        value = _next_subscription_run(interval, row.get('schedule_time'))
        if value:
            return value
    return _next_daily_retry(now)


class SyncScheduler:
    def __init__(self, db):
        self.db = db
        self.interval = max(5, int(os.getenv('SYNC_INTERVAL_MINUTES', '30')))
        self._last_daily_slot: tuple | None = None

    async def sync_provider(self, provider: str):
        account = self.db.fetchone("select * from accounts where provider=? and cookie is not null and status='connected'", (provider,))
        if not account: return {'provider': provider, 'status': 'skipped', 'reason': 'not_connected'}
        connector = NeteaseConnector(account.get('cookie')) if provider == 'netease' else QQMusicConnector(account.get('cookie'))
        data = await connector.daily()
        rows = data if provider == 'qq' else self._rows(data, provider)
        rows = [row for row in rows if isinstance(row, dict) and row.get('song_id') and row.get('title')]
        previous = {row['song_id'] for row in self.db.fetchall('select song_id from daily_recommendations where provider=?', (provider,))}
        with self.db.transaction() as conn:
            conn.execute('delete from daily_recommendations where provider=?', (provider,))
            conn.executemany(
                '''insert into daily_recommendations(provider,song_id,title,artist,album,duration_ms,cover_url,fetched_at)
                   values(?,?,?,?,?,?,?,CURRENT_TIMESTAMP)''',
                ((provider, str(row['song_id']), row['title'], row.get('artist',''), row.get('album',''), row.get('duration_ms'), row.get('cover_url')) for row in rows),
            )
            conn.commit()
        new_count = sum(1 for row in rows if str(row['song_id']) not in previous)
        return {'provider': provider, 'status': 'preview_ready', 'received': len(rows), 'new': new_count, 'downloads_created': 0}

    async def sync_subscriptions(self, subscription_id: int | None = None, force: bool = False):
        token = load_fnos_token()
        if not token:
            return {'provider': 'playlist_subscriptions', 'status': 'skipped', 'reason': 'fnos_token_missing'}
        from .full import (_attach_track_paths, _bound_fnos_guid, _canonical_kind,
                           _enqueue_playlist_items, _find_target_for_source,
                           _normalize_tracks, _playlist_payload, _push_mode,
                           _run_now, _save_playlist, _upsert_push_target)
        adapter = FnosAdapter(os.getenv('FNOS_API_URL'), token)
        if subscription_id is not None:
            rows = self.db.fetchall(
                'select * from subscriptions where id=?' + ('' if force else ' and enabled=1'),
                (subscription_id,),
            )
        else:
            rows = self.db.fetchall(
                """select * from subscriptions where enabled=1
                   and (next_run_at is null or datetime(next_run_at) <= CURRENT_TIMESTAMP)
                   order by id"""
            )
        synced = failed = 0
        errors = []
        for row in rows:
            try:
                account = self.db.fetchone('select * from accounts where provider=?', (row['provider'],)) or {}
                connector = NeteaseConnector(account.get('cookie')) if row['provider'] == 'netease' else QQMusicConnector(account.get('cookie'))
                playlist_id = str(row['playlist_id'])
                # 2026-09-20 修复：kind 归一 —— 雷达 ID 清单 / 标题含「雷达」一律 radar，
                # 避免订阅与推送目标因 kind 分裂（playlist vs radar）互相对不上。
                kind = _canonical_kind(row.get('kind'), playlist_id, row.get('target_title') or '')
                if kind == 'daily':
                    previous = self.db.fetchall(
                        '''select i.platform,i.song_id from playlists p join playlist_items i on i.playlist_id=p.id
                           where p.provider=? and p.playlist_id='daily' and p.kind='daily' order by i.position''',
                        (row['provider'],),
                    )
                    previous_ids = tuple((str(item['platform']), str(item['song_id'])) for item in previous)
                    raw = await connector.daily()
                    items = _normalize_tracks(raw, row['provider']) if row['provider'] == 'netease' else raw
                    items = [item for item in items if isinstance(item, dict) and item.get('song_id')]
                    if not items:
                        raise RuntimeError('平台返回的日推歌单为空，保留现有歌单并稍后重试')
                    current_ids = tuple((str(item.get('platform') or row['provider']), str(item['song_id'])) for item in items)
                    if previous_ids and current_ids == previous_ids and not force:
                        self.db.execute(
                            "update subscriptions set last_run_at=CURRENT_TIMESTAMP,next_run_at=?,last_status='waiting_update',last_error=null where id=?",
                            (_daily_next_retry(row), row['id']),
                        )
                        continue
                    # Preserve the real album artwork when the scheduled daily
                    # snapshot is pushed.  Leaving this null caused the
                    # daily playlist cover to disappear on every refresh.
                    from .full import _daily_cover
                    result = {
                        'provider': row['provider'], 'playlist_id': playlist_id,
                        'title': row.get('target_title') or ('网易云 · 每日推荐' if row['provider'] == 'netease' else 'QQ 音乐 · 每日推荐'),
                        'kind': 'daily', 'description': '每日自动更新',
                        'cover_url': _daily_cover(items), 'owner': 'fnmusic-flow',
                        'items': [{**item, 'position': index} for index, item in enumerate(items)],
                    }
                elif playlist_id.startswith('chart:'):
                    raw = await connector.chart(playlist_id.partition(':')[2])
                    result = raw if row['provider'] == 'qq' else _playlist_payload(raw, row['provider'], playlist_id)
                else:
                    raw = await connector.playlist(playlist_id)
                    result = raw if row['provider'] == 'qq' else _playlist_payload(raw, row['provider'], playlist_id)
                result['playlist_id'] = playlist_id
                # 内容快照短路（用户要求的「有更新就继续更新、没更新就跳过」）：
                # 上游内容与库内快照一致时不推送飞牛、不重建下载队列，只滚动
                # next_run_at 并标记 changed_none，避免无意义地重写飞牛歌单。
                # 2026-09-25 修复（BUG-002）：变化指纹从「歌曲 ID+顺序」扩展为
                # 「歌曲 + 标题 + 封面」三分项 —— 元数据单独变化不再被判成
                # changed_none 而跳过整个同步；只有歌曲变化才入队下载。
                tracks_changed = True
                if kind != 'daily' and not force:
                    existing_row = self.db.fetchone(
                        '''select p.id,p.title,c.cover_url from playlists p
                           left join playlist_covers c on c.playlist_row_id=p.id
                           where p.provider=? and p.playlist_id=?''',
                        (row['provider'], playlist_id),
                    )
                    if existing_row:
                        previous_items = self.db.fetchall(
                            'select platform,song_id from playlist_items where playlist_id=? order by position',
                            (existing_row['id'],),
                        )
                        tracks_changed, title_changed, cover_changed = _snapshot_diff(
                            existing_row, previous_items, result, row['provider'])
                        if not tracks_changed and not title_changed and not cover_changed:
                            self.db.execute(
                                "update subscriptions set last_run_at=CURRENT_TIMESTAMP,next_run_at=?,last_status='changed_none',last_error=null where id=?",
                                (_subscription_next_run(row) or _sql_utc(datetime.now(timezone.utc) + timedelta(days=1)), row['id']),
                            )
                            continue
                saved = await _save_playlist(result, {})
                _attach_track_paths(saved.get('items') or [])
                run_started_at = _run_now()
                target = _find_target_for_source(row['provider'], row['playlist_id'], kind)
                pushed = await adapter.create_or_update_playlist(
                    # 2026-09-20 修复：雷达歌单标题以来源快照的最新名优先 ——
                    # 源平台每天换标题（「今天从《XX》听起|私人雷达」），旧实现
                    # 固定用订阅创建当天的 target_title，歌单名/封面永远停在旧名。
                    (saved['title'] if kind == 'radar' else None) or row.get('target_title') or saved['title'],
                    saved.get('description',''), saved.get('cover_url'), saved.get('items',[]),
                    fnos_guid=(target or {}).get('fnos_guid') or _bound_fnos_guid(row['provider'], row['playlist_id']),
                    mirror=_push_mode(kind, playlist_id) == 'mirror',
                )
                # 2026-09-25（BUG-002/A2）：只有歌曲集合变化才需要下载缺歌；
                # 仅标题/封面变化时跳过入队，队列键保持形状一致供状态落库。
                if kind == 'daily' or tracks_changed:
                    queue = await _enqueue_playlist_items(
                        saved.get('items', []), 3 if kind == 'daily' else 4,
                        'daily' if kind == 'daily' else 'playlist',
                    )
                else:
                    queue = {'total': len(saved.get('items') or []), 'tasks_created': 0,
                             'active_reused': 0, 'library_reused': 0, 'cache_promoted': 0,
                             'skipped_reason': 'tracks_unchanged'}
                # 日推未显式配置锚点时沿用 DAILY_START 的环境默认（默认 06:00），
                # 其余情况一律按订阅自己的两段式配置计算下次执行时间。
                if kind == 'daily' and not row.get('schedule_time'):
                    next_run = _next_daily_start()
                else:
                    next_run = _subscription_next_run(row)
                if next_run is None:
                    # 「不循环」且没有每日锚点：推完这一次就关闭。若保留
                    # enabled=1 而 next_run_at 为 null，调度器会把它当成「已到期」
                    # 而在每个 tick 反复触发。
                    self.db.execute(
                        "update subscriptions set enabled=0,last_run_at=CURRENT_TIMESTAMP,next_run_at=null,last_status='done_once',last_error=null where id=?",
                        (row['id'],),
                    )
                else:
                    self.db.execute(
                        "update subscriptions set last_run_at=CURRENT_TIMESTAMP,next_run_at=?,last_status='success',last_error=null where id=?",
                        (next_run, row['id']),
                    )
                target = _find_target_for_source(row['provider'], row['playlist_id'], kind)
                if target:
                    _upsert_push_target(saved, kind, (saved['title'] if kind == 'radar' else None) or row.get('target_title') or saved['title'], pushed, {
                        'retention_mode': target.get('retention_mode') or 'permanent',
                        'expires_at': target.get('expires_at'), '_run_started_at': run_started_at,
                        '_queue': queue, '_action': 'auto_sync',
                    })
                synced += 1
            except Exception as exc:
                failed += 1
                error = str(exc)[:160]
                retry_at = (datetime.now(timezone.utc) + timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:%S')
                self.db.execute(
                    "update subscriptions set next_run_at=?,last_status='failed',last_error=? where id=?",
                    (retry_at, error, row['id']),
                )
                errors.append({'id': row['id'], 'error': error})
        return {'provider': 'playlist_subscriptions', 'status': 'ok' if not failed else 'partial',
                'received': len(rows), 'synced': synced, 'failed': failed, 'errors': errors}

    async def process_push_targets(self):
        candidates = self.db.fetchall(
            "select * from push_targets where status in ('active','scheduled') and expires_at is not null"
        )
        now = datetime.now(timezone.utc)
        # Server-side parsing instead of SQLite string comparison: a legacy or
        # oddly formatted expires_at used to make datetime() return NULL and the
        # row never expire. Unparseable values are treated as "not due yet".
        expired = [row for row in candidates
                   if (moment := _parse_stored_moment(row.get('expires_at'))) is not None and moment <= now]
        expired_count = expire_failed = 0
        token = load_fnos_token()
        adapter = FnosAdapter(os.getenv('FNOS_API_URL'), token) if token else None
        expire_errors = []
        for target in expired:
            if target.get('fnos_guid'):
                if not adapter:
                    error = '飞牛音乐未连接，暂不能删除到期歌单'
                    self.db.execute('update push_targets set last_error=?,updated_at=CURRENT_TIMESTAMP where id=?', (error, target['id']))
                    expire_errors.append({'id': target['id'], 'error': error})
                    expire_failed += 1
                    continue
                try:
                    await adapter.delete_playlist(target['fnos_guid'])
                except Exception as exc:
                    error = f"删除到期飞牛歌单失败: {str(exc)[:220]}"
                    self.db.execute('update push_targets set last_error=?,updated_at=CURRENT_TIMESTAMP where id=?', (error, target['id']))
                    self.db.execute(
                        "insert into push_history(target_id,action,status,detail) values(?, 'expire', 'failed', ?)",
                        (target['id'], error),
                    )
                    expire_errors.append({'id': target['id'], 'error': error})
                    expire_failed += 1
                    continue
            self.db.execute(
                "update push_targets set status='expired',fnos_guid=null,scheduled_push_at=null,scheduled_auto_sync=null,scheduled_interval_minutes=null,last_error=null,updated_at=CURRENT_TIMESTAMP where id=?",
                (target['id'],),
            )
            self.db.execute(
                "update subscriptions set enabled=0,last_status='paused' where provider=? and playlist_id=? and kind=?",
                (target['provider'], target['playlist_id'], target['kind']),
            )
            self.db.execute(
                "insert into push_history(target_id,action,status,detail) values(?, 'expire', 'success', ?)",
                (target['id'], '已到期、停止同步并删除飞牛音乐歌单；记录已进入历史，可重新推送恢复'),
            )
            expired_count += 1

        due_candidates = self.db.fetchall(
            "select * from push_targets where status='scheduled' and scheduled_push_at is not null order by id"
        )
        due = [row for row in due_candidates
               if (moment := _parse_stored_moment(row.get('scheduled_push_at'))) is not None and moment <= now]
        pushed = failed = 0
        errors = []
        if due and load_fnos_token():
            from .full import _execute_push_target
            for row in due:
                try:
                    await _execute_push_target(row['id'], 'scheduled_push')
                    pushed += 1
                except Exception as exc:
                    failed += 1
                    errors.append({'id': row['id'], 'error': str(exc)[:200]})
        errors.extend(expire_errors)
        return {'provider': 'push_targets', 'status': 'ok' if not (failed or expire_failed) else 'partial',
                'expired': expired_count, 'expire_failed': expire_failed, 'due': len(due),
                'pushed': pushed, 'failed': failed + expire_failed, 'errors': errors}

    def _daily_poll_due(self, now: datetime | None = None) -> bool:
        local = (now or datetime.now(timezone.utc)).astimezone(BEIJING)
        hour, minute = _daily_start_parts()
        start_minutes = hour * 60 + minute
        current_minutes = local.hour * 60 + local.minute
        if current_minutes < start_minutes:
            return False
        slot = (local.date().isoformat(), (current_minutes - start_minutes) // _daily_interval_minutes())
        if slot == self._last_daily_slot:
            return False
        self._last_daily_slot = slot
        return True

    async def maybe_run_for_you(self, now: datetime | None = None) -> dict | None:
        """「为我推荐」每日轮换（默认北京时间 06:00 起，失败按小时重试）。

        幂等性由 for_you.run_rotation 内部保证（当日已成功/预算用尽会跳过），
        调度器只负责到点触发；整轮失败不影响旧池，也不打断调度循环。
        """
        local = (now or datetime.now(timezone.utc)).astimezone(BEIJING)
        value = os.getenv('FOR_YOU_ROTATION_TIME', '06:00').strip()
        try:
            hour, minute = (int(part) for part in value.split(':', 1))
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                raise ValueError
        except (TypeError, ValueError):
            hour, minute = 6, 0
        start_minutes = hour * 60 + minute
        current_minutes = local.hour * 60 + local.minute
        if current_minutes < start_minutes:
            return None
        slot = (local.date().isoformat(), (current_minutes - start_minutes) // 60)
        if slot == getattr(self, '_last_for_you_slot', None):
            return None
        self._last_for_you_slot = slot
        from .for_you import run_rotation
        return await run_rotation(self.db, trigger='scheduled', now=now)

    async def maintain_daily_cache(self):
        token = load_fnos_token()
        music_root = Path(os.getenv('MUSIC_ROOT', '/music'))
        result = {'favorites': None, 'history': None, 'cleanup': None, 'errors': []}
        if token:
            adapter = FnosAdapter(os.getenv('FNOS_API_URL'), token)
            try:
                result['favorites'] = await sync_favorites(self.db, music_root)
            except Exception as exc:
                result['errors'].append({'stage': 'favorites', 'error': str(exc)[:200]})
            try:
                history = await adapter.play_history()
                result['history'] = record_fnos_history(self.db, music_root, history)
            except Exception as exc:
                result['errors'].append({'stage': 'play_history', 'error': str(exc)[:200]})
        result['cleanup'] = cleanup_expired(self.db, music_root)
        if token and result['cleanup'].get('removed'):
            try:
                await FnosAdapter(os.getenv('FNOS_API_URL'), token).refresh_library()
            except Exception as exc:
                result['errors'].append({'stage': 'library_refresh', 'error': str(exc)[:200]})
        return result

    # 2026-09-28 修复「推送快照冻结 / retry_wait 永不复算」：
    # 旧实现只捞 run_status='running'。异常分支把行改成 run_stage='retry_wait'
    # 但 run_status 仍是 'running'、run_attempts=0，而进入本循环的前提是
    #   (a) 该歌单还有 pending/matching/downloading 下载，或
    #   (b) 通过 scan/8s 闸门。
    # 下载已全部落地、扫描也做完之后，(a)(b) 都不再成立，于是这行**永远不会**
    # 被再次捞出来 —— push_item_status 里那批 `fnos_match_failed` 就成了
    # 永久冻结的旧快照，哪怕飞牛几分钟后完成了索引也不会转成 pushed。
    # 用户反馈的「文件已下载，但飞牛曲库没有找到唯一匹配」正是这个冻结快照。
    #
    # 修复：把 retry_wait 行一并捞出来复算，并给它们一个退避闸门
    # （RETRY_RECOMPUTE_SECONDS，默认 30s），避免 3s 循环里反复刷屏；
    # 复算次数用 run_retry_count 计数，超过 RETRY_MAX（默认 40，约 20 分钟）
    # 就把目标收敛为 partial，不再无限空转。
    RETRY_STALL_SECONDS = max(30, int(os.getenv('PUSH_RETRY_STALL_SECONDS', '600')))

    async def _recompute_retry_wait_runs(self, adapter) -> int:
        """复算停在 retry_wait 的推送目标：让 push_item_status 有机会从
        fnos_match_failed 转为 pushed / ready_to_append。返回处理条数。"""
        from .full import (_record_push_item_status, _run_now, _saved_target_playlist,
                           _stored_time)
        rows = self.db.fetchall(
            # 只复算「还有 fnos_match_failed、且没有活跃下载」的目标 —— 真正
            # 需要等飞牛索引补上的那一类。
            """select t.* from push_targets t
                where t.status='active' and t.run_status='running' and t.run_stage='retry_wait'
                  and exists(select 1 from push_item_status s
                              where s.target_id=t.id and s.status='fnos_match_failed')
                  and not exists(
                    select 1 from playlist_items i
                      join tracks tr on tr.platform=i.platform and tr.song_id=i.song_id
                      join tasks q on q.track_id=tr.id
                     where i.playlist_id=t.playlist_row_id
                       and q.status in ('pending','matching','downloading'))
                order by t.id"""
        )
        handled = 0
        now = datetime.now(timezone.utc)
        for target in rows:
            try:
                last = _stored_time(target.get('run_last_checked_at'))
                if last and (now - last).total_seconds() < 30:
                    continue
                started = _stored_time(target.get('run_started_at'))
                stalled = (now - started).total_seconds() if started else 0
                playlist = _saved_target_playlist(target)
                items = playlist.get('items') or []
                total = len({(str(i.get('platform')), str(i.get('song_id'))) for i in items})
                result = await adapter.create_or_update_playlist(
                    target['target_title'], playlist.get('description', ''), playlist.get('cover_url'), items,
                    fnos_guid=target.get('fnos_guid'),
                    sync_cover=False,
                    mirror=False,
                )
                _record_push_item_status(target['id'], playlist, result)
                matched = min(total, int(result.get('verified', result.get('matched', 0)) or 0))
                if matched >= total:
                    self.db.execute(
                        """update push_targets set fnos_guid=coalesce(?,fnos_guid),run_status='completed',
                           run_stage='completed',run_total=?,run_matched=?,run_missing=0,run_completed_at=?,
                           run_duration_seconds=?,run_last_checked_at=CURRENT_TIMESTAMP,
                           last_pushed_at=CURRENT_TIMESTAMP,last_error=null,updated_at=CURRENT_TIMESTAMP where id=?""",
                        (str(result.get('guid') or '').strip() or None, total, matched, _run_now(),
                         round(stalled, 3), target['id']),
                    )
                elif stalled >= self.RETRY_STALL_SECONDS:
                    # 收敛：等了足够久还是匹配不齐，落成 partial，交回人工。
                    self.db.execute(
                        """update push_targets set run_status='partial',run_stage='partial',
                           run_total=?,run_matched=?,run_missing=?,run_duration_seconds=?,
                           run_last_checked_at=CURRENT_TIMESTAMP,
                           last_error=?,updated_at=CURRENT_TIMESTAMP where id=?""",
                        (total, matched, total - matched, round(stalled, 3),
                         f'{self.RETRY_STALL_SECONDS} 秒内仅 {matched}/{total} 首匹配到飞牛曲库；'
                         f'文件已下载但未被索引识别，请检查歌曲标签或稍后重推', target['id']),
                    )
                else:
                    self.db.execute(
                        """update push_targets set run_stage='indexing',run_total=?,run_matched=?,
                           run_missing=?,run_attempts=coalesce(run_attempts,0)+1,
                           run_last_checked_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP where id=?""",
                        (total, matched, total - matched, target['id']),
                    )
                handled += 1
            except Exception as exc:
                self.db.execute(
                    """update push_targets set run_last_checked_at=CURRENT_TIMESTAMP,last_error=?,
                       updated_at=CURRENT_TIMESTAMP where id=?""",
                    (str(exc)[:300], target['id']),
                )
        return handled

    async def recompute_stale_push_snapshots(self) -> int:
        """兜底：连 stage 都不是 retry_wait、但快照里还留着 fnos_match_failed 的
        已完成/部分目标，也定期复算一次（例如 completed 之后飞牛又补索引完成、
        或人工补齐了标签）。返回复算条数。

        2026-09-29（1.0.7）新增收尸逻辑：目标已非 active（如推送中途被手动
        取消）但 run_status 仍冻结在 running 的行——process_push_runs 与本函数
        的复算查询都只捞 status='active'，这类行过去会永远显示假 running
        （Queen target=3 实测冻结 22 小时）。超过 10 分钟未动的直接落成
        cancelled，纯 DB 操作、不碰飞牛 API。"""
        from .full import _record_push_item_status, _saved_target_playlist, _stored_time
        frozen_rows = self.db.fetchall(
            """select id from push_targets
               where status<>'active' and run_status='running'
                 and coalesce(run_last_checked_at,updated_at) <= datetime('now','-10 minutes') limit 20"""
        )
        if frozen_rows:
            self.db.execute(
                """update push_targets set run_status='cancelled',run_stage='cancelled',
                   run_last_checked_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP
                   where id in (%s)""" % ','.join('?' * len(frozen_rows)),
                tuple(r['id'] for r in frozen_rows),
            )
        handled = len(frozen_rows)
        rows = self.db.fetchall(
            """select t.* from push_targets t
                where t.status='active' and coalesce(t.run_status,'') in ('completed','partial','running')
                  and exists(select 1 from push_item_status s
                              where s.target_id=t.id and s.status='fnos_match_failed')
                  and coalesce(t.run_last_checked_at,t.updated_at) <= datetime('now','-2 minutes')
                order by t.id limit 5"""
        )
        if not rows:
            return handled
        adapter = FnosAdapter(os.getenv('FNOS_API_URL'), load_fnos_token())
        for target in rows:
            try:
                playlist = _saved_target_playlist(target)
                items = playlist.get('items') or []
                total = len({(str(i.get('platform')), str(i.get('song_id'))) for i in items})
                result = await adapter.create_or_update_playlist(
                    target['target_title'], playlist.get('description', ''), playlist.get('cover_url'), items,
                    fnos_guid=target.get('fnos_guid'), sync_cover=False, mirror=False,
                )
                _record_push_item_status(target['id'], playlist, result)
                matched = min(total, int(result.get('verified', result.get('matched', 0)) or 0))
                self.db.execute(
                    """update push_targets set run_matched=?,run_missing=?,run_total=?,
                       run_status=case when ?>=? then 'completed' else run_status end,
                       run_stage=case when ?>=? then 'completed' else run_stage end,
                       run_last_checked_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP where id=?""",
                    (matched, total - matched, total, matched, total, matched, total, target['id']),
                )
                handled += 1
            except Exception:
                pass
        return handled

    async def process_push_runs(self):
        from .full import (_push_history, _push_mode, _record_push_item_status, _run_now,
                           _saved_target_playlist, _stored_time)

        # 2026-09-28：retry_wait 行交给 _recompute_retry_wait_runs 复算（见上方注释）
        try:
            await self._recompute_retry_wait_runs(
                FnosAdapter(os.getenv('FNOS_API_URL'), load_fnos_token())
            )
        except Exception:
            pass

        rows = self.db.fetchall(
            "select * from push_targets where status='active' and run_status='running' "
            "and coalesce(run_stage,'')<>'retry_wait' order by id"
        )
        completed = partial = 0
        adapter = FnosAdapter(os.getenv('FNOS_API_URL'), load_fnos_token())
        for target in rows:
            try:
                playlist = _saved_target_playlist(target)
                items = playlist.get('items') or []
                total = len({(str(item.get('platform')), str(item.get('song_id'))) for item in items})
                active = self.db.fetchone(
                    """select count(distinct t.id) count from playlist_items i
                       join tracks t on t.platform=i.platform and t.song_id=i.song_id
                       where i.playlist_id=? and exists(
                         select 1 from tasks q where q.track_id=t.id and q.status in ('pending','matching','downloading'))""",
                    (playlist['id'],),
                ) or {'count': 0}
                failed = self.db.fetchone(
                    """select count(distinct t.id) count from playlist_items i
                       join tracks t on t.platform=i.platform and t.song_id=i.song_id
                       where i.playlist_id=? and (t.file_path is null or t.file_path='') and exists(
                         select 1 from tasks q where q.track_id=t.id and q.status='failed_final')""",
                    (playlist['id'],),
                ) or {'count': 0}
                if int(active['count']) > 0:
                    self.db.execute(
                        """update push_targets set run_stage='downloading',run_total=?,run_missing=max(0,?-run_matched),
                           run_last_checked_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP where id=?""",
                        (total, total, target['id']),
                    )
                    continue

                now = datetime.now(timezone.utc)
                # 2026-09-18 事故修复：推送刚启动时（_execute_push_target 正在
                # 入队/晋升文件），调度 tick 不得并发触发全库扫描 ——
                # 今早 06:58 手动 repush 期间 tick 发出的 scan-all 与文件操作竞态，
                # 导致 8 首被飞牛误标 is_physical_file_deleted=1。等 20 秒让
                # 文件操作收尾，下一 tick 再扫。
                started_at = _stored_time(target.get('run_started_at'))
                if started_at and (now - started_at).total_seconds() < 20:
                    continue
                scan_at = _stored_time(target.get('run_scan_requested_at'))
                if scan_at is None or (now - scan_at).total_seconds() >= 30:
                    await adapter.refresh_library()
                    self.db.execute(
                        """update push_targets set run_stage='indexing',run_total=?,run_scan_requested_at=?,
                           run_last_checked_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP where id=?""",
                        (total, _run_now(), target['id']),
                    )
                    continue
                if (now - scan_at).total_seconds() < 8:
                    continue
                last_checked = _stored_time(target.get('run_last_checked_at'))
                if int(target.get('run_attempts') or 0) > 0 and last_checked and (now - last_checked).total_seconds() < 8:
                    continue

                self.db.execute(
                    "update push_targets set run_stage='repushing',run_last_checked_at=CURRENT_TIMESTAMP where id=?",
                    (target['id'],),
                )
                result = await adapter.create_or_update_playlist(
                    target['target_title'], playlist.get('description', ''), playlist.get('cover_url'), items,
                    fnos_guid=target.get('fnos_guid'),
                    sync_cover=False,
                    mirror=_push_mode(target.get('kind'), target.get('playlist_id')) == 'mirror',
                )
                _record_push_item_status(target['id'], playlist, result)
                matched = min(total, int(result.get('verified', result.get('matched', 0)) or 0))
                attempts = int(target.get('run_attempts') or 0) + 1
                started = _stored_time(target.get('run_started_at')) or now
                duration = max(0.0, (datetime.now(timezone.utc) - started).total_seconds())
                if matched >= total:
                    self.db.execute(
                        """update push_targets set fnos_guid=coalesce(?,fnos_guid),run_status='completed',
                           run_stage='completed',run_total=?,run_matched=?,run_missing=0,run_completed_at=?,
                           run_duration_seconds=?,run_attempts=?,run_last_checked_at=CURRENT_TIMESTAMP,
                           last_pushed_at=CURRENT_TIMESTAMP,last_error=null,updated_at=CURRENT_TIMESTAMP where id=?""",
                        (str(result.get('guid') or '').strip() or None, total, matched, _run_now(),
                         round(duration, 3), attempts, target['id']),
                    )
                    _push_history(target['id'], 'pipeline_complete', 'success', {
                        'total': total, 'verified': matched, 'duration_seconds': round(duration, 3),
                        'repush_attempts': attempts, 'playlist_count': result.get('playlist_count'),
                        'removed': result.get('removed', 0), 'mirror': bool(result.get('mirror')),
                    })
                    completed += 1
                elif duration >= 900 and attempts >= 12:
                    error = f"15 分钟内仅有 {matched}/{total} 首进入飞牛歌单；下载失败 {int(failed['count'])} 首"
                    self.db.execute(
                        """update push_targets set run_status='partial',run_stage='partial',run_total=?,
                           run_matched=?,run_missing=?,run_duration_seconds=?,run_attempts=?,
                           run_last_checked_at=CURRENT_TIMESTAMP,last_error=?,updated_at=CURRENT_TIMESTAMP where id=?""",
                        (total, matched, total - matched, round(duration, 3), attempts, error, target['id']),
                    )
                    _push_history(target['id'], 'pipeline_partial', 'failed', error)
                    partial += 1
                else:
                    self.db.execute(
                        """update push_targets set fnos_guid=coalesce(?,fnos_guid),run_stage='indexing',
                           run_total=?,run_matched=?,run_missing=?,run_duration_seconds=?,run_attempts=?,
                           run_last_checked_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP where id=?""",
                        (str(result.get('guid') or '').strip() or None, total, matched, total - matched,
                         round(duration, 3), attempts, target['id']),
                    )
            except Exception as exc:
                error = str(exc)[:300]
                self.db.execute(
                    "update push_targets set run_stage='retry_wait',run_last_checked_at=CURRENT_TIMESTAMP,last_error=?,updated_at=CURRENT_TIMESTAMP where id=?",
                    (error, target['id']),
                )
        return {'provider': 'push_pipeline', 'running': len(rows), 'completed': completed, 'partial': partial}

    @staticmethod
    def _rows(data, provider):
        rows = data.get('data') or data.get('songs') or data.get('recommend') or [] if isinstance(data, dict) else data
        if isinstance(rows, dict): rows = rows.get('songs') or rows.get('data') or []
        result=[]
        for row in rows if isinstance(rows,list) else []:
            if not isinstance(row,dict): continue
            sid=row.get('id') or row.get('songmid') or row.get('mid'); title=row.get('name') or row.get('title')
            artists=row.get('ar') or row.get('artists') or []
            artist=row.get('artist') or row.get('singer') or ' / '.join(x.get('name','') for x in artists if isinstance(x,dict))
            album=row.get('album') or row.get('al') or {}; album=album.get('name','') if isinstance(album,dict) else album
            if sid and title: result.append({'song_id':str(sid),'title':str(title),'artist':str(artist or ''),'album':str(album or ''),'duration_ms':row.get('duration') or row.get('dt')})
        return result

    async def run_once(self):
        results=[await self.process_push_targets()]
        for provider in ('netease','qq'):
            try: result=await self.sync_provider(provider); self.db.execute("insert or replace into sync_settings(name,enabled,interval_minutes,last_run_at,last_error,updated_at) values(?,?,?,?,?,CURRENT_TIMESTAMP)", (provider,1,self.interval,datetime.now(timezone.utc).isoformat(),None))
            except Exception as exc: result={'provider':provider,'status':'error','error':str(exc)[:300]}; self.db.execute("insert or replace into sync_settings(name,enabled,interval_minutes,last_run_at,last_error,updated_at) values(?,?,?,?,?,CURRENT_TIMESTAMP)", (provider,1,self.interval,datetime.now(timezone.utc).isoformat(),result['error']))
            results.append(result)
        if load_fnos_token():
            try:
                results.append({'provider':'fnos_favorites', **await sync_favorites(self.db, Path(os.getenv('MUSIC_ROOT','/music')))})
            except Exception as exc:
                results.append({'provider':'fnos_favorites','status':'error','error':str(exc)[:300]})
            results.append(await self.sync_subscriptions())
        return results

    async def run_forever(self):
        # Rules are checked frequently. Daily snapshots and cache maintenance
        # run once per aligned half-hour slot, beginning at 06:00 Beijing time.
        check_seconds = max(5, int(os.getenv('SUBSCRIPTION_CHECK_SECONDS', '15')))
        while True:
            try:
                await self.process_push_targets()
                if self._daily_poll_due():
                    for provider in ('netease', 'qq'):
                        try:
                            result = await self.sync_provider(provider)
                            self.db.execute(
                                "insert or replace into sync_settings(name,enabled,interval_minutes,last_run_at,last_error,updated_at) values(?,?,?,?,?,CURRENT_TIMESTAMP)",
                                (provider, 1, self.interval, datetime.now(timezone.utc).isoformat(), None),
                            )
                        except Exception as exc:
                            self.db.execute(
                                "insert or replace into sync_settings(name,enabled,interval_minutes,last_run_at,last_error,updated_at) values(?,?,?,?,?,CURRENT_TIMESTAMP)",
                                (provider, 1, self.interval, datetime.now(timezone.utc).isoformat(), str(exc)[:300]),
                            )
                    try:
                        await self.maintain_daily_cache()
                    except Exception:
                        pass
                # 「为我推荐」轮换与飞牛登录态无关，独立于 sync_subscriptions 触发。
                try:
                    await self.maybe_run_for_you()
                except Exception:
                    # 任何调度侧异常都不能打断 scheduler 循环；run_rotation 内部
                    # 已把失败写入 for_you_runs 并保留旧池。
                    pass
                if load_fnos_token():
                    await self.sync_subscriptions()
            except Exception:
                # A failed upstream request must not stop the scheduler loop.
                pass
            await asyncio.sleep(check_seconds)

    async def run_push_pipeline_forever(self):
        # 2026-09-28：每 3s 跑一次推送管线；每 ~60s 再兜底复算一次
        # 「快照里还留着 fnos_match_failed」的目标，让下载完成后飞牛补索引
        # 的歌曲能自动由 fnos_match_failed 转成 pushed，无需人工重推。
        tick = 0
        while True:
            try:
                await self.process_push_runs()
            except Exception:
                pass
            tick += 1
            if tick % 20 == 0:
                try:
                    await self.recompute_stale_push_snapshots()
                except Exception:
                    pass
            await asyncio.sleep(3)

    async def run_fnos_token_guard_forever(self):
        """周期性飞牛 token 探活 + 自愈（1.0.10 新增）。

        背景（2026-09-29 第 4 次复发定案）：
            磁盘 token 在飞牛库 `user_token` 表里的**行会被物理删除**
            （用户在飞牛音乐侧重新登录/换设备登录）——不是过期。
            `_request()` 的 401 兜底只在**真有请求**时才触发，
            空闲实例的坏 token 会一直烂在磁盘上 → 用户下次点开即「登录失败」。
            本循环补齐这个缺口。

        设计要点：
        - 默认 120s（约 2 分钟）探一次；可用 `FNOS_TOKEN_GUARD_SECONDS` 调整，设 0 关闭。
        - **先 sleep 再检查**：启动瞬间不抢资源；且实例刚启动时 token 通常刚被写入。
        - 仅在**不健康**时才写盘，健康路径零副作用（避免无谓 churn）。
        - 任何异常都不得打断循环（自愈任务本身绝不能成为新的故障源）。
        """
        try:
            period = int(os.getenv('FNOS_TOKEN_GUARD_SECONDS', '120'))
        except (TypeError, ValueError):
            period = 120
        if period <= 0:
            return  # 显式关闭
        period = max(60, period)
        while True:
            await asyncio.sleep(period)
            try:
                adapter = FnosAdapter(os.getenv('FNOS_API_URL'), os.getenv('FNOS_TOKEN'))
                result = await adapter.self_heal_token()
                status = result.get('status')
                if status == 'healed':
                    print(f'[token-guard] 🔧 自愈成功：{result.get("detail")}', flush=True)
                elif status in ('no_token', 'unrecoverable'):
                    print(f'[token-guard] ⚠️ 自愈未能恢复（{status}）：{result.get("detail")}', flush=True)
            except Exception as exc:
                print(f'[token-guard] 探活异常（已忽略，下轮重试）：{str(exc)[:200]}', flush=True)
