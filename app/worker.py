import asyncio
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from .db import Database
from .downloader import download, cleanup_part_files, DownloadAborted
from .artwork import resolve_artwork
from .metadata import file_sha256, has_embedded_cover, safe_filename, validate_audio, write_tags
from .models import TrackQuery, DEFAULT_QUALITY, QUALITY_ORDER as QUALITY_FALLBACK_ORDER, \
    LOSSLESS_QUALITIES, is_lossless_quality, quality_extension  # noqa: F401  (2026-09-29 上移到 models 统一维护)
from .sources import SourceRegistry
from .fnos import FnosAdapter, load_fnos_token
from .library import find_library_match, upsert_library_file
from .platforms import NeteaseConnector, QQMusicConnector
from .track_match import match_score, title_search_terms
from .daily_cache import cache_root, is_cache_path, promote_cached_track, record_cached_file

# 供 /health 读取的 worker 心跳（monotonic 时间戳）。旧实现的健康检查会逐源
# 真实探测外部接口导致容器误重启；现在 /health 只看数据库与这个心跳。
WORKER: dict = {'heartbeat': 0.0}

# 单例引用：任务 pause/cancel 端点经 get_worker() 拿到实例后设置协作式事件
TASK_WORKER: 'TaskWorker | None' = None


def get_worker():
    return TASK_WORKER


def alternate_search_enabled() -> bool:
    """跨平台同曲兜底开关（社区版默认关闭）。

    直连解析全部失败后搜索其他平台同名曲目再走一遍用户音源。该机制会把
    「拿不到的歌」替换成同名/翻唱版本，社区用户对错歌容忍度低，默认关闭；
    compose 里 ALTERNATE_SEARCH=true 可恢复。
    """
    return str(os.getenv('ALTERNATE_SEARCH', 'false')).strip().lower() in ('1', 'true', 'yes', 'on')


class TaskWorker:
    def __init__(self, db: Database, sources: SourceRegistry, music_root='/music'):
        self.db, self.sources, self.music_root = db, sources, Path(music_root)
        self.last_library_refresh = 0.0
        try:
            concurrency = int(os.getenv('DOWNLOAD_CONCURRENCY', '3'))
        except ValueError:
            concurrency = 3
        self.concurrency = max(1, min(6, concurrency))
        # 任务级协作式取消事件：端点 set，worker 在解析/下载检查点响应
        self._pause_events: dict[int, asyncio.Event] = {}
        self._cancel_events: dict[int, asyncio.Event] = {}
        WORKER['concurrency'] = self.concurrency

    def _target(self, track: dict, task: dict, extension: str) -> Path:
        if task['task_type'] == 'daily':
            folder = cache_root(self.music_root) / datetime.now(timezone(timedelta(hours=8))).date().isoformat()
        else:
            folder = self.music_root / 'Library' / safe_filename(track.get('artist','未知歌手')) / safe_filename(track.get('album','未知专辑'))
        return folder / f"{safe_filename(track['title'])} - {safe_filename(track.get('artist',''))}{extension}"

    async def _refresh_library(self):
        token = load_fnos_token()
        if not token or time.monotonic() - self.last_library_refresh < 15:
            return
        try:
            await FnosAdapter(os.getenv('FNOS_API_URL'), token).refresh_library()
            self.last_library_refresh = time.monotonic()
        except Exception:
            pass

    async def _write_and_verify_artwork(self, track: dict, path: Path, alternate: dict | None = None) -> dict:
        try:
            artwork = await resolve_artwork(self.db, track, alternate)
            # mutagen 标签写入与封面回读是同步重活，放线程池避免阻塞事件循环。
            tagged = await asyncio.to_thread(
                write_tags,
                str(path), title=track['title'], artist=track.get('artist', ''), album=track.get('album', ''),
                cover=artwork['data'], cover_mime=artwork['mime'],
            )
            if not tagged['tags_written'] or not tagged['cover_written'] or not await asyncio.to_thread(has_embedded_cover, path):
                raise RuntimeError(tagged.get('error') or 'cover_verification_failed')
            self.db.execute(
                "update tracks set cover_url=?,cover_status='embedded',cover_error=null,updated_at=CURRENT_TIMESTAMP where id=?",
                (artwork['url'], track['id']),
            )
            return artwork
        except Exception as exc:
            self.db.execute(
                "update tracks set cover_status='failed',cover_error=?,updated_at=CURRENT_TIMESTAMP where id=?",
                (str(exc)[:300], track['id']),
            )
            raise RuntimeError(f'artwork_failed: {str(exc)[:240]}') from exc

    async def _complete_download(self, track: dict, task: dict, target: Path, size: int,
                                 source_name: str, alternate: dict | None = None) -> None:
        # 时长校验（mutagen 读盘）与全文件 sha256 是同步重活，放线程池执行，
        # 避免推大歌单时整个事件循环被逐曲卡住。
        validation = await asyncio.to_thread(validate_audio, str(target), track.get('duration_ms'))
        if not validation['valid']:
            target.unlink(missing_ok=True)
            raise RuntimeError('invalid_audio_or_trial_snippet')
        try:
            artwork = await self._write_and_verify_artwork(track, target, alternate)
        except Exception:
            target.unlink(missing_ok=True)
            raise
        digest = await asyncio.to_thread(file_sha256, target)
        duplicate = self.db.fetchone(
            "select id,file_path from tracks where file_hash=? and id<>? and file_path is not null limit 1",
            (digest, track['id']),
        )
        if duplicate and duplicate.get('file_path') and await asyncio.to_thread(Path(duplicate['file_path']).exists):
            target.unlink(missing_ok=True)
            final_path = str(duplicate['file_path'])
        else:
            final_path = str(target)
            upsert_library_file(self.db, target, track, digest)
            self.db.execute(
                "update library_files set artwork_status='embedded',artwork_checked_at=CURRENT_TIMESTAMP,artwork_error=null where path=?",
                (final_path,),
            )
        self.db.execute(
            "update tracks set file_path=?,file_hash=?,status='archived',cover_status='embedded',cover_error=null,updated_at=CURRENT_TIMESTAMP where id=?",
            (final_path, digest, track['id']),
        )
        if task['task_type'] == 'daily' and is_cache_path(Path(final_path), self.music_root):
            record_cached_file(self.db, track['id'], Path(final_path))
        self.db.execute("update tasks set status='archived',error=null,finished_at=CURRENT_TIMESTAMP,"
                        "progress_bytes=total_bytes,speed_bytes=0,lease_owner=NULL,lease_until=NULL,"
                        "updated_at=CURRENT_TIMESTAMP where id=?", (task['id'],))
        self.db.execute(
            "insert into task_attempts(task_id,source_name,status,detail) values(?,?,?,?)",
            (task['id'], source_name, 'success', f"{size}; cover={artwork['source']}; sha256={digest[:12]}"),
        )
        await self._refresh_library()

    async def _reuse_library_file(self, track: dict, task: dict, path: Path, file_hash: str | None) -> None:
        if task['task_type'] != 'daily' and is_cache_path(path, self.music_root):
            promoted = promote_cached_track(self.db, track, self.music_root, f"task:{task['task_type']}")
            if promoted:
                path = promoted
                file_hash = file_sha256(path)
        if not has_embedded_cover(path):
            await self._write_and_verify_artwork(track, path)
            file_hash = file_sha256(path)
            upsert_library_file(self.db, path, track, file_hash)
        self.db.execute(
            "update library_files set artwork_status='embedded',artwork_checked_at=CURRENT_TIMESTAMP,artwork_error=null where path=?",
            (str(path),),
        )
        self.db.execute(
            "update tracks set file_path=?,file_hash=?,status='archived',cover_status='embedded',cover_error=null,updated_at=CURRENT_TIMESTAMP where id=?",
            (str(path), file_hash, track['id']),
        )
        self.db.execute("update tasks set status='archived',source_name='曲库去重',error=null,finished_at=CURRENT_TIMESTAMP,"
                        "progress_bytes=total_bytes,speed_bytes=0,lease_owner=NULL,lease_until=NULL,"
                        "updated_at=CURRENT_TIMESTAMP where id=?", (task['id'],))
        self.db.execute(
            "insert into task_attempts(task_id,source_name,status,detail) values(?,?,?,?)",
            (task['id'], '曲库去重', 'success', f'reused_with_cover:{path}'),
        )

    async def _alternate_tracks(self, track: dict) -> list[dict]:
        original = track.get('platform')
        # QQ keeps more original-catalog results for fnOS favorites; try it
        # first before cover/remix-heavy Netease results.
        platforms = (['qq', 'netease'] if original not in {'netease', 'qq'}
                     else ['qq' if original == 'netease' else 'netease'])
        titles = title_search_terms(track.get('title'))
        artist = str(track.get('artist') or '').strip()
        queries = [f"{title} {artist}".strip() for title in titles]
        queries.extend(titles)
        candidates: list[dict] = []
        seen: set[tuple[str, str]] = set()
        for platform in platforms:
            account = self.db.fetchone('select cookie from accounts where provider=?', (platform,)) or {}
            connector = (NeteaseConnector(account.get('cookie')) if platform == 'netease'
                         else QQMusicConnector(account.get('cookie')))
            for query in dict.fromkeys(value for value in queries if value):
                try:
                    rows = await connector.search(query, 30) if platform == 'netease' else await connector.search(query, 1, 30)
                except Exception:
                    continue
                for row in rows:
                    key = (str(row.get('platform') or platform), str(row.get('song_id') or ''))
                    if key[1] and key not in seen:
                        seen.add(key)
                        candidates.append(row)
        ranked = [(match_score(track, candidate), index, candidate)
                  for index, candidate in enumerate(candidates)]
        ranked = [item for item in ranked if item[0] >= 0]
        ranked.sort(key=lambda item: (-item[0], item[1]))
        return [item[2] for item in ranked[:6]]

    async def _alternate_track(self, track: dict) -> dict | None:
        rows = await self._alternate_tracks(track)
        return rows[0] if rows else None

    async def _download_alternate(self, track: dict, task: dict) -> tuple[str, int] | None:
        # 社区版默认关闭跨平台同曲兜底（见 alternate_search_enabled）
        if not alternate_search_enabled():
            return None
        alternates = await self._alternate_tracks(track)
        if not alternates:
            return None
        quality = track.get('quality') or DEFAULT_QUALITY
        for alternate in alternates:
            query = TrackQuery(platform=alternate['platform'], song_id=alternate['song_id'], quality=quality)
            attempts: list[tuple[str, object]] = []
            # Use the registry's health-aware order so a known-good source is
            # tried before a source that recently timed out or lost auth.
            for name in self.sources.ordered_names(only_enabled=True):
                attempts.append((str(name), lambda name=name, q=query: self.sources.resolve(name, q)))
            for source_name, resolver in attempts:
                state = self._user_state(task['id'])
                if state:
                    await self._handle_user_state(task, state)
                    return None
                try:
                    label = f"{source_name} ({'网易云' if alternate['platform'] == 'netease' else 'QQ'}同曲兜底)"
                    if not self._mark(task['id'], 'downloading'):
                        return None
                    self.db.execute("UPDATE tasks SET source_name=?,attempts=attempts+1 WHERE id=?", (label, task['id']))
                    url = await resolver()
                    extension = quality_extension(quality)
                    target = self._target(track, task, extension)
                    size, _ = await download(url, target, source_name,
                                             should_abort=lambda: self._user_state(task['id']) is not None,
                                             on_progress=self._progress_writer(task))
                    await self._complete_download(track, task, target, size, label, alternate)
                    return label, size
                except DownloadAborted:
                    await self._handle_user_state(task, self._user_state(task['id']) or 'cancelled', target=target)
                    return None
                except Exception as exc:
                    detail = str(exc)[:500]
                    self.db.execute("INSERT INTO task_attempts(task_id,source_name,status,detail) VALUES(?,?,?,?)", (task['id'], source_name, 'failed', f"alternate:{alternate.get('platform')}:{alternate.get('song_id')}:{detail}"))
        return None

    # ------------------------------------------------------------ 认领与状态
    def _claim_next(self, owner: str) -> dict | None:
        """原子认领：候选先查再条件更新（WHERE status='pending'），并发槽不会重复领到同一任务；
        认领即打 15 分钟租约并重置进度。"""
        candidate = self.db.fetchone(
            "SELECT id FROM tasks WHERE status='pending' AND (next_run_at IS NULL OR next_run_at<=CURRENT_TIMESTAMP) "
            "ORDER BY priority,id LIMIT 1")
        if not candidate:
            return None
        claimed = self.db.execute_rc(
            "UPDATE tasks SET status='matching',lease_owner=?,lease_until=datetime('now','+15 minutes'),"
            "started_at=coalesce(started_at,CURRENT_TIMESTAMP),progress_bytes=0,total_bytes=0,speed_bytes=0,"
            "updated_at=CURRENT_TIMESTAMP WHERE id=? AND status='pending'",
            (owner, candidate['id']))
        if not claimed:
            return None
        return self.db.fetchone('SELECT * FROM tasks WHERE id=?', (candidate['id'],))

    def _mark(self, task_id: int, status: str) -> bool:
        """过渡状态写回（matching/downloading）。paused/cancelled 是用户终态，
        不得被 worker 的过渡写回覆盖；写不进去说明用户已介入，调用方应立即收尾。"""
        return self.db.execute_rc(
            "UPDATE tasks SET status=?,updated_at=CURRENT_TIMESTAMP "
            "WHERE id=? AND status NOT IN ('paused','cancelled')", (status, task_id)) > 0

    def _user_state(self, task_id: int) -> str | None:
        """用户意图：None=无 / 'paused' / 'cancelled'。"""
        cancel = self._cancel_events.get(task_id)
        if cancel is not None and cancel.is_set():
            return 'cancelled'
        pause = self._pause_events.get(task_id)
        if pause is not None and pause.is_set():
            return 'paused'
        return None

    def request_pause(self, task_id: int) -> None:
        self._pause_events.setdefault(task_id, asyncio.Event()).set()

    def request_cancel(self, task_id: int) -> None:
        self._cancel_events.setdefault(task_id, asyncio.Event()).set()

    def clear_user_state(self, task_id: int) -> None:
        self._pause_events.pop(task_id, None)
        self._cancel_events.pop(task_id, None)

    async def _handle_user_state(self, task: dict, state: str, target: Path | None = None) -> None:
        """在检查点响应暂停/取消：取消清 .part，暂停保留 .part（断点续传）。"""
        if state == 'cancelled':
            if target is not None:
                cleanup_part_files(target)
            self.db.execute(
                "UPDATE tasks SET status='cancelled',error='cancelled_by_user',finished_at=CURRENT_TIMESTAMP,"
                "lease_owner=NULL,lease_until=NULL,updated_at=CURRENT_TIMESTAMP WHERE id=?", (task['id'],))
        else:
            self.db.execute(
                "UPDATE tasks SET status='paused',lease_owner=NULL,lease_until=NULL,"
                "updated_at=CURRENT_TIMESTAMP WHERE id=?", (task['id'],))
        self.clear_user_state(task['id'])

    def _progress_writer(self, task: dict):
        """download() 的进度回调：≥2s 节流写库，顺带续租并记录速度。"""
        state = {'last': 0.0, 'bytes': 0, 'time': 0.0}

        def on_progress(done: int, total: int | None) -> None:
            state['bytes'] = done
            now = time.monotonic()
            if now - state['last'] < 2.0:
                return
            delta_bytes = done - state['bytes']
            delta_time = now - state['time'] if state['time'] else 0.0
            speed = int(delta_bytes / delta_time) if delta_time > 0.5 else 0
            state['last'], state['time'] = now, now
            try:
                self.db.execute(
                    "UPDATE tasks SET progress_bytes=?,total_bytes=coalesce(?,total_bytes),speed_bytes=?,"
                    "lease_until=datetime('now','+15 minutes'),updated_at=CURRENT_TIMESTAMP WHERE id=?",
                    (done, total, speed, task['id']))
            except Exception:
                pass
        return on_progress

    def recover_inflight(self):
        """容器重启/worker 重启后回收：matching/downloading → pending，清租约。
        paused/cancelled 是用户终态，重启后原样保留。"""
        rows = self.db.fetchall("SELECT id FROM tasks WHERE status IN ('matching','downloading')")
        for row in rows:
            self.db.execute("UPDATE tasks SET status='pending', error=?, lease_owner=NULL, lease_until=NULL WHERE id=?",
                            ('interrupted_by_restart', row['id']))
            self.db.execute("INSERT INTO task_attempts(task_id,source_name,status,detail) VALUES(?,?,?,?)",
                            (row['id'], 'system', 'interrupted', 'worker restart'))
        return len(rows)
    async def process_task(self, task: dict) -> None:
        """处理单个已认领任务；暂停/取消经协作事件在检查点生效。"""
        track = self.db.fetchone('SELECT * FROM tracks WHERE id=?', (task['track_id'],))
        if not track:
            self.db.execute("UPDATE tasks SET status='cancelled',error='track_missing',finished_at=CURRENT_TIMESTAMP,"
                            "lease_owner=NULL,lease_until=NULL,updated_at=CURRENT_TIMESTAMP WHERE id=?", (task['id'],))
            return
        if track.get('file_path') and Path(track['file_path']).exists():
            try:
                await self._reuse_library_file(track, task, Path(track['file_path']), track.get('file_hash'))
                return
            except Exception as exc:
                self.db.execute("update tasks set error=? where id=?", (str(exc)[:300], task['id']))
        library_match = find_library_match(self.db, track)
        if library_match:
            try:
                await self._reuse_library_file(track, task, Path(library_match['path']), library_match.get('file_hash'))
                return
            except Exception as exc:
                failure = str(exc)[:300]
                self.db.execute("insert into task_attempts(task_id,source_name,status,detail) values(?,?,?,?)",
                                (task['id'], '曲库封面补全', 'failed', failure))
        if not self._mark(task['id'], 'matching'):
            return
        failure_details: list[str] = []
        # Keep the same health-aware ordering as preview/playback resolution.
        # A single track failure is recorded below and does not reorder a source;
        # only SourceRegistry classifies source-level failures as unhealthy.
        # fnos 等非平台来源没有真实歌曲 ID，逐源直连解析必然失败，跳过。
        direct_resolvable = track['platform'] in ('netease', 'qq')
        if not direct_resolvable:
            # fnos 收藏的歌曲没有平台歌曲 ID（song_id 是飞牛 GUID），直连解析必然
            # 失败还消耗重试配额；跳过直连，仅曲库去重/（可选）跨平台同名搜索可用。
            self.db.execute(
                "INSERT INTO task_attempts(task_id,source_name,status,detail) VALUES(?,?,?,?)",
                (task['id'], 'system', 'skipped', f"skip_direct:platform={track['platform']}:song_id_is_not_platform_id"),
            )
        else:
            for name in self.sources.ordered_names(only_enabled=True):
                state = self._user_state(task['id'])
                if state:
                    await self._handle_user_state(task, state)
                    return
                try:
                    if not self._mark(task['id'], 'downloading'):
                        return
                    self.db.execute("UPDATE tasks SET source_name=?,attempts=attempts+1 WHERE id=?", (name, task['id']))
                    url = await self.sources.resolve(name, TrackQuery(platform=track['platform'], song_id=track['song_id'], quality=(track.get('quality') or DEFAULT_QUALITY)))
                    extension = quality_extension(track.get('quality') or DEFAULT_QUALITY)
                    target = self._target(track, task, extension)
                    size, _ = await download(url, target, name,
                                             should_abort=lambda: self._user_state(task['id']) is not None,
                                             on_progress=self._progress_writer(task))
                    await self._complete_download(track, task, target, size, name)
                    return
                except DownloadAborted:
                    # DownloadAborted 只会从 download() 抛出，target 必已赋值
                    await self._handle_user_state(task, self._user_state(task['id']) or 'cancelled', target=target)
                    return
                except Exception as exc:
                    detail = str(exc)[:500]
                    failure_details.append(f"{name}: {detail}")
                    self.db.execute("INSERT INTO task_attempts(task_id,source_name,status,detail) VALUES(?,?,?,?)", (task['id'],name,'failed',detail))
        # 社区版零内置音源：解析只走用户导入的音源，没有内置兜底。
        alternate_result = await self._download_alternate(track, task)
        if alternate_result:
            return
        current = self.db.fetchone('select attempts from tasks where id=?', (task['id'],)) or {'attempts': 0}
        enabled_sources = self.db.fetchall('select name from sources where enabled=1')
        source_count = max(1, len(enabled_sources))
        if not enabled_sources:
            # 零内置音源：没有用户音源时任何路径都不可能成功，直接终态，不再空转重试
            if direct_resolvable:
                reason = '尚未配置可用音源：请在「音源管理」导入音源 JS 或添加音源服务地址'
            else:
                reason = '尚未配置可用音源，且该任务无平台歌曲 ID（跨平台兜底未开启，ALTERNATE_SEARCH=true 可开）'
            self.db.execute("UPDATE tasks SET status='failed_final',error=?,next_run_at=null,finished_at=CURRENT_TIMESTAMP,"
                            "lease_owner=NULL,lease_until=NULL,updated_at=CURRENT_TIMESTAMP WHERE id=?", (reason, task['id']))
            self.clear_user_state(task['id'])
            return
        if not direct_resolvable and not failure_details:
            # 无平台歌曲 ID（如飞牛收藏）且跨平台兜底未开启：重试不会变好，直接终态，
            # 避免每分钟空转一次（用户导入音源/开启 ALTERNATE_SEARCH 后可手动重试）
            reason = '无可解析路径：该任务无平台歌曲 ID 且跨平台兜底未开启（ALTERNATE_SEARCH=true 可开）'
            self.db.execute("UPDATE tasks SET status='failed_final',error=?,next_run_at=null,finished_at=CURRENT_TIMESTAMP,"
                            "lease_owner=NULL,lease_until=NULL,updated_at=CURRENT_TIMESTAMP WHERE id=?", (reason, task['id']))
            self.clear_user_state(task['id'])
            return
        state = self._user_state(task['id'])
        if state:
            await self._handle_user_state(task, state)
            return
        if failure_details:
            reason = 'all_sources_failed: ' + ' | '.join(failure_details[-3:])
        elif direct_resolvable:
            reason = 'all_sources_failed'
        else:
            reason = '无可解析路径：该任务无平台歌曲 ID 且跨平台兜底未开启（ALTERNATE_SEARCH=true 可开）'
        if int(current['attempts']) < max(3, source_count * 3):
            delay = min(600, 60 * (2 ** max(0, int(current['attempts']) // max(1, source_count) - 1)))
            self.db.execute("UPDATE tasks SET status='pending',error=?,next_run_at=datetime('now',?),lease_owner=NULL,"
                            "lease_until=NULL,updated_at=CURRENT_TIMESTAMP WHERE id=?", (reason, f'+{delay} seconds', task['id']))
        else:
            self.db.execute("UPDATE tasks SET status='failed_final',error=?,next_run_at=null,finished_at=CURRENT_TIMESTAMP,"
                            "lease_owner=NULL,lease_until=NULL,updated_at=CURRENT_TIMESTAMP WHERE id=?", (reason, task['id']))
        self.clear_user_state(task['id'])
    async def _slot(self, index: int):
        """并发下载槽：原子认领 → 处理 → 循环。单任务逃逸异常只影响该任务，不拖垮循环。"""
        owner = f'slot{index}'
        while True:
            task = None
            try:
                task = self._claim_next(owner)
                if not task:
                    await asyncio.sleep(1)
                    continue
                await self.process_task(task)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                WORKER['last_error'] = str(exc)[:200]
                try:
                    if task is not None:
                        self.db.execute(
                            "UPDATE tasks SET status='pending',error=?,lease_owner=NULL,lease_until=NULL,"
                            "updated_at=CURRENT_TIMESTAMP WHERE id=? AND status IN ('matching','downloading')",
                            (f'slot_escape:{str(exc)[:200]}', task['id']))
                except Exception:
                    pass
            finally:
                WORKER['heartbeat'] = time.monotonic()

    async def _zombie_reaper(self) -> int:
        """租约过期收尸：worker 崩溃/断电后 matching/downloading 残骸回 pending（续租正常的不受影响）。"""
        rows = self.db.fetchall(
            "SELECT id FROM tasks WHERE status IN ('matching','downloading') "
            "AND lease_until IS NOT NULL AND lease_until < CURRENT_TIMESTAMP")
        for row in rows:
            self.db.execute("UPDATE tasks SET status='pending',lease_owner=NULL,lease_until=NULL,"
                            "updated_at=CURRENT_TIMESTAMP WHERE id=?", (row['id'],))
            self.db.execute("INSERT INTO task_attempts(task_id,source_name,status,detail) VALUES(?,?,?,?)",
                            (row['id'], 'system', 'lease_expired', 'zombie reaper'))
        return len(rows)

    async def run_forever(self):
        WORKER['concurrency'] = self.concurrency
        slots = [asyncio.create_task(self._slot(i)) for i in range(self.concurrency)]
        ticks = 0
        try:
            while True:
                WORKER['heartbeat'] = time.monotonic()
                ticks += 1
                if ticks >= 60:
                    ticks = 0
                    try:
                        await self._zombie_reaper()
                    except Exception:
                        pass
                await asyncio.sleep(1)
        finally:
            for slot in slots:
                slot.cancel()
            await asyncio.gather(*slots, return_exceptions=True)
