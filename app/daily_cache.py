from __future__ import annotations

import os
import shutil
import hashlib
import json
from pathlib import Path

from .library import upsert_library_file
from .metadata import file_sha256, safe_filename


def cache_root(music_root: Path) -> Path:
    configured = os.getenv('DAILY_CACHE_ROOT', '').strip()
    return Path(configured) if configured else music_root / 'fn_recommend_cache'


def cache_days() -> int:
    try:
        return max(1, int(os.getenv('DAILY_CACHE_DAYS', '3')))
    except ValueError:
        return 3


def repeat_play_threshold() -> int:
    try:
        return max(1, int(os.getenv('DAILY_REPEAT_PLAYS', '2')))
    except ValueError:
        return 2


def is_cache_path(path: Path, music_root: Path) -> bool:
    try:
        path.resolve().relative_to(cache_root(music_root).resolve())
        return True
    except (OSError, ValueError):
        return False


def record_cached_file(db, track_id: int, path: Path) -> None:
    expires_at = f"datetime('now','+{cache_days()} days')"
    db.execute(
        f'''insert into daily_cache(track_id,path,cached_at,expires_at,status,removed_at,reason,play_count,last_played_at,promoted_at)
           values(?,?,CURRENT_TIMESTAMP,{expires_at},'active',null,null,0,null,null)
           on conflict(track_id) do update set path=excluded.path,cached_at=CURRENT_TIMESTAMP,
             expires_at={expires_at},status='active',removed_at=null,reason=null,
             play_count=0,last_played_at=null,promoted_at=null''',
        (track_id, str(path)),
    )


def promote_cached_track(db, track: dict, music_root: Path, reason: str) -> Path | None:
    source_value = str(track.get('file_path') or '').strip()
    if not source_value:
        return None
    source = Path(source_value)
    if not source.exists() or not is_cache_path(source, music_root):
        return source if source.exists() else None
    target = (music_root / 'Library' / safe_filename(track.get('artist') or '未知歌手') /
              safe_filename(track.get('album') or '未知专辑') / source.name)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        source.unlink(missing_ok=True)
    else:
        shutil.move(str(source), str(target))
    digest = file_sha256(target)
    upsert_library_file(db, target, track, digest)
    db.execute('delete from library_files where path=? and path<>?', (source_value, str(target)))
    db.execute(
        "update tracks set file_path=?,file_hash=?,status='archived',updated_at=CURRENT_TIMESTAMP where id=?",
        (str(target), digest, track['id']),
    )
    db.execute(
        "update daily_cache set path=?,status='promoted',promoted_at=CURRENT_TIMESTAMP,reason=? where track_id=?",
        (str(target), reason[:100], track['id']),
    )
    _remove_empty_parents(source.parent, cache_root(music_root))
    return target


def record_play(db, music_root: Path, platform: str, song_id: str) -> dict:
    track = db.fetchone('select * from tracks where platform=? and song_id=?', (platform, song_id))
    if not track:
        return {'tracked': False, 'promoted': False}
    cached = db.fetchone("select * from daily_cache where track_id=? and status='active'", (track['id'],))
    if not cached:
        return {'tracked': False, 'promoted': False}
    db.execute(
        'update daily_cache set play_count=play_count+1,last_played_at=CURRENT_TIMESTAMP where track_id=?',
        (track['id'],),
    )
    cached = db.fetchone('select * from daily_cache where track_id=?', (track['id'],))
    # 播放计数仅用于统计展示，不再触发「晋级保留」——
    # 保留与否只看收藏（见 promote_signalled_tracks 的规则说明）。
    return {'tracked': True, 'promoted': False, 'play_count': int(cached.get('play_count') or 0)}


def record_fnos_history(db, music_root: Path, rows: list[dict]) -> dict:
    recorded = promoted = 0
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        item = raw.get('track') if isinstance(raw.get('track'), dict) else raw
        title = str(item.get('title') or item.get('name') or '').strip()
        if not title:
            continue
        artist_value = item.get('artist') or item.get('singer') or item.get('artists') or ''
        if isinstance(artist_value, list):
            artist_value = ' / '.join(str(x.get('name') or x) for x in artist_value if x)
        candidates = db.fetchall(
            '''select t.* from daily_cache c join tracks t on t.id=c.track_id
               where c.status='active' and lower(t.title)=lower(?)''',
            (title,),
        )
        artist = str(artist_value).lower().strip()
        track = next((candidate for candidate in candidates
                      if not artist or artist in str(candidate.get('artist') or '').lower()
                      or str(candidate.get('artist') or '').lower() in artist), None)
        if not track:
            continue
        played_at = raw.get('playedAt') or raw.get('playTime') or raw.get('createdAt') or raw.get('createTime')
        identity = raw.get('id') or raw.get('guid') or raw.get('historyGUID')
        event_key = hashlib.sha256(json.dumps(
            {'id': identity, 'played_at': played_at, 'title': title, 'artist': artist_value},
            ensure_ascii=False, sort_keys=True,
        ).encode('utf-8')).hexdigest()
        with db.transaction() as conn:
            exists = conn.execute('select 1 from daily_cache_plays where event_key=?', (event_key,)).fetchone()
            if exists:
                continue
            conn.execute(
                'insert into daily_cache_plays(event_key,track_id,played_at) values(?,?,?)',
                (event_key, track['id'], str(played_at or '')),
            )
            conn.execute(
                'update daily_cache set play_count=play_count+1,last_played_at=CURRENT_TIMESTAMP where track_id=?',
                (track['id'],),
            )
        recorded += 1
        # 与 record_play 同理：历史播放仅计数，不触发晋级保留。
    return {'recorded': recorded, 'promoted': promoted}


def promote_signalled_tracks(db, music_root: Path) -> dict:
    """把「已被用户收藏」的缓存曲目移入正式曲库。

    规则（2026-09-24 用户指定）：**只有收藏才免删**。
    播放次数不再作为保留依据 —— 曾用过 ``play_count>=N``，但实测该计数只统计
    fnmusic-flow 网页内嵌播放器的播放，用户在飞牛音乐 App 里的收听完全统计不到，
    致其长期为 0、规则形同虚设。现改为仅凭收藏判定，语义更明确。

    收藏来源两条：
      1. 飞牛音乐收藏 —— 经 ``favorite_sync.sync_favorites`` 同步；
      2. 本地 ``tasks`` 表中 ``task_type='favorite'`` 的任务记录。
    """
    rows = db.fetchall(
        '''select t.* from daily_cache c join tracks t on t.id=c.track_id
           where c.status='active' and exists(
             select 1 from tasks q where q.track_id=t.id and q.task_type='favorite'
           )'''
    )
    promoted = 0
    for track in rows:
        if promote_cached_track(db, track, music_root, 'favorite_only'):
            promoted += 1
    return {'promoted': promoted}


def cleanup_expired(db, music_root: Path) -> dict:
    promoted = promote_signalled_tracks(db, music_root)['promoted']
    rows = db.fetchall(
        '''select c.*,t.file_path from daily_cache c join tracks t on t.id=c.track_id
           where c.status='active' and datetime(c.expires_at)<=CURRENT_TIMESTAMP'''
    )
    removed = 0
    for row in rows:
        path = Path(str(row.get('path') or row.get('file_path') or ''))
        if path.exists() and not is_cache_path(path, music_root):
            db.execute(
                "update daily_cache set status='promoted',promoted_at=CURRENT_TIMESTAMP,reason='already_in_formal_library' where track_id=?",
                (row['track_id'],),
            )
            promoted += 1
            continue
        if path.exists():
            path.unlink(missing_ok=True)
            _remove_empty_parents(path.parent, cache_root(music_root))
        db.execute('delete from library_files where path=?', (str(path),))
        db.execute(
            "update tracks set file_path=null,file_hash=null,status='pending',updated_at=CURRENT_TIMESTAMP where id=?",
            (row['track_id'],),
        )
        db.execute(
            "update daily_cache set path=null,status='removed',removed_at=CURRENT_TIMESTAMP,reason='expired_unengaged' where track_id=?",
            (row['track_id'],),
        )
        removed += 1
    return {'promoted': promoted, 'removed': removed, 'expired_checked': len(rows)}


def summary(db) -> dict:
    counts = {row['status']: int(row['count']) for row in db.fetchall(
        'select status,count(*) count from daily_cache group by status'
    )}
    active = db.fetchone(
        "select count(*) count,coalesce(sum(play_count),0) plays,min(expires_at) next_expiry from daily_cache where status='active'"
    ) or {}
    return {'active': int(active.get('count') or 0), 'plays': int(active.get('plays') or 0),
            'next_expiry': active.get('next_expiry'), 'promoted': counts.get('promoted', 0),
            'removed': counts.get('removed', 0)}


def _remove_empty_parents(folder: Path, stop: Path) -> None:
    try:
        stop = stop.resolve()
        current = folder.resolve()
        while current != stop and stop in current.parents:
            current.rmdir()
            current = current.parent
    except OSError:
        pass
