from __future__ import annotations

from pathlib import Path
from .metadata import has_embedded_cover
from .track_match import normalize_text


SUPPORTED = {'.mp3', '.flac', '.m4a', '.wav', '.ogg', '.opus', '.aac'}


def read_audio_metadata(path: Path) -> dict:
    title = path.stem
    folder_artist = path.parent.parent.name if path.parent.parent != path.parent else ''
    artist = folder_artist
    album = path.parent.name
    duration_ms = None
    if ' - ' in path.stem:
        left, right = (part.strip() for part in path.stem.split(' - ', 1))
        if folder_artist and normalize_text(left) == normalize_text(folder_artist):
            artist, title = left, right
        else:
            title, artist = left, right
    return {
        'title': title,
        'artist': artist,
        'album': album,
        'norm_title': normalize_text(title),
        'norm_artist': normalize_text(artist),
        'duration_ms': duration_ms,
        'artwork_status': 'embedded' if has_embedded_cover(path) else 'missing',
    }


def index_music(root: str, db) -> dict:
    base = Path(root)
    if not base.exists():
        return {'status': 'missing', 'count': 0, 'indexed': 0, 'updated': 0, 'skipped': 0, 'removed': 0}

    existing = {row['path']: row for row in db.fetchall('select path,size,mtime_ns from library_files')}
    seen: set[str] = set()
    changes: list[tuple] = []
    count = skipped = 0
    for path in base.rglob('*'):
        if not path.is_file() or path.suffix.lower() not in SUPPORTED:
            continue
        if 'fn_recommend' in path.relative_to(base).parts:
            continue
        count += 1
        path_text = str(path)
        seen.add(path_text)
        try:
            stat = path.stat()
        except OSError:
            continue
        old = existing.get(path_text)
        if old and int(old['size']) == stat.st_size and int(old['mtime_ns']) == stat.st_mtime_ns:
            skipped += 1
            continue
        meta = read_audio_metadata(path)
        changes.append((path_text, stat.st_size, stat.st_mtime_ns, meta['title'], meta['artist'], meta['album'],
                        meta['norm_title'], meta['norm_artist'], meta['duration_ms'], meta['artwork_status']))

    removed = [path for path in existing if path not in seen]
    with db.transaction() as conn:
        conn.executemany(
            '''insert into library_files(path,size,mtime_ns,title,artist,album,norm_title,norm_artist,duration_ms,artwork_status,artwork_checked_at,indexed_at)
               values(?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
               on conflict(path) do update set size=excluded.size,mtime_ns=excluded.mtime_ns,title=excluded.title,
               artist=excluded.artist,album=excluded.album,norm_title=excluded.norm_title,norm_artist=excluded.norm_artist,
               duration_ms=excluded.duration_ms,artwork_status=excluded.artwork_status,
               artwork_checked_at=CURRENT_TIMESTAMP,artwork_error=null,indexed_at=CURRENT_TIMESTAMP''',
            changes,
        )
        conn.executemany('delete from library_files where path=?', ((path,) for path in removed))
        conn.commit()
    return {'status': 'complete', 'count': count, 'indexed': len(changes), 'updated': len(changes),
            'new': len(changes), 'skipped': skipped, 'removed': len(removed)}


def find_library_match(db, track: dict) -> dict | None:
    title = normalize_text(track.get('title'))
    artist = normalize_text(track.get('artist'))
    if not title:
        return None
    rows = db.fetchall(
        'select path,title,artist,album,norm_artist,duration_ms,file_hash,artwork_status,artwork_error from library_files where norm_title=?',
        (title,),
    )
    expected_duration = int(track.get('duration_ms') or 0)
    for row in rows:
        path = Path(row['path'])
        if not path.exists():
            continue
        indexed_artist = str(row.get('norm_artist') or '')
        if artist and indexed_artist and artist not in indexed_artist and indexed_artist not in artist:
            continue
        duration = int(row.get('duration_ms') or 0)
        if expected_duration and duration and abs(expected_duration - duration) > 5000:
            continue
        if artist and not indexed_artist and not (expected_duration and duration and abs(expected_duration - duration) <= 2000):
            continue
        return row
    return None


def annotate_items(db, items: list[dict], *, attach_match: bool = False) -> list[dict]:
    """批量标注曲库状态。与 find_library_match 的匹配规则一致，但差异在于：
    - 一次性载入 library_files 索引，在内存中按 norm_title 分桶匹配（避免逐首歌查询）；
    - 不做 path.exists() 文件系统 stat —— 索引由定时任务维护，存在性以索引为准。
    歌单可能有几百首歌，逐条 stat 在 NAS 挂载盘上是主要卡顿来源，且此前的
    逐条循环全部跑在事件循环线程里，会拖慢整个应用。"""
    rows = db.fetchall(
        'select path,title,artist,album,norm_title,norm_artist,duration_ms,file_hash,artwork_status '
        'from library_files')
    by_title: dict[str, list[dict]] = {}
    for row in rows:
        key = str(row.get('norm_title') or '')
        if key:
            by_title.setdefault(key, []).append(row)
    for item in items:
        title = normalize_text(item.get('title'))
        artist = normalize_text(item.get('artist'))
        expected_duration = int(item.get('duration_ms') or 0)
        matched = None
        for row in by_title.get(title, ()):
            indexed_artist = str(row.get('norm_artist') or '')
            if artist and indexed_artist and artist not in indexed_artist and indexed_artist not in artist:
                continue
            duration = int(row.get('duration_ms') or 0)
            if expected_duration and duration and abs(expected_duration - duration) > 5000:
                continue
            if artist and not indexed_artist and not (expected_duration and duration and abs(expected_duration - duration) <= 2000):
                continue
            matched = row
            break
        item['library_status'] = 'in_library' if matched else 'missing'
        if attach_match and matched is not None:
            item['_library_file'] = matched
    return items


def upsert_library_file(db, path: Path, track: dict, file_hash: str | None = None) -> None:
    stat = path.stat()
    db.execute(
        '''insert into library_files(path,size,mtime_ns,title,artist,album,norm_title,norm_artist,duration_ms,file_hash,artwork_status,artwork_checked_at,indexed_at)
           values(?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
           on conflict(path) do update set size=excluded.size,mtime_ns=excluded.mtime_ns,title=excluded.title,
           artist=excluded.artist,album=excluded.album,norm_title=excluded.norm_title,norm_artist=excluded.norm_artist,
           duration_ms=excluded.duration_ms,file_hash=excluded.file_hash,artwork_status=excluded.artwork_status,
           artwork_checked_at=CURRENT_TIMESTAMP,artwork_error=null,indexed_at=CURRENT_TIMESTAMP''',
        (str(path), stat.st_size, stat.st_mtime_ns, track.get('title') or path.stem, track.get('artist') or '',
         track.get('album') or '', normalize_text(track.get('title') or path.stem), normalize_text(track.get('artist')),
         track.get('duration_ms'), file_hash, 'embedded' if has_embedded_cover(path) else 'missing'),
    )
