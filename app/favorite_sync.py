from __future__ import annotations

import os
from pathlib import Path

from .daily_cache import is_cache_path, promote_cached_track
from .fnos import FnosAdapter
from .models import DEFAULT_QUALITY  # 2026-09-29：入库显式带音质，不依赖（老库仍是320k的）表默认值


async def sync_favorites(db, music_root: Path) -> dict:
    adapter = FnosAdapter(os.getenv('FNOS_API_URL'), os.getenv('FNOS_TOKEN'))
    data = await adapter.favorites()
    rows = data.get('songs') if isinstance(data, dict) else data
    rows = rows if isinstance(rows, list) else []
    created = promoted = 0
    for item in rows:
        if not isinstance(item, dict):
            continue
        platform = str(item.get('platform') or item.get('source') or 'fnos')
        song_id = item.get('song_id') or item.get('id') or item.get('mid') or item.get('guid')
        title = item.get('title') or item.get('name')
        if not song_id or not title:
            continue
        artist_value = item.get('artist') or item.get('singer') or item.get('artists') or ''
        if isinstance(artist_value, list):
            artist_value = ' / '.join(str(x.get('name') or '') for x in artist_value if isinstance(x, dict))
        artist_value = str(artist_value)
        candidates = db.fetchall('select * from tracks where lower(title)=lower(?) and file_path is not null', (str(title),))
        local = next((x for x in candidates if not artist_value or artist_value.lower() in str(x.get('artist') or '').lower() or str(x.get('artist') or '').lower() in artist_value.lower()), None)
        if local and local.get('file_path') and is_cache_path(Path(local['file_path']), music_root):
            source_path = Path(local['file_path'])
            if source_path.exists():
                if promote_cached_track(db, local, music_root, 'fnos_favorite'):
                    promoted += 1
            continue
        cover_url = item.get('cover_url') or item.get('cover') or item.get('picUrl')
        db.execute('insert or ignore into tracks(platform,song_id,title,artist,album,duration_ms,quality,cover_url) values(?,?,?,?,?,?,?,?)',
                   (platform, str(song_id), str(title), artist_value, str(item.get('album') or ''), item.get('duration_ms') or item.get('duration'), DEFAULT_QUALITY, cover_url))
        track = db.fetchone('select id,file_path from tracks where platform=? and song_id=?', (platform, str(song_id)))
        if track and not track.get('file_path') and not db.fetchone("select id from tasks where track_id=? and task_type='favorite' and status in ('pending','matching','downloading','archived','failed_final')", (track['id'],)):
            db.execute("insert into tasks(track_id,task_type,priority) values(?, 'favorite', 2)", (track['id'],))
            created += 1
    return {'status':'queued', 'received':len(rows), 'promoted':promoted, 'tasks_created':created}
