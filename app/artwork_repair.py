from __future__ import annotations

import asyncio
import os
from pathlib import Path

from .artwork import resolve_artwork
from .fnos import FnosAdapter, load_fnos_token
from .metadata import file_sha256, has_embedded_cover, write_tags


async def run_artwork_repair(db, job_id: int, limit: int | None = None, managed_only: bool = True,
                             only_status: str | None = None) -> None:
    conditions = []
    params = []
    if managed_only:
        conditions.append("exists(select 1 from tracks t where t.file_path=library_files.path)")
    if only_status:
        conditions.append('artwork_status=?')
        params.append(only_status)
    scope = (' where ' + ' and '.join(conditions)) if conditions else ''
    rows = db.fetchall(
        'select * from library_files' + scope
        + ' order by case artwork_status when \'missing\' then 0 when \'unknown\' then 1 else 2 end,path'
        + (f' limit {max(1, int(limit))}' if limit else ''), tuple(params)
    )
    db.execute(
        "update artwork_jobs set status='running',total=?,started_at=CURRENT_TIMESTAMP where id=?",
        (len(rows), job_id),
    )
    checked = repaired = already_ok = failed = 0
    changed = False
    try:
        for row in rows:
            path = Path(row['path'])
            checked += 1
            db.execute('update artwork_jobs set checked=?,current_path=? where id=?', (checked, str(path), job_id))
            if not path.exists():
                failed += 1
                db.execute(
                    "update library_files set artwork_status='failed',artwork_checked_at=CURRENT_TIMESTAMP,artwork_error='file_missing' where path=?",
                    (str(path),),
                )
                continue
            if has_embedded_cover(path):
                already_ok += 1
                db.execute(
                    "update library_files set artwork_status='embedded',artwork_checked_at=CURRENT_TIMESTAMP,artwork_error=null where path=?",
                    (str(path),),
                )
                continue
            track = db.fetchone('select * from tracks where file_path=? order by updated_at desc limit 1', (str(path),))
            candidate = track or {
                'platform': 'netease', 'song_id': '', 'title': row.get('title') or path.stem,
                'artist': row.get('artist') or '', 'album': row.get('album') or '', 'cover_url': None,
            }
            try:
                artwork = await resolve_artwork(db, candidate)
                tagged = write_tags(
                    str(path), title=candidate['title'], artist=candidate.get('artist', ''),
                    album=candidate.get('album', ''), cover=artwork['data'], cover_mime=artwork['mime'],
                )
                if not tagged['cover_written'] or not has_embedded_cover(path):
                    raise RuntimeError(tagged.get('error') or 'cover_verification_failed')
                digest = file_sha256(path)
                stat = path.stat()
                db.execute(
                    """update library_files set size=?,mtime_ns=?,file_hash=?,artwork_status='embedded',
                       artwork_checked_at=CURRENT_TIMESTAMP,artwork_error=null,indexed_at=CURRENT_TIMESTAMP where path=?""",
                    (stat.st_size, stat.st_mtime_ns, digest, str(path)),
                )
                db.execute(
                    """update tracks set file_hash=?,cover_url=?,cover_status='embedded',cover_error=null,
                       updated_at=CURRENT_TIMESTAMP where file_path=?""",
                    (digest, artwork['url'], str(path)),
                )
                repaired += 1
                changed = True
            except Exception as exc:
                failed += 1
                detail = str(exc)[:300]
                db.execute(
                    "update library_files set artwork_status='failed',artwork_checked_at=CURRENT_TIMESTAMP,artwork_error=? where path=?",
                    (detail, str(path)),
                )
                if track:
                    db.execute(
                        "update tracks set cover_status='failed',cover_error=?,updated_at=CURRENT_TIMESTAMP where id=?",
                        (detail, track['id']),
                    )
            if checked % 20 == 0:
                db.execute(
                    'update artwork_jobs set repaired=?,already_ok=?,failed=? where id=?',
                    (repaired, already_ok, failed, job_id),
                )
                await asyncio.sleep(0)
        db.execute(
            """update artwork_jobs set status='completed',checked=?,repaired=?,already_ok=?,failed=?,
               current_path=null,completed_at=CURRENT_TIMESTAMP where id=?""",
            (checked, repaired, already_ok, failed, job_id),
        )
        if changed:
            token = load_fnos_token()
            if token:
                try:
                    await FnosAdapter(os.getenv('FNOS_API_URL'), token).refresh_library()
                except Exception:
                    pass
    except Exception as exc:
        db.execute(
            "update artwork_jobs set status='failed',checked=?,repaired=?,already_ok=?,failed=?,error=?,completed_at=CURRENT_TIMESTAMP where id=?",
            (checked, repaired, already_ok, failed, str(exc)[:500], job_id),
        )
