from __future__ import annotations

import httpx

from .metadata import image_mime
from .platforms import NeteaseConnector, QQMusicConnector
from .track_match import choose_match, title_search_terms


MAX_ARTWORK_BYTES = 8 * 1024 * 1024


def qq_cover_url(album_mid: object) -> str | None:
    value = str(album_mid or '').strip()
    return f'https://y.gtimg.cn/music/photo_new/T002R800x800M000{value}.jpg' if value else None


async def download_artwork(url: str) -> tuple[bytes, str]:
    if not str(url or '').lower().startswith(('http://', 'https://')):
        raise RuntimeError('cover_url_invalid')
    headers = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://y.qq.com/'}
    async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
        async with client.stream('GET', url, headers=headers) as response:
            response.raise_for_status()
            content = bytearray()
            async for chunk in response.aiter_bytes():
                content.extend(chunk)
                if len(content) > MAX_ARTWORK_BYTES:
                    raise RuntimeError('cover_too_large')
    data = bytes(content)
    mime = image_mime(data)
    if not mime:
        raise RuntimeError('cover_not_supported_image')
    return data, mime


async def _search_candidates(db, track: dict, platform: str) -> list[dict]:
    account = db.fetchone('select cookie from accounts where provider=?', (platform,)) or {}
    connector = NeteaseConnector(account.get('cookie')) if platform == 'netease' else QQMusicConnector(account.get('cookie'))
    artist = str(track.get('artist') or '').strip()
    queries = [f'{title} {artist}'.strip() for title in title_search_terms(track.get('title'))]
    rows: list[dict] = []
    for query in dict.fromkeys(value for value in queries if value):
        try:
            found = await connector.search(query, 30) if platform == 'netease' else await connector.search(query, 1, 30)
            rows.extend(found)
            if choose_match(track, rows):
                break
        except Exception:
            continue
    return rows


async def resolve_artwork(db, track: dict, alternate: dict | None = None) -> dict:
    errors: list[str] = []
    direct = [track.get('cover_url'), (alternate or {}).get('cover_url')]
    for url in dict.fromkeys(str(value) for value in direct if value):
        try:
            data, mime = await download_artwork(url)
            return {'data': data, 'mime': mime, 'url': url, 'source': 'platform'}
        except Exception as exc:
            errors.append(f'{url[:80]}: {exc}')

    preferred = str(track.get('platform') or 'netease')
    platforms = [preferred, 'qq' if preferred == 'netease' else 'netease']
    for platform in platforms:
        candidates = await _search_candidates(db, track, platform)
        match = choose_match(track, candidates)
        ordered = ([match] if match else []) + [row for row in candidates if row is not match]
        for candidate in ordered[:5]:
            url = candidate.get('cover_url')
            if not url:
                continue
            try:
                data, mime = await download_artwork(str(url))
                return {'data': data, 'mime': mime, 'url': str(url), 'source': f'{platform}_scrape'}
            except Exception as exc:
                errors.append(f'{platform}: {exc}')
    raise RuntimeError('cover_not_found' + (': ' + ' | '.join(errors[-3:]) if errors else ''))
