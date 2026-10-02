from __future__ import annotations

from urllib.parse import parse_qs, urlparse
import re


def identify(url: str) -> dict:
    parsed = urlparse(url.strip())
    # NetEase commonly shares links as music.163.com/#/playlist?id=123 and
    # QQ uses y.qq.com/n/ryqq/playlist/123. Parse query, fragment and path.
    query = parse_qs(parsed.query)
    fragment_query = parse_qs(parsed.fragment.split('?', 1)[-1]) if parsed.fragment else {}
    for key, values in fragment_query.items():
        query.setdefault(key, values)
    host = parsed.netloc.lower()
    if 'y.qq.com' in host or 'qq.com' in host:
        playlist_id = (query.get('id') or query.get('songlistid') or query.get('disstid') or [None])[0]
        if not playlist_id:
            match = re.search(r'/(?:playlist|n/ryqq/playlist|songlist)/([0-9]+)', parsed.path)
            playlist_id = match.group(1) if match else None
        platform = 'qq'
    elif 'music.163.com' in host or '163.com' in host:
        playlist_id = (query.get('id') or [None])[0]
        if not playlist_id:
            match = re.search(r'/(?:playlist|discover/toplist/detail)/([0-9]+)', parsed.path)
            playlist_id = match.group(1) if match else None
        platform = 'netease'
    else:
        platform, playlist_id = 'unknown', None
    return {'platform': platform, 'playlist_id': playlist_id, 'url': url, 'supported': bool(playlist_id)}


def parse_text(text: str) -> list[dict]:
    """解析用户粘贴的简单歌曲清单，作为无平台接口时的兜底。"""
    result = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith('#'): continue
        pieces = re.split(r'\s+-\s+|\s+—\s+', line, maxsplit=1)
        title, artist = pieces[0], pieces[1] if len(pieces) > 1 else ''
        result.append({'title': title, 'artist': artist})
    return result
