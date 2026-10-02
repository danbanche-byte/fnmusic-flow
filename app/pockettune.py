from __future__ import annotations

from dataclasses import dataclass
import httpx


@dataclass
class Recommendation:
    platform: str
    song_id: str
    title: str
    artist: str = ''
    album: str = ''
    duration_ms: int | None = None


class PocketTuneCompatibleAdapter:
    """独立的日推适配器接口；不启动或依赖 PocketTune 进程。"""

    def __init__(self, platform: str, endpoint: str, cookie: str | None = None):
        self.platform, self.endpoint, self.cookie = platform, endpoint.rstrip('/'), cookie

    async def fetch(self) -> list[Recommendation]:
        headers = {'User-Agent': 'fnmusic-flow/recommendation-adapter'}
        if self.cookie:
            headers['Cookie'] = self.cookie
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            response = await client.get(self.endpoint, headers=headers)
            response.raise_for_status()
            data = response.json()
        return self._normalize(data)

    def _normalize(self, data: dict) -> list[Recommendation]:
        rows = data.get('data') or data.get('songs') or data.get('playlist') or []
        result = []
        for row in rows:
            result.append(Recommendation(
                platform=self.platform,
                song_id=str(row.get('id') or row.get('songmid') or row.get('mid') or row.get('hash')),
                title=row.get('name') or row.get('title') or '',
                artist=row.get('artist') or row.get('singer') or '',
                album=row.get('album') or '',
                duration_ms=row.get('duration') or row.get('interval'),
            ))
        return [item for item in result if item.song_id and item.title]

