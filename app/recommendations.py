import httpx
from .phase23 import normalize_rows

class RecommendationProvider:
    def __init__(self, provider, endpoint, cookie=None):
        self.provider, self.endpoint, self.cookie = provider, endpoint.rstrip('/'), cookie
    async def fetch(self, kind='daily'):
        headers = {'User-Agent':'fnmusic-flow/recommendation-adapter/1.0'}
        if self.cookie: headers['Cookie'] = self.cookie
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            response = await client.get(self.endpoint, params={'kind': kind}, headers=headers)
            response.raise_for_status()
            return normalize_rows(response.json(), self.provider)
