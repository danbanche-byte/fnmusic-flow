from __future__ import annotations

import asyncio
import hashlib
import os
import re
from dataclasses import dataclass

import httpx

from .models import TrackQuery

# 协议探测使用的稳定歌曲 ID（网易云）
PROBE_SONG_ID = '421423808'
# 各平台对应的探针歌曲（仅用于「一键测试」时的在线探测）
PROBE_SONGS = {'wy': '421423808', 'tx': '0039MnYb0qxYhV'}

# 应用内的平台名 <-> lx-music 的源 key
PLATFORM_TO_LX = {'netease': 'wy', 'qq': 'tx', 'kugou': 'kg', 'kuwo': 'kw', 'migu': 'mg'}
LX_TO_PLATFORM = {value: key for key, value in PLATFORM_TO_LX.items()}
# 音质降级顺序：请求的音质脚本不支持时，向后找第一个支持的
QUALITY_ORDER = ['master', 'atmos_plus', 'atmos', 'hires', 'flac24bit', 'flac', '320k', '128k']


@dataclass
class ParsedLXScript:
    api_url: str
    api_key: str | None
    qualities: dict[str, list[str]]
    name: str = "LX 音源"
    version: str = "unknown"


class LXScriptParser:
    """只解析落雪音源声明，不执行第三方 JavaScript。"""

    def parse(self, content: str) -> ParsedLXScript:
        api_match = re.search(r'''(?:const|let|var)\s+API_URL\s*=\s*["']([^"']+)''', content)
        if not api_match:
            raise ValueError("未找到 API_URL，暂不支持该音源格式")
        if not api_match.group(1).strip():
            raise ValueError("音源 API_URL 为空，请导入包含服务地址的落雪音源")
        key_match = re.search(r'''(?:const|let|var)\s+API_KEY\s*=\s*["']([^"']*)''', content)
        request_key_match = re.search(r'''["']X-Request-Key["']\s*:\s*API_KEY|(?:const|let|var)\s+API_KEY\s*=\s*["']([^"']*)''', content)
        quality_match = re.search(r'''const\s+MUSIC_QUALITY\s*=\s*(\{.*?\});''', content, re.S)
        qualities: dict[str, list[str]] = {}
        if quality_match:
            for match in re.finditer(r'''["']([a-z]+)["']\s*:\s*\[([^]]*)\]''', quality_match.group(1)):
                qualities[match.group(1)] = re.findall(r'''["']([^"']+)["']''', match.group(2))
        name_match = re.search(r'@name\s+([^\r\n*]+)', content)
        version_match = re.search(r'@version\s+([^\r\n*]+)', content)
        key = key_match.group(1) if key_match else (request_key_match.group(1) if request_key_match and request_key_match.lastindex else None)
        return ParsedLXScript(api_match.group(1).rstrip('/'), key,
                              qualities, name_match.group(1).strip() if name_match else "LX 音源",
                              version_match.group(1).strip() if version_match else "unknown")


class LXSourceRuntime:
    """私有 REST 协议适配器：GET {api_url}/url?source&songId&quality，鉴权 X-API-Key。"""

    def __init__(self, parsed: ParsedLXScript):
        self.parsed = parsed

    async def resolve(self, query: TrackQuery) -> str:
        source = {'netease': 'wy', 'qq': 'tx'}.get(query.platform, query.platform)
        params = {"source": source, "songId": query.song_id, "quality": query.quality}
        headers = {"Content-Type": "application/json", "User-Agent": "fnmusic-flow/lx-runtime"}
        if self.parsed.api_key:
            headers["X-API-Key"] = self.parsed.api_key
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            response = await client.get(f"{self.parsed.api_url}/url", params=params, headers=headers)
            response.raise_for_status()
            data = response.json()
        code = int(data.get("code", 0)) if isinstance(data, dict) else 0
        if code != 200 or not data.get("url"):
            message = data.get("message", "音源未返回有效地址") if isinstance(data, dict) else "音源响应格式错误"
            raise RuntimeError(f"source_error:{code}:{message}")
        return str(data["url"])

    async def search(self, keyword: str, platform: str = 'netease') -> list[dict]:
        source = {'netease': 'wy', 'qq': 'tx'}.get(platform, platform)
        headers = {'User-Agent': 'fnmusic-flow/lx-runtime'}
        if self.parsed.api_key: headers['X-API-Key'] = self.parsed.api_key
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            response = await client.get(f'{self.parsed.api_url}/search', params={'keyword': keyword, 'kw': keyword, 'source': source}, headers=headers)
            response.raise_for_status(); data = response.json()
        rows = data.get('data') or data.get('songs') or data.get('result') or [] if isinstance(data, dict) else data
        if isinstance(rows, dict): rows = rows.get('songs') or rows.get('list') or []
        result=[]
        for row in rows if isinstance(rows,list) else []:
            if not isinstance(row,dict): continue
            sid=row.get('song_id') or row.get('songId') or row.get('id') or row.get('songmid') or row.get('mid')
            title=row.get('title') or row.get('name') or ''; artist=row.get('artist') or row.get('singer') or row.get('author') or ''
            album = row.get('album') or row.get('al') or ''
            cover_url = ((album.get('picUrl') or album.get('cover_url')) if isinstance(album, dict)
                         else row.get('cover_url') or row.get('cover') or row.get('picUrl'))
            album_name = album.get('name', '') if isinstance(album, dict) else album
            if platform == 'qq' and not cover_url:
                album_mid = (album.get('mid') if isinstance(album, dict) else None) or row.get('albummid')
                if album_mid: cover_url = f'https://y.gtimg.cn/music/photo_new/T002R800x800M000{album_mid}.jpg'
            if sid and title: result.append({'platform':platform,'song_id':str(sid),'title':str(title),'artist':str(artist),'album':str(album_name or ''),'cover_url':cover_url})
        return result

    async def health(self) -> dict:
        """有界只读探针：先试解析接口，失败再试搜索接口（探针歌曲下架不算音源故障）。"""
        try:
            await self.resolve(TrackQuery(platform='netease', song_id=PROBE_SONG_ID, quality='320k'))
            return {'status': 'ready', 'detail': '歌曲解析接口响应正常'}
        except httpx.HTTPStatusError as exc:
            code = exc.response.status_code
            status = 'forbidden' if code in (401, 403) else 'error'
            return {'status': status, 'detail': f'HTTP {code}'}
        except httpx.TimeoutException:
            return {'status': 'timeout', 'detail': '请求超时'}
        except Exception as exc:
            try:
                await self.search('测试')
                return {'status': 'ready', 'detail': '解析探针未命中，搜索接口正常'}
            except Exception:
                return {'status': 'error', 'detail': str(exc)[:200]}


class MusicApiSourceRuntime:
    """ikun / 自建中转 API 协议适配器。

    这类落雪「自定义源」脚本本身只是薄壳：声明 API_URL 与 API_KEY 后，把请求转发给
    服务端的 POST {api_url}/music/url，鉴权头是 X-Api-Key，请求体
    {source, musicId, quality}，响应 {code:200, url, message, quality}。

    与标准 LX Server 的区别：路径不带 /api 前缀、用 POST、参数名是 musicId、
    鉴权头是 X-Api-Key；与私有 REST 的区别：不是 GET /url，参数名也不是 songId。
    """

    def __init__(self, api_url: str, api_key: str | None = None):
        self.api_url = api_url.rstrip('/')
        self.api_key = api_key or None

    def _headers(self) -> dict:
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json',
                   'User-Agent': 'lx-music-request/2.11.0'}
        if self.api_key:
            headers['X-Api-Key'] = self.api_key
        return headers

    async def _post(self, path: str, payload: dict) -> dict:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            response = await client.post(f'{self.api_url}/{path.lstrip("/")}', json=payload,
                                         headers=self._headers())
        if response.status_code >= 400:
            raise RuntimeError(f'lx_http_{response.status_code}: {response.text[:120]}')
        data = _try_json(response)
        if not isinstance(data, dict):
            raise RuntimeError(f'响应不是有效 JSON：{response.text[:120]}')
        return data

    async def resolve(self, query: TrackQuery) -> str:
        source = {'netease': 'wy', 'qq': 'tx'}.get(query.platform, query.platform)
        data = await self._post('music/url', {'source': source, 'musicId': str(query.song_id),
                                             'quality': query.quality})
        code = int(data.get('code') or 0)
        if code == 200 and data.get('url'):
            return str(data['url'])
        message = data.get('message') or '音源未返回有效地址'
        raise RuntimeError(f'source_error:{code or "no_url"}:{message}')

    async def search(self, keyword: str, platform: str = 'netease') -> list[dict]:
        raise RuntimeError('该音源只提供播放地址解析，不支持搜索；搜索请用应用内置的网易云 / QQ 接口')

    async def health(self) -> dict:
        try:
            await self.resolve(TrackQuery(platform='netease', song_id=PROBE_SONG_ID, quality='320k'))
            return {'status': 'ready', 'detail': '解析接口正常（POST /music/url）'}
        except httpx.TimeoutException:
            return {'status': 'timeout', 'detail': '请求超时'}
        except Exception as exc:
            text = str(exc)
            lowered = text.lower()
            if any(key in lowered for key in ('lx_http_401', 'lx_http_403', 'source_error:401', 'source_error:403')):
                return {'status': 'forbidden', 'detail': text[:200]}
            return {'status': 'error', 'detail': text[:200]}


class LXServerSourceRuntime:
    """标准 LX Server 协议适配器：GET {url}/api/music/*，鉴权 x-user-token。"""

    def __init__(self, url: str, token: str | None = None):
        self.url = url.rstrip('/')
        self.token = token or None

    def _headers(self):
        headers = {'User-Agent': 'fnmusic-flow/1.0', 'Accept': 'application/json'}
        if self.token:
            headers['x-user-token'] = self.token
        return headers

    async def _get(self, path: str, params: dict) -> dict | list:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            response = await client.get(f'{self.url}/api/{path.lstrip("/")}', params=params, headers=self._headers())
            if response.status_code >= 400:
                raise RuntimeError(f'lx_http_{response.status_code}: {response.text[:120]}')
            return response.json()

    async def resolve(self, query: TrackQuery) -> str:
        source = {'netease': 'wy', 'qq': 'tx'}.get(query.platform, query.platform)
        data = await self._get('music/url', {'source': source, 'id': query.song_id, 'quality': query.quality})
        value = None
        if isinstance(data, dict):
            value = data.get('url')
            inner = data.get('data')
            if not value and isinstance(inner, dict):
                value = inner.get('url')
        if not value:
            code = data.get('code') if isinstance(data, dict) else ''
            raise RuntimeError(f'source_error:{code or "no_url"}:LX Server 未返回有效地址')
        return str(value)

    async def search(self, keyword: str, platform: str = 'netease') -> list[dict]:
        source = {'netease': 'wy', 'qq': 'tx'}.get(platform, platform)
        data = await self._get('music/search', {'kw': keyword, 'source': source, 'page': 1, 'type': 'song'})
        rows = data.get('data') or data.get('result') or data.get('songs') or [] if isinstance(data, dict) else data
        if isinstance(rows, dict): rows = rows.get('list') or rows.get('songs') or []
        result = []
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict): continue
            sid = row.get('song_id') or row.get('songId') or row.get('id') or row.get('songmid') or row.get('mid')
            title = row.get('title') or row.get('name') or row.get('songname') or ''
            if not (sid and title): continue
            artists = row.get('artists') or row.get('ar') or []
            artist = row.get('artist') or row.get('singer') or ' / '.join(x.get('name', '') for x in artists if isinstance(x, dict))
            album = row.get('album') or row.get('al') or {}
            cover_url = ((album.get('picUrl') or album.get('cover_url')) if isinstance(album, dict)
                         else None) or row.get('cover_url') or row.get('cover') or row.get('picUrl')
            if source == 'tx' and not cover_url:
                album_mid = (album.get('mid') if isinstance(album, dict) else None) or row.get('albummid')
                if album_mid: cover_url = f'https://y.gtimg.cn/music/photo_new/T002R800x800M000{album_mid}.jpg'
            album_name = album.get('name', '') if isinstance(album, dict) else album
            result.append({'platform': platform, 'song_id': str(sid), 'title': str(title), 'artist': str(artist or ''),
                            'album': str(album_name or ''), 'cover_url': cover_url})
        return result

    async def health(self) -> dict:
        try:
            rows = await self.search('流行')
            return {'status': 'ready', 'detail': f'LX Server 搜索接口正常（{len(rows)} 条结果）'}
        except httpx.TimeoutException:
            return {'status': 'timeout', 'detail': '请求超时'}
        except Exception as exc:
            text = str(exc)
            status = 'forbidden' if 'lx_http_401' in text or 'lx_http_403' in text else 'error'
            return {'status': status, 'detail': text[:200]}


def _try_json(response: httpx.Response):
    try:
        return response.json()
    except Exception:
        return None


def extract_url_candidates(content: str, limit: int = 6) -> list[str]:
    """从音源 JS 中提取候选 API 地址（脚本未声明 API_URL 时的兜底探测）。"""
    urls: list[str] = []
    for match in re.finditer(r'''["'](https?://[^"'\s<>]{4,200})["']''', content):
        url = match.group(1).rstrip('/')
        if url in urls:
            continue
        if re.search(r'\.(js|css|png|jpe?g|svg|gif|ico|woff2?|html)(\?|$)', url, re.I):
            continue
        urls.append(url)
        if len(urls) >= limit:
            break
    return urls


async def detect_protocol(api_url: str, api_key: str | None = None):
    """探测音源后端协议。返回 (kind, runtime)；三种协议都不通时返回 None。

    判定依据：目标路由返回 404 视为该协议不存在；返回 JSON（含 code/url 字段）
    视为协议匹配（即使探针歌曲本身解析失败，也说明端点与协议是对的）。
    依次尝试：私有 REST（GET /url）→ 标准 LX Server（GET /api/music/url）→
    ikun 自建 API（POST /music/url）。三条路由互不相同，不会互相误判。

    注意：原生 lx-music 自定义源脚本（globalThis.lx 回调型）不走这里，它由
    LXCustomSourceRuntime + 内置 Node 宿主执行，见 looks_like_lx_custom_script。
    """
    api_url = api_url.rstrip('/')
    # 1) 私有 REST 协议：GET {url}/url
    try:
        headers = {'User-Agent': 'fnmusic-flow/probe', 'Accept': 'application/json'}
        if api_key:
            headers['X-API-Key'] = api_key
        async with httpx.AsyncClient(timeout=8, follow_redirects=True) as client:
            probe = await client.get(f'{api_url}/url',
                                    params={'source': 'wy', 'songId': PROBE_SONG_ID, 'quality': '128k'},
                                    headers=headers)
        if probe.status_code != 404:
            data = _try_json(probe)
            if isinstance(data, dict) and ('url' in data or 'code' in data):
                return 'lx_script', LXSourceRuntime(ParsedLXScript(api_url, api_key, {}))
    except Exception:
        pass
    # 2) 标准 LX Server 协议：GET {url}/api/music/url
    try:
        headers = {'User-Agent': 'fnmusic-flow/probe', 'Accept': 'application/json'}
        if api_key:
            headers['x-user-token'] = api_key
        async with httpx.AsyncClient(timeout=8, follow_redirects=True) as client:
            probe = await client.get(f'{api_url}/api/music/url',
                                    params={'source': 'wy', 'id': PROBE_SONG_ID, 'quality': '128k'},
                                    headers=headers)
        if probe.status_code != 404:
            data = _try_json(probe)
            if isinstance(data, dict) and ('url' in data or 'code' in data or 'data' in data):
                return 'lx_server', LXServerSourceRuntime(api_url, api_key)
    except Exception:
        pass
    # 3) ikun / 自建中转 API 协议：POST {url}/music/url
    try:
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json',
                   'User-Agent': 'lx-music-request/2.11.0'}
        if api_key:
            headers['X-Api-Key'] = api_key
        async with httpx.AsyncClient(timeout=8, follow_redirects=True) as client:
            probe = await client.post(f'{api_url}/music/url',
                                      json={'source': 'wy', 'musicId': PROBE_SONG_ID, 'quality': '128k'},
                                      headers=headers)
        if probe.status_code != 404:
            data = _try_json(probe)
            if isinstance(data, dict) and ('url' in data or 'code' in data):
                return 'lx_api', MusicApiSourceRuntime(api_url, api_key)
    except Exception:
        pass
    return None


# ---------------------------------------------------------------- 原生 lx-music 自定义源

def looks_like_lx_custom_script(content: str) -> bool:
    """判断一份 .js 是不是「lx-music 自定义源脚本」（回调型，跑在 globalThis.lx 宿主里）。

    这类脚本在 lx-music 客户端里能直接用，它的解析逻辑写在脚本内部，任何靠
    API_URL + 固定路由去猜的适配器都不该接它 —— 必须原样交给宿主执行。
    """
    text = content or ''
    if 'globalThis.lx' in text or 'globalThis["lx"]' in text or "globalThis['lx']" in text:
        return True
    return 'EVENT_NAMES' in text and ('EVENT_NAMES.request' in text or 'EVENT_NAMES.inited' in text)


def parse_lx_custom_meta(content: str) -> dict:
    """读取脚本头部注释里的 @name / @version / @author / @homepage / @description。"""
    result: dict[str, str] = {}
    for key in ('name', 'version', 'author', 'homepage', 'description'):
        match = re.search(r'@' + key + r'\s+([^\r\n*]+)', content or '')
        if match:
            result[key] = match.group(1).strip()
    return result


class LXCustomSourceRuntime:
    """原生 lx-music 自定义源运行时。

    脚本本身不在这里解释，而是原样交给容器内的 Node 宿主（app/lxnode/host.mjs）执行：
    宿主实现 globalThis.lx（EVENT_NAMES / on / send / request / utils / env / version），
    脚本代码与在 lx-music 客户端里运行时完全一致，因此任何能在 lx-music 里用的自定义源
    都能直接导入使用，不需要额外的中转服务。
    """

    def __init__(self, script_id: str, code: str, name: str = 'LX 自定义源', endpoint: str | None = None):
        self.script_id = script_id
        self.code = code
        self.name = name
        self.endpoint = endpoint
        self.host = (os.getenv('LX_HOST_URL')
                     or f'http://127.0.0.1:{os.getenv("LX_HOST_PORT", "18432")}').rstrip('/')
        self._ready = False
        self._error: str | None = None
        self._platforms: dict = {}
        self._script_version: str | None = None
        self._lock = asyncio.Lock()

    @property
    def digest(self) -> str:
        return hashlib.md5((self.code or '').encode('utf-8')).hexdigest()

    # ----------------------------------------------------------------- 宿主通信
    async def _post(self, path: str, payload: dict, timeout: float = 45.0) -> dict:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            response = await client.post(f'{self.host}{path}', json=payload)
        data = _try_json(response)
        if not isinstance(data, dict):
            raise RuntimeError(f'内置自定义源宿主返回了非 JSON 响应（HTTP {response.status_code}）')
        return data

    async def ensure_loaded(self, force: bool = False) -> None:
        """把脚本装进宿主（幂等：同一脚本内容不会重复初始化）。"""
        async with self._lock:
            if self._ready and not force:
                return
            try:
                data = await self._post('/script', {
                    'id': self.script_id, 'hash': self.digest, 'name': self.name, 'code': self.code,
                })
            except Exception as exc:
                self._ready = False
                self._error = str(exc)[:200]
                raise RuntimeError(f'lx_host_error:内置自定义源宿主不可用（{self._error}）') from exc
            script = data.get('script') or {}
            self._platforms = script.get('sources') or {}
            self._script_version = script.get('version')
            if not data.get('ok'):
                self._ready = False
                self._error = str(data.get('error') or '脚本初始化失败')[:200]
                raise RuntimeError(f'lx_host_error:自定义源脚本初始化失败（{self._error}）')
            self._ready = True
            self._error = None

    def _pick_quality(self, source: str, quality: str) -> str:
        """脚本没有该音质时向后降级，避免因为音质档位不匹配直接失败。"""
        supported = (self._platforms.get(source) or {}).get('qualitys') or []
        if not supported or quality in supported:
            return quality
        if quality in QUALITY_ORDER:
            for candidate in QUALITY_ORDER[QUALITY_ORDER.index(quality):]:
                if candidate in supported:
                    return candidate
        for candidate in QUALITY_ORDER:
            if candidate in supported:
                return candidate
        return quality

    async def resolve(self, query: TrackQuery) -> str:
        await self.ensure_loaded()
        source = PLATFORM_TO_LX.get(query.platform, query.platform)
        if self._platforms and source not in self._platforms:
            raise RuntimeError(f'source_error:unsupported:该脚本不支持 {query.platform} 平台'
                               f'（它声明的是 {"、".join(self._platforms) or "无"}）')
        song_id = str(query.song_id)
        # 2026-09-29（1.0.8）：候选档位 = 首选档 + 声明支持的更低档位逐级降级。
        # 部分源对 hires/master 是「声明了但单曲没有」直接报错而不是返回低档地址
        # （实测 ikun 源请求 hires 返回「获取URL失败」），不做同源降档重试的话
        # 整次解析直接失败。沿 QUALITY_ORDER 向下重试，直到拿到地址或试完。
        supported = (self._platforms.get(source) or {}).get('qualitys') or []
        picked = self._pick_quality(source, query.quality)
        candidates = [picked]
        if picked in QUALITY_ORDER:
            for cand in QUALITY_ORDER[QUALITY_ORDER.index(picked) + 1:]:
                if supported and cand not in supported:
                    continue
                candidates.append(cand)
        last_error = '脚本未返回有效地址'
        for quality in candidates:
            data = await self._post('/resolve', {
                'id': self.script_id, 'source': source, 'songId': song_id,
                'quality': quality,
                # lx-music 传给自定义源的 musicInfo：各平台取 ID 的字段名不同，这里都填上
                'musicInfo': {'id': song_id, 'songmid': song_id, 'mid': song_id,
                              'hash': song_id, 'songId': song_id, 'source': source},
            }, timeout=45.0)
            if data.get('ok') and data.get('url'):
                return str(data['url'])
            last_error = str(data.get('error') or '脚本未返回有效地址')[:180]
        raise RuntimeError(f'source_error:custom:{last_error}')

    async def search(self, keyword: str, platform: str = 'netease') -> list[dict]:
        raise RuntimeError('原生自定义源脚本只提供播放地址解析，不支持搜索；'
                          '搜索请用应用内置的网易云 / QQ 接口')

    async def logs(self) -> dict:
        """取回宿主里该脚本的运行日志，方便排查「这个源为什么不行」。"""
        try:
            async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
                response = await client.get(f'{self.host}/health')
            data = response.json()
        except Exception as exc:
            return {'ok': False, 'error': f'内置自定义源宿主不可用：{str(exc)[:160]}'}
        for row in (data.get('scripts') or []):
            if row.get('id') == self.script_id:
                return {'ok': True, 'script': {key: value for key, value in row.items() if key != 'logs'},
                        'logs': row.get('logs') or []}
        return {'ok': False, 'error': '脚本尚未加载到宿主（首次解析或健康探测后才会加载）'}

    async def health(self) -> dict:
        try:
            await self.ensure_loaded()
        except Exception as exc:
            return {'status': 'error', 'detail': str(exc)[:200], 'platforms': []}

        platforms = sorted(self._platforms.keys())
        label = '、'.join(platforms) or '无'
        base = f'脚本已就绪（{self._script_version or "unknown"}），支持：{label}'
        probe_source = next((key for key in ('wy', 'tx') if key in self._platforms), None)
        if not probe_source:
            return {'status': 'ready', 'detail': f'{base}；该平台未做在线探测', 'platforms': platforms}
        try:
            await self.resolve(TrackQuery(platform=LX_TO_PLATFORM.get(probe_source, 'netease'),
                                          song_id=PROBE_SONGS[probe_source], quality='320k'))
            return {'status': 'ready', 'detail': f'{base}；解析探针通过', 'platforms': platforms}
        except httpx.TimeoutException:
            return {'status': 'timeout', 'detail': '请求超时', 'platforms': platforms}
        except Exception as exc:
            text = str(exc)
            lowered = text.lower()
            if any(key in text for key in ('鉴权', '密钥', '令牌', '已失效', '已过期')) \
                    or '401' in lowered or '403' in lowered:
                return {'status': 'forbidden', 'detail': f'{base}；{text[:140]}', 'platforms': platforms}
            if '超时' in text or 'timeout' in lowered:
                return {'status': 'timeout', 'detail': f'{base}；{text[:140]}', 'platforms': platforms}
            # 探针歌曲本身不可用（下架 / 无版权 / 该音质未开放）不算音源故障
            return {'status': 'ready', 'detail': f'{base}；探针未命中：{text[:120]}', 'platforms': platforms}
