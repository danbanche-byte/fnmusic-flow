"""Built-in platform connectors.

The service talks to the bundled PocketTune/Netease API over loopback and to
QQ Music's public mobile gateway directly. No second application is required.
"""
from __future__ import annotations

import asyncio
import html
import os
import re
import time
from typing import Any

import httpx


class PlatformError(RuntimeError):
    pass


def _qq_cover(row: dict[str, Any]) -> str | None:
    album = row.get('album') if isinstance(row.get('album'), dict) else {}
    album_mid = album.get('mid') or row.get('albummid') or row.get('album_mid')
    if album_mid:
        return f'https://y.gtimg.cn/music/photo_new/T002R800x800M000{album_mid}.jpg'
    value = row.get('cover_url') or row.get('picurl') or row.get('picUrl')
    return str(value).replace('http:', 'https:') if value else None


def _plain_text(value: Any) -> str:
    return html.unescape(re.sub(r'<[^>]+>', '', str(value or ''))).strip()


class NeteaseConnector:
    def __init__(self, cookie: str | None = None):
        self.base = os.getenv("POCKETTUNE_API_URL", "http://127.0.0.1:3000/api").rstrip("/")
        self.cookie = cookie or ""

    async def call(self, route: str, params: dict[str, Any] | None = None) -> Any:
        headers = {"User-Agent": "fnmusic-flow/1.0"}
        if self.cookie:
            headers["Cookie"] = self.cookie
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            response = await client.get(f"{self.base}/netease/{route.lstrip('/')}", params=params or {}, headers=headers)
            raw = response.text
            try:
                data = response.json()
            except (ValueError, TypeError) as exc:
                detail = ' '.join(raw.split())[:160]
                raise PlatformError(f"netease_non_json_{response.status_code}"
                                    + (f": {detail}" if detail else "")) from exc
            if response.status_code >= 400:
                message = data.get('message') or data.get('msg') or data.get('error') if isinstance(data, dict) else ''
                suffix = f": {str(message)[:120]}" if message else ''
                raise PlatformError(f"netease_http_{response.status_code}{suffix}")
            if isinstance(data, dict):
                code = data.get('code')
                # Netease uses 301/302 for an expired session while still
                # returning HTTP 200. Surface this distinctly so callers can
                # retain cached playlists and show a re-login hint.
                if code not in (None, 0, 200):
                    raise PlatformError(f"netease_api_{code}")
            return data

    async def daily(self) -> Any:
        return await self.call("recommend/songs", {"timestamp": int(time.time() * 1000)})

    async def playlists(self, uid: str) -> Any:
        return await self.call("user/playlist", {"uid": uid, "limit": 100, "offset": 0})

    async def exclusive_playlists(self) -> list[dict[str, Any]]:
        """Return 网易云账号专属推荐歌单（含私人雷达）。

        ``user/playlist`` only contains playlists owned/collected by the
        account.  网易云的“专属歌单/私人雷达” is exposed by a separate
        ``recommend/resource`` endpoint, so treating the account playlist
        response as complete makes that card disappear even though the login
        is valid.  Keep this request separate and let the caller fall back to
        its local snapshot when the session has expired.
        """
        data = await self.call("recommend/resource", {"limit": 100, "offset": 0})
        rows = data.get("recommend") if isinstance(data, dict) else []
        result: list[dict[str, Any]] = []
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict) or not row.get("id"):
                continue
            creator = row.get("creator") or {}
            result.append({
                "provider": "netease", "playlist_id": str(row["id"]),
                "title": str(row.get("name") or ""),
                "description": str(row.get("description") or ""),
                "cover_url": row.get("picUrl") or row.get("coverImgUrl") or row.get("cover"),
                "owner": str(creator.get("nickname") or "网易云音乐") if isinstance(creator, dict) else "网易云音乐",
                "item_count": int(row.get("trackCount") or 0),
                "play_count": int(row.get("playCount") or 0),
                "kind": "exclusive",
            })
        return result

    async def recommend_playlists(self, limit: int = 50) -> list[dict[str, Any]]:
        """返回网易云首页「专属歌单」卡片（kind='exclusive'）。

        与 PocketTune 首页「专属歌单」区块使用同一个接口 ``personalized``。
        实测同一账号：``recommend/resource`` 只返回 7 条，而
        ``personalized?limit=50`` 返回 50 条 —— 此前"专属歌单太少"
        （PocketTune 有 21 张、我们只有 7 张）根因就是调错了接口。

        返回的第一条通常是「私人雷达」，调用方按固定雷达 ID 迁移到雷达分区。
        """
        size = max(1, min(int(limit or 50), 100))
        data = await self.call("personalized", {"limit": size})
        rows = data.get("result") if isinstance(data, dict) else []
        result: list[dict[str, Any]] = []
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict) or not row.get("id"):
                continue
            creator = row.get("creator") or {}
            result.append({
                "provider": "netease", "playlist_id": str(row["id"]),
                "title": _plain_text(row.get("name")),
                "description": str(row.get("description") or row.get("copywriter") or ""),
                "cover_url": row.get("picUrl") or row.get("coverImgUrl") or row.get("cover"),
                "owner": str(creator.get("nickname") or "网易云音乐") if isinstance(creator, dict) else "网易云音乐",
                "item_count": int(row.get("trackCount") or row.get("songCount") or 0),
                "play_count": int(row.get("playCount") or 0),
                "kind": "exclusive",
            })
        return result

    # 网易云"雷达歌单"是固定 playlist_id 承载的动态推荐歌单（每人内容不同、
    # 每日更新），用登录 cookie 调 playlist/detail 即可拉取。ID 清单已与
    # PocketTune 首页「雷达歌单」逐条对齐（7 张，含此前遗漏的会员雷达）；
    # 若未来某 ID 失效，拉取后按"名称含'雷达'"校验兜底，失配即跳过该卡片，
    # 不影响其它卡片与整个分区。
    RADAR_PLAYLISTS: list[dict[str, str]] = [
        {"playlist_id": "3136952023", "name": "私人雷达"},
        {"playlist_id": "8402996200", "name": "会员雷达"},
        {"playlist_id": "5320167908", "name": "时光雷达"},
        {"playlist_id": "5327906368", "name": "乐迷雷达"},
        {"playlist_id": "5362359247", "name": "宝藏雷达"},
        {"playlist_id": "5300458264", "name": "新歌雷达"},
        {"playlist_id": "5341776086", "name": "神秘雷达"},
    ]

    async def radar_playlists(self) -> list[dict[str, Any]]:
        """返回网易云账号的雷达歌单卡片（kind='radar'）。

        7 张卡片并发拉取：串行实测 2.37s，并发后 0.61s（总耗时由最慢一路
        决定，不再线性叠加）。单张失败只跳过它自己，不影响其它卡片。
        """

        async def one(spec: dict[str, str]) -> dict[str, Any] | None:
            try:
                detail = await self.playlist(spec["playlist_id"])
            except Exception:
                # 单个雷达拉取失败（网络/登录态/超时）只跳过该卡片
                return None
            pl = detail.get("playlist") if isinstance(detail, dict) else None
            if not isinstance(pl, dict) or not pl.get("id"):
                return None
            title = str(pl.get("name") or "")
            # 防御：ID 失效时返回的是别的歌单，名称对不上就跳过
            if "雷达" not in title:
                return None
            creator = pl.get("creator") or {}
            return {
                "provider": "netease", "playlist_id": str(pl["id"]),
                "title": title or spec["name"],
                "description": str(pl.get("description") or ""),
                "cover_url": pl.get("coverImgUrl") or pl.get("picUrl") or pl.get("cover"),
                "owner": str(creator.get("nickname") or "网易云音乐") if isinstance(creator, dict) else "网易云音乐",
                "item_count": int(pl.get("trackCount") or 0),
                "play_count": int(pl.get("playCount") or 0),
                "kind": "radar",
                "daily_update": True,
            }

        cards = await asyncio.gather(*(one(spec) for spec in self.RADAR_PLAYLISTS))
        # 保持 RADAR_PLAYLISTS 的声明顺序（与 PocketTune 首页展示顺序一致）
        return [card for card in cards if card]

    async def playlist(self, playlist_id: str) -> Any:
        detail = await self.call("playlist/detail", {"id": playlist_id, "s": 0})
        # Fetch all tracks where the detail response only contains privileges.
        if isinstance(detail, dict) and isinstance(detail.get("playlist"), dict):
            pl = detail["playlist"]
            tracks = pl.get("tracks") or []
            if len(tracks) < int(pl.get("trackCount") or 0):
                try:
                    all_tracks = await self.call("playlist/track/all", {"id": playlist_id, "limit": 1000, "offset": 0})
                    if isinstance(all_tracks, dict) and isinstance(all_tracks.get("songs"), list):
                        pl["tracks"] = all_tracks["songs"]
                except PlatformError:
                    pass
        return detail

    async def charts(self) -> list[dict[str, Any]]:
        data = await self.call('toplist')
        rows = data.get('list') if isinstance(data, dict) else []
        async def enrich(row: dict[str, Any]) -> dict[str, Any]:
            if row.get('tracks'):
                return row
            try:
                detail = await self.call('playlist/track/all', {'id': row.get('id'), 'limit': 3, 'offset': 0})
                songs = detail.get('songs') if isinstance(detail, dict) else []
                row = dict(row)
                row['tracks'] = [{'first': song.get('name') or '',
                                  'second': ' / '.join(x.get('name','') for x in song.get('ar') or [] if isinstance(x,dict))}
                                 for song in songs if isinstance(song, dict)]
            except Exception:
                pass
            return row
        rows = list(rows) if isinstance(rows, list) else []
        if rows:
            rows[:4] = await asyncio.gather(*(enrich(row) for row in rows[:4]))
        result = []
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict) or not row.get('id'):
                continue
            tracks = []
            for track in row.get('tracks') or []:
                if isinstance(track, dict):
                    tracks.append({'title': track.get('first') or '', 'artist': track.get('second') or ''})
            result.append({'provider': 'netease', 'chart_id': str(row['id']), 'playlist_id': f"chart:{row['id']}",
                           'title': row.get('name') or '', 'cover_url': row.get('coverImgUrl'),
                           'description': row.get('updateFrequency') or '', 'tracks': tracks[:3]})
        return result

    async def chart(self, chart_id: str) -> Any:
        return await self.playlist(chart_id)

    async def plaza(self, category: str = '', offset: int = 0, limit: int = 30,
                    sort: str = 'hot') -> list[dict[str, Any]]:
        data = await self.call('top/playlist', {
            'limit': limit, 'offset': offset, 'cat': category or '全部',
            'order': 'new' if sort == 'latest' else 'hot',
        })
        rows = data.get('playlists') if isinstance(data, dict) else []
        return [{'provider': 'netease', 'playlist_id': str(x.get('id')), 'title': x.get('name') or '',
                 'description': x.get('description') or '', 'cover_url': x.get('coverImgUrl') or x.get('cover'),
                 'owner': (x.get('creator') or {}).get('nickname','') if isinstance(x.get('creator'),dict) else '',
                 'item_count': x.get('trackCount') or 0, 'play_count': x.get('playCount') or 0,
                 'update_time': x.get('updateTime') or x.get('createTime')}
                for x in rows if isinstance(x,dict) and x.get('id')]

    async def categories(self) -> list[dict[str, Any]]:
        data = await self.call('playlist/catlist')
        category_names = data.get('categories') if isinstance(data, dict) else {}
        groups: dict[str, list[dict[str, Any]]] = {}
        for row in (data.get('sub') or []) if isinstance(data, dict) else []:
            if not isinstance(row, dict) or not row.get('name'):
                continue
            group = str((category_names or {}).get(str(row.get('category')), '其他'))
            groups.setdefault(group, []).append({'id': str(row.get('name')), 'name': str(row.get('name'))})
        return [{'name': name, 'items': items} for name, items in groups.items()]

    async def search(self, keyword: str, limit: int = 30) -> list[dict[str, Any]]:
        data = await self.call('cloudsearch', {'keywords': keyword, 'type': 1, 'limit': limit, 'offset': 0})
        rows = ((data.get('result') or {}).get('songs') if isinstance(data, dict) else []) or []
        result=[]
        for row in rows:
            if not isinstance(row, dict): continue
            artists=row.get('ar') or []; album=row.get('al') or {}
            result.append({'platform':'netease','song_id':str(row.get('id')),'title':row.get('name') or '',
                           'artist':' / '.join(x.get('name','') for x in artists if isinstance(x,dict)),
                           'album':album.get('name','') if isinstance(album,dict) else '', 'duration_ms':row.get('dt'),
                           'cover_url': album.get('picUrl') if isinstance(album, dict) else None})
        return [x for x in result if x['song_id'] and x['title']]

    async def search_playlists(self, keyword: str, page: int = 1,
                               page_size: int = 30) -> dict[str, Any]:
        page = max(1, int(page))
        page_size = min(100, max(1, int(page_size)))
        data = await self.call('cloudsearch', {
            'keywords': keyword, 'type': 1000, 'limit': page_size,
            'offset': (page - 1) * page_size,
        })
        root = (data.get('result') or {}) if isinstance(data, dict) else {}
        rows = root.get('playlists') or []
        items = []
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict) or not row.get('id'):
                continue
            creator = row.get('creator') or {}
            items.append({
                'provider': 'netease', 'playlist_id': str(row['id']),
                'title': str(row.get('name') or ''),
                'description': str(row.get('description') or ''),
                'cover_url': row.get('coverImgUrl') or row.get('cover'),
                'owner': str(creator.get('nickname') or '') if isinstance(creator, dict) else '',
                'item_count': int(row.get('trackCount') or 0),
                'play_count': int(row.get('playCount') or 0),
                'update_time': row.get('updateTime') or row.get('createTime'),
            })
        total = int(root.get('playlistCount') or len(items))
        return {'items': items, 'total': total,
                'has_more': page * page_size < total}


class QQMusicConnector:
    API = "https://u.y.qq.com/cgi-bin/musicu.fcg"

    def __init__(self, cookie: str | None = None):
        self.cookie = cookie or ""

    def _cookies(self) -> dict[str, str]:
        result = {}
        for part in self.cookie.split(';'):
            key, separator, value = part.strip().partition('=')
            if separator and key:
                result[key] = value
        return result

    def account_id(self) -> str:
        cookies = self._cookies()
        return str(cookies.get('qm_str_musicid') or cookies.get('uin') or cookies.get('p_uin') or cookies.get('wxuin') or '').lstrip('o0')

    async def request(self, method: str, module: str, param: dict[str, Any]) -> dict[str, Any]:
        comm = {"ct": 11, "cv": "1003006", "v": "1003006", "os_ver": "15", "phonetype": "24122RKC7C",
                "tmeAppID": "qqmusiclight", "nettype": "NETWORK_WIFI", "udid": "0"}
        headers = {"Content-Type": "application/json", "User-Agent": "okhttp/3.14.9", "Cookie": self.cookie or "tmeLoginType=-1;"}
        body = {"comm": comm, "request": {"method": method, "module": module, "param": param}}
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            response = await client.post(self.API, json=body, headers=headers)
            if response.status_code >= 400:
                raise PlatformError(f"qq_http_{response.status_code}")
            data = response.json()
        if data.get("code") not in (None, 0) or data.get("request", {}).get("code") not in (None, 0):
            raise PlatformError(f"qq_api_{data.get('code') or data.get('request', {}).get('code')}")
        return data.get("request", {}).get("data", data)

    async def search(self, keyword: str, page: int = 1, page_size: int = 20) -> list[dict[str, Any]]:
        # The MusicU Lite search can stream an unbounded response for some
        # queries. QQ's public web search returns the same song mids with a
        # small, bounded payload and is considerably more stable on a NAS.
        url = "https://c.y.qq.com/soso/fcgi-bin/client_search_cp"
        params = {
            "w": keyword, "p": max(1, page), "n": min(max(1, page_size), 100),
            "format": "json", "new_json": 1, "cr": 1, "aggr": 1,
            "lossless": 1, "platform": "yqq.json",
        }
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            response = await client.get(
                url,
                params=params,
                headers={"User-Agent": "Mozilla/5.0", "Referer": "https://y.qq.com/"},
            )
            if response.status_code >= 400:
                raise PlatformError(f"qq_search_http_{response.status_code}")
            data = response.json()
        result = []
        rows = (((data.get("data") or {}).get("song") or {}).get("list") or [])
        for row in rows:
            album = row.get("album") or {}
            result.append({"platform": "qq", "song_id": str(row.get("mid") or row.get("songmid") or row.get("id")),
                           "mid": row.get("mid") or row.get("songmid"),
                           "title": row.get("title") or row.get("songname") or "",
                           "artist": " / ".join(x.get("name", "") for x in row.get("singer", []) if isinstance(x, dict)),
                           "album": album.get("name", "") if isinstance(album, dict) else str(row.get("albumname") or ""),
                           "duration_ms": int(row.get("interval") or 0) * 1000,
                           "cover_url": _qq_cover(row)})
        return [x for x in result if x["song_id"] and x["title"]]

    async def playlist(self, playlist_id: str) -> dict[str, Any]:
        # QQ's mobile gateway exposes public diss/song lists without requiring
        # a separate QQMusic application. Different gateway revisions return
        # either data.songlist or data.cdlist; normalize both forms.
        # This endpoint is more stable than the fcg gateway for public QQ playlists.
        url = 'https://c.y.qq.com/qzone/fcg-bin/fcg_ucc_getcdinfo_byids_cp.fcg'
        params = {'type': 1, 'json': 1, 'utf8': 1, 'onlysong': 0, 'new_format': 1,
                  'loginUin': 0, 'hostUin': 0, 'format': 'json', 'inCharset': 'utf8', 'outCharset': 'utf-8',
                  'platform': 'yqq.json', 'needNewCode': 0, 'disstid': str(playlist_id)}
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            response = await client.get(url, params=params, headers={'User-Agent': 'Mozilla/5.0', 'Referer': 'https://y.qq.com/'})
            if response.status_code >= 400: raise PlatformError(f'qq_playlist_http_{response.status_code}')
            raw = response.text.strip()
        raw = re.sub(r'^jsonCallback\s*\(', '', raw).rstrip('); \n\r')
        payload = __import__('json').loads(raw)
        root = (payload.get('cdlist') or [{}])[0]
        rows = root.get('songlist', []) if isinstance(root, dict) else []
        items = []
        for index, row in enumerate(rows if isinstance(rows, list) else []):
            artists = row.get("singer") or []
            items.append({"position": index, "platform": "qq", "song_id": str(row.get("mid") or row.get("songmid") or row.get("id")),
                          "title": row.get("songname") or row.get("songName") or row.get('title') or row.get('name') or "",
                          "artist": " / ".join(x.get("name", "") for x in artists),
                          "album": (row.get("album") or {}).get("name", "") if isinstance(row.get("album"), dict) else str(row.get("albumname") or ""),
                          "duration_ms": int(row.get("interval") or 0) * 1000,
                          "cover_url": _qq_cover(row)})
        return {"provider": "qq", "playlist_id": str(playlist_id), "title": root.get("dissname") or root.get("name") or str(playlist_id),
                "description": root.get("desc") or root.get("description") or "", "cover_url": root.get("logo") or root.get("cover_url"),
                "owner": root.get("nickname") or "", "items": [x for x in items if x["song_id"] and x["title"]]}

    async def charts(self) -> list[dict[str, Any]]:
        url = 'https://c.y.qq.com/v8/fcg-bin/fcg_myqq_toplist.fcg'
        params = {'format': 'json', 'inCharset': 'utf8', 'outCharset': 'utf-8',
                  'platform': 'yqq.json', 'needNewCode': 0}
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            response = await client.get(url, params=params, headers={'User-Agent': 'Mozilla/5.0', 'Referer': 'https://y.qq.com/'})
        if response.status_code >= 400:
            raise PlatformError(f'qq_charts_http_{response.status_code}')
        rows = (response.json().get('data') or {}).get('topList') or []
        result = []
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict) or row.get('id') is None:
                continue
            tracks = [{'title': x.get('songname') or '', 'artist': x.get('singername') or ''}
                      for x in row.get('songList') or [] if isinstance(x, dict)]
            result.append({'provider': 'qq', 'chart_id': str(row['id']), 'playlist_id': f"chart:{row['id']}",
                           'title': row.get('topTitle') or '', 'cover_url': str(row.get('picUrl') or '').replace('http:', 'https:'),
                           'description': row.get('update_key') or '', 'tracks': tracks[:3]})
        return result

    async def chart(self, chart_id: str) -> dict[str, Any]:
        url = 'https://c.y.qq.com/v8/fcg-bin/fcg_v8_toplist_cp.fcg'
        params = {'topid': str(chart_id), 'format': 'json', 'inCharset': 'utf8', 'outCharset': 'utf-8',
                  'platform': 'yqq.json', 'needNewCode': 0}
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            response = await client.get(url, params=params, headers={'User-Agent': 'Mozilla/5.0', 'Referer': 'https://y.qq.com/'})
        if response.status_code >= 400:
            raise PlatformError(f'qq_chart_http_{response.status_code}')
        payload = response.json()
        rows = payload.get('songlist') or []
        items = []
        for index, wrapper in enumerate(rows if isinstance(rows, list) else []):
            row = wrapper.get('data') if isinstance(wrapper, dict) and isinstance(wrapper.get('data'), dict) else wrapper
            if not isinstance(row, dict):
                continue
            artists = row.get('singer') or []
            song_id = row.get('songmid') or row.get('mid') or row.get('songid')
            title = row.get('songname') or row.get('title') or ''
            if song_id and title:
                items.append({'position': index, 'platform': 'qq', 'song_id': str(song_id), 'title': title,
                              'artist': ' / '.join(x.get('name','') for x in artists if isinstance(x,dict)),
                              'album': row.get('albumname') or '', 'duration_ms': int(row.get('interval') or 0) * 1000,
                              'cover_url': _qq_cover(row)})
        top = payload.get('topinfo') or {}
        return {'provider': 'qq', 'playlist_id': f'chart:{chart_id}', 'title': top.get('ListName') or f'QQ 榜单 {chart_id}',
                'description': top.get('info') or '', 'cover_url': str(top.get('pic_v12') or top.get('pic') or '').replace('http:', 'https:'),
                'owner': 'QQ 音乐排行榜', 'items': items}

    async def categories(self) -> list[dict[str, Any]]:
        url = 'https://c.y.qq.com/splcloud/fcgi-bin/fcg_get_diss_tag_conf.fcg'
        params = {'format': 'json', 'inCharset': 'utf8', 'outCharset': 'utf-8',
                  'platform': 'yqq.json', 'needNewCode': 0}
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            response = await client.get(url, params=params, headers={'User-Agent': 'Mozilla/5.0', 'Referer': 'https://y.qq.com/'})
        if response.status_code >= 400:
            raise PlatformError(f'qq_categories_http_{response.status_code}')
        rows = (response.json().get('data') or {}).get('categories') or []
        result = []
        for group in rows if isinstance(rows, list) else []:
            if not isinstance(group, dict):
                continue
            items = []
            for row in group.get('items') or []:
                if not isinstance(row, dict) or row.get('categoryId') is None:
                    continue
                items.append({'id': str(row['categoryId']), 'name': html.unescape(str(row.get('categoryName') or ''))})
            if items:
                result.append({'name': str(group.get('categoryGroupName') or '其他'), 'items': items})
        return result

    async def plaza(self, category: str = '', offset: int = 0, limit: int = 30,
                    sort: str = 'hot') -> list[dict[str, Any]]:
        url = self.API
        tag_id = 10000000
        if category and category != '全部':
            if str(category).isdigit():
                tag_id = int(category)
            else:
                aliases = {'华语': '国语', '欧美': '英语', '驾车': '开车', 'R&B': 'R&B'}
                wanted = aliases.get(category, category)
                groups = await self.categories()
                match = next((item for group in groups for item in group['items'] if item['name'] == wanted), None)
                if match:
                    tag_id = int(match['id'])
        request_data = {'comm': {'cv': 1602, 'ct': 20}, 'playlist': {
            'method': 'get_playlist_by_tag',
            'param': {'id': tag_id, 'sin': offset, 'size': limit,
                      'order': 2 if sort == 'latest' else 5,
                      'cur_page': offset // max(1, limit) + 1},
            'module': 'playlist.PlayListPlazaServer'}}
        params = {'loginUin': 0, 'hostUin': 0, 'format': 'json', 'inCharset': 'utf-8', 'outCharset': 'utf-8',
                  'notice': 0, 'platform': 'wk_v15.json', 'needNewCode': 0, 'data': __import__('json').dumps(request_data, ensure_ascii=False)}
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            response = await client.get(url, params=params, headers={'User-Agent': 'Mozilla/5.0', 'Referer': 'https://y.qq.com/'})
            if response.status_code >= 400: raise PlatformError(f'qq_plaza_http_{response.status_code}')
            data = response.json()
        root = (data.get('playlist') or {}).get('data') or {}
        rows = root.get('v_playlist') or []
        result=[]
        for x in rows if isinstance(rows,list) else []:
            if not isinstance(x,dict) or not x.get('tid'): continue
            creator=x.get('creator_info') or {}
            result.append({'provider':'qq','playlist_id':str(x['tid']),'title':x.get('title') or '',
                 'description':x.get('desc') or '', 'cover_url':x.get('cover_url_medium') or x.get('cover_url_big'),
                 'owner':creator.get('nick','') if isinstance(creator,dict) else '',
                 'item_count':x.get('song_count') or len(x.get('song_ids') or []),
                 'play_count':x.get('access_num') or x.get('listen_num') or 0,
                 'update_time':x.get('modify_time') or x.get('create_time')})
        return result

    async def search_playlists(self, keyword: str, page: int = 1,
                               page_size: int = 30) -> dict[str, Any]:
        page = max(1, int(page))
        page_size = min(100, max(1, int(page_size)))
        param = {
            'search_id': str(int(time.time() * 1000000)),
            'remoteplace': 'search.android.keyboard', 'query': keyword,
            'search_type': 3, 'num_per_page': page_size, 'page_num': page,
            'highlight': 0, 'nqc_flag': 0, 'page_id': 1, 'grp': 1,
        }
        data = await self.request(
            'DoSearchForQQMusicLite', 'music.search.SearchCgiService', param,
        )
        body = data.get('body') or {}
        rows = body.get('item_songlist') or []
        items = []
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict) or not row.get('dissid'):
                continue
            items.append({
                'provider': 'qq', 'playlist_id': str(row['dissid']),
                'title': _plain_text(row.get('dissname')),
                'description': _plain_text(row.get('description') or row.get('subhead')),
                'cover_url': str(row.get('logo') or '').replace('http:', 'https:') or None,
                'owner': _plain_text(row.get('nickname')),
                'item_count': int(row.get('songnum') or 0),
                'play_count': int(row.get('listennum') or 0),
                'update_time': row.get('modifytime') or row.get('createtime'),
            })
        meta = data.get('meta') or {}
        total = int(meta.get('sum') or len(items))
        return {'items': items, 'total': total,
                'has_more': page * page_size < total}

    async def daily(self) -> list[dict[str, Any]]:
        cookies = self._cookies(); uin = self.account_id()
        key = cookies.get('qm_keyst') or cookies.get('qqmusic_key') or ''
        if not uin or not key:
            raise PlatformError('QQ 音乐尚未登录，无法获取账号专属日推')
        comm = {'ct': 19, 'cv': 0, 'format': 'json', 'inCharset': 'utf-8', 'outCharset': 'utf-8',
                'notice': 0, 'platform': 'yqq.json', 'needNewCode': 1, 'uin': uin, 'authst': key}
        if str(cookies.get('tmeLoginType') or '').isdigit():
            comm['tmeLoginType'] = int(cookies['tmeLoginType'])
        body = {'comm': comm, 'req_0': {'module': 'music.srfDissInfo.DissInfo', 'method': 'CgiGetDiss',
                'param': {'dirid': 202, 'uinAttached': True, 'enc_host_uin': uin, 'local_time': int(time.time())}}}
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            response = await client.post(self.API, json=body, headers={'Accept':'application/json','Content-Type':'application/json',
                'Cookie':self.cookie,'Origin':'https://y.qq.com','Referer':'https://y.qq.com/','User-Agent':'Mozilla/5.0'})
        if response.status_code >= 400: raise PlatformError(f'qq_daily_http_{response.status_code}')
        payload = response.json(); item = payload.get('req_0') or {}
        if payload.get('code') != 0 or item.get('code') != 0:
            raise PlatformError('QQ 音乐登录状态失效或日推接口拒绝访问')
        rows = (item.get('data') or {}).get('songlist') or []
        result=[]
        for row in rows:
            if not isinstance(row,dict): continue
            singers=row.get('singer') or []; mid=row.get('songmid') or row.get('mid'); title=row.get('songname') or row.get('title')
            album_value = row.get('albumname') or ''
            if not album_value and isinstance(row.get('album'), dict):
                album_value = row['album'].get('name','')
            if mid and title: result.append({'platform':'qq','song_id':str(mid),'mid':str(mid),'title':str(title),
                'artist':' / '.join(x.get('name','') for x in singers if isinstance(x,dict)),
                'album':album_value,
                'duration_ms':int(row.get('interval') or 0)*1000,
                'cover_url':_qq_cover(row)})
        return result

    async def playlists(self, offset: int = 0, limit: int = 100) -> list[dict[str, Any]]:
        uin = self.account_id()
        if not uin:
            raise PlatformError('QQ 音乐尚未登录')
        common = {'format':'json','inCharset':'utf8','outCharset':'utf-8','notice':0,
                  'platform':'yqq.json','needNewCode':0}
        requests = [
            ('created', 'https://c.y.qq.com/rsc/fcgi-bin/fcg_user_created_diss',
             {**common, 'hostUin':0, 'hostuin':uin, 'loginUin':0, 'sin':0, 'size':200, 'g_tk':5381}),
            ('collected', 'https://c.y.qq.com/fav/fcgi-bin/fcg_get_profile_order_asset.fcg',
             {**common, 'ct':20, 'cid':205360956, 'userid':uin, 'reqtype':3, 'sin':0, 'ein':199}),
        ]
        headers = {'Cookie':self.cookie,'Referer':'https://y.qq.com/portal/profile.html','User-Agent':'Mozilla/5.0'}
        rows: list[dict[str, Any]] = []
        errors: list[str] = []
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            for kind, url, params in requests:
                try:
                    response = await client.get(url, params=params, headers=headers)
                    if response.status_code >= 400:
                        raise PlatformError(f'qq_{kind}_playlists_http_{response.status_code}')
                    data = response.json()
                    if data.get('code') not in (None, 0):
                        raise PlatformError(f"qq_{kind}_playlists_api_{data.get('code')}")
                    root = data.get('data') or {}
                    payload = root.get('disslist') if kind == 'created' else root.get('cdlist')
                    rows.extend(x for x in payload or [] if isinstance(x, dict))
                except Exception as exc:
                    errors.append(str(exc))
        if not rows and errors:
            raise PlatformError('; '.join(errors))
        result: list[dict[str, Any]] = []
        seen: set[str] = set()
        for row in rows:
            playlist_id = row.get('dissid') or row.get('tid') or row.get('id')
            if not playlist_id or str(playlist_id) in seen:
                continue
            seen.add(str(playlist_id))
            cover_url = row.get('logo') or row.get('diss_cover') or row.get('cover') or row.get('imgurl')
            if not cover_url or str(cover_url).startswith('?'):
                cover_url = ('https://y.gtimg.cn/mediastyle/y/img/cover_love_300.jpg'
                             if (row.get('dissname') or row.get('diss_name') or row.get('name')) == '我喜欢'
                             else None)
            result.append({'provider':'qq','playlist_id':str(playlist_id),
                'title':row.get('dissname') or row.get('diss_name') or row.get('name') or '',
                'description':row.get('desc') or row.get('description') or '',
                'cover_url':cover_url,
                'owner':row.get('nickname') or '',
                'item_count':row.get('songnum') or row.get('song_cnt') or row.get('trackCount') or 0})
        return result[max(0, offset):max(0, offset) + max(1, limit)]
