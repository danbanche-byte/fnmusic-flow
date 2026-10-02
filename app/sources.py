from __future__ import annotations

import json
from datetime import datetime, timezone
import asyncio
import hashlib

from .lx_source import (LXCustomSourceRuntime, LXScriptParser, LXSourceRuntime,
                        LXServerSourceRuntime, MusicApiSourceRuntime, ParsedLXScript,
                        detect_protocol, extract_url_candidates, looks_like_lx_custom_script,
                        parse_lx_custom_meta)
from .models import SourceDefinition, TrackQuery

# 需要把服务地址与令牌存成 JSON 的协议（脚本内容本身没有可复用的信息）
ENDPOINT_KINDS = ('lx_server', 'lx_api')

KIND_LABELS = {'lx_custom': 'lx-music 自定义源', 'lx_server': 'LX Server',
               'lx_api': 'API 协议', 'lx_script': '私有 REST'}


class SourceRegistry:
    """音源注册表：多协议适配、健康探测、故障切换排序、启用/停用/删除管理。"""

    # 故障切换排序权重：可用的排前面，故障的排最后
    STATUS_RANK = {'ready': 0, 'unknown': 1, 'timeout': 2, 'forbidden': 3, 'error': 4}

    def __init__(self) -> None:
        self._sources: dict[str, tuple[SourceDefinition, LXSourceRuntime | LXServerSourceRuntime]] = {}
        self._health_cache: dict[str, dict] = {}

    # ----------------------------------------------------------------- 注册
    async def register_lx_script(self, content: str, name: str | None = None, probe: bool = True,
                                 sort_order: int = 100) -> SourceDefinition:
        """导入音源 JS。probe=True 时自动探测后端协议（私有 REST / 标准 LX Server /
        ikun 自建 API），探测失败时按私有协议注册（保持旧行为，之后 health 会给出真实状态）。
        声明式解析失败（无 API_URL）时，从脚本中提取候选地址逐个探测。
        如果脚本本身是「lx-music 自定义源脚本」（globalThis.lx 回调型），则直接交给
        内置 Node 宿主执行，不做任何协议猜测 —— 这类脚本在 lx-music 里能跑就能在这里跑。"""
        if looks_like_lx_custom_script(content):
            return await self.register_lx_custom(content, name, sort_order)
        try:
            parsed = LXScriptParser().parse(content)
        except ValueError:
            parsed = None
        if parsed is None:
            for url in extract_url_candidates(content):
                detected = await detect_protocol(url, None)
                if detected:
                    kind, runtime = detected
                    if kind in ENDPOINT_KINDS:
                        script_content = json.dumps({'url': url, 'token': None}, ensure_ascii=False)
                        return self._register(kind, name or self._auto_name(kind, url), script_content,
                                              runtime, url, sort_order)
                    script = f'// fnmusic-flow 导入（地址探测自音源脚本）\nconst API_URL = "{url}";\nconst API_KEY = "";\n'
                    return self._register('lx_script', name or 'LX 音源', script, runtime, url, sort_order)
            raise ValueError('未识别的音源格式：脚本中没有 API_URL 声明，也未探测到可用的音源服务地址。'
                             '支持声明 API_URL 的音源 JS，或在「在线添加」里直接填服务地址接入。')
        if name:
            parsed = ParsedLXScript(parsed.api_url, parsed.api_key, parsed.qualities, name, parsed.version)
        runtime = LXSourceRuntime(parsed)
        kind = 'lx_script'
        script_content = content
        if probe:
            detected = await detect_protocol(parsed.api_url, parsed.api_key)
            if detected:
                kind, runtime = detected
                if kind in ENDPOINT_KINDS:
                    # 这类脚本只是服务端中转的薄壳，真正需要落库的是地址与令牌
                    script_content = json.dumps({'url': parsed.api_url, 'token': parsed.api_key},
                                                ensure_ascii=False)
        return self._register(kind, parsed.name, script_content, runtime, parsed.api_url, sort_order)

    async def register_lx_custom(self, content: str, name: str | None = None, sort_order: int = 100,
                                 load: bool = True) -> SourceDefinition:
        """注册「原生 lx-music 自定义源脚本」。

        脚本内容原样保存，执行交给容器内的 Node 宿主（app/lxnode/host.mjs）：
        宿主提供 globalThis.lx 全套 API，脚本逻辑与在 lx-music 客户端里完全一致。
        同一份脚本重复导入是原地更新，不会产生重复条目。
        """
        meta = parse_lx_custom_meta(content)
        display = (name or '').strip() or meta.get('name') or 'LX 自定义源'
        script_id = hashlib.md5((content or '').encode('utf-8')).hexdigest()[:20]
        # 脚本声明了 API_URL 就把它当展示地址（同地址的更新版脚本会原地覆盖）
        try:
            api_url = LXScriptParser().parse(content).api_url
        except ValueError:
            api_url = None
        endpoint = (api_url or f'lx-custom://{script_id}').rstrip('/')
        runtime = LXCustomSourceRuntime(script_id, content, display, endpoint=api_url)
        definition = self._register('lx_custom', display, content, runtime, endpoint, sort_order)
        if load:
            try:
                await runtime.ensure_loaded()
            except Exception:
                # 宿主未就绪 / 脚本暂时初始化失败都不影响入库，健康探测会给出真实状态
                pass
        return definition

    @staticmethod
    def _auto_name(kind: str, url: str) -> str:
        host = url.split('://', 1)[-1]
        return {'lx_server': f'LX Server ({host})', 'lx_api': f'API 音源 ({host})'}.get(kind, 'LX 音源')

    def register_stored(self, kind: str, url: str, token: str | None = None, name: str | None = None,
                        sort_order: int = 100) -> SourceDefinition:
        """按已知协议恢复一个音源（启动时从数据库回读，或在线添加时指定协议）。"""
        url = url.rstrip('/')
        runtime = (LXServerSourceRuntime(url, token) if kind == 'lx_server'
                   else MusicApiSourceRuntime(url, token) if kind == 'lx_api'
                   else LXSourceRuntime(ParsedLXScript(url, token, {})))
        script_content = json.dumps({'url': url, 'token': token}, ensure_ascii=False)
        return self._register(kind, name or self._auto_name(kind, url), script_content, runtime, url, sort_order)

    def register_lx_server(self, url: str, token: str | None = None, name: str | None = None,
                           sort_order: int = 100) -> SourceDefinition:
        """直连一个标准 LX Server（无需 JS 文件）。script_content 存 JSON（含令牌，API 响应中已排除）。"""
        return self.register_stored('lx_server', url, token, name, sort_order)

    async def register_direct(self, url: str, token: str | None = None, name: str | None = None,
                              sort_order: int = 100) -> SourceDefinition:
        """在线添加一个音源服务地址，自动探测协议后注册。"""
        detected = await detect_protocol(url, token)
        if not detected:
            raise ValueError('无法连接该地址：未响应私有 REST（GET /url）、标准 LX Server（GET /api/music/url）'
                             '或 API 协议（POST /music/url），请检查地址与令牌')
        kind, runtime = detected
        url = url.rstrip('/')
        if kind in ENDPOINT_KINDS:
            return self.register_stored(kind, url, token, name, sort_order)
        script = f'// fnmusic-flow 在线添加\nconst API_URL = "{url}";\nconst API_KEY = "{token or ""}";\n'
        return self._register('lx_script', name or '在线音源', script, runtime, url, sort_order)

    def _register(self, kind: str, name: str, script_content: str, runtime, endpoint: str,
                  sort_order: int = 100) -> SourceDefinition:
        endpoint = endpoint.rstrip('/')
        # 同一服务地址重复添加时原地更新，不产生重复条目
        reused = next((definition.name for definition, _ in self._sources.values()
                       if (definition.endpoint or '').rstrip('/') == endpoint), None)
        resolved_name = reused or self._unique_name(name)
        definition = SourceDefinition(name=resolved_name, kind=kind, endpoint=endpoint,
                                      sort_order=sort_order, script_content=script_content)
        if reused:
            definition.sort_order = self._sources[reused][0].sort_order
        self._sources[definition.name] = (definition, runtime)
        self._health_cache.pop(definition.name, None)
        return definition

    def _unique_name(self, base: str) -> str:
        """多个音源同名时追加序号，保证批量导入不会互相覆盖。"""
        if base not in self._sources:
            return base
        index = 2
        while f'{base} ({index})' in self._sources:
            index += 1
        return f'{base} ({index})'

    # ----------------------------------------------------------------- 管理
    def list(self) -> list[SourceDefinition]:
        return [item[0] for item in self._sources.values()]

    def get(self, name: str):
        return self._sources.get(name)

    def remove(self, name: str) -> bool:
        existed = self._sources.pop(name, None) is not None
        self._health_cache.pop(name, None)
        return existed

    def set_enabled(self, name: str, enabled: bool) -> bool:
        item = self._sources.get(name)
        if not item:
            return False
        item[0].enabled = bool(enabled)
        return True

    def reorder(self, names: list[str]) -> list[str]:
        """按给定顺序重排音源优先级（每次步长 10），未列出的保持在末尾。"""
        ordered: list[str] = []
        for index, name in enumerate(names):
            item = self._sources.get(name)
            if item:
                item[0].sort_order = (index + 1) * 10
                ordered.append(name)
        tail = (len(ordered) + 1) * 10
        for definition, _ in self._sources.values():
            if definition.name not in ordered:
                definition.sort_order = tail
                tail += 10
        return self.ordered_names(only_enabled=False)

    def ordered_names(self, only_enabled: bool = True) -> list[str]:
        """故障切换顺序：健康可用的在前，随后按用户优先级权重与名称。"""
        rows = []
        for definition, _ in self._sources.values():
            if only_enabled and not definition.enabled:
                continue
            status = self._health_cache.get(definition.name, {}).get('status', 'unknown')
            rows.append((definition.name, self.STATUS_RANK.get(status, 1), definition.sort_order, status))
        rows.sort(key=lambda row: (row[1], row[2], row[0]))
        return [row[0] for row in rows]

    def status_of(self, name: str) -> str:
        return self._health_cache.get(name, {}).get('status', 'unknown')

    def seed_health(self, name: str, status: str | None, detail: str | None, last_check: str | None) -> None:
        """重启后从数据库回填上一次的探测结果，避免启动即乱序。"""
        item = self._sources.get(name)
        if not item:
            return
        self._health_cache[name] = {
            'name': name, 'enabled': item[0].enabled,
            'status': status or 'unknown', 'detail': detail or '尚未检测',
            'last_check': last_check,
        }

    async def resolve_first(self, query: TrackQuery) -> tuple[str, str]:
        """按故障切换顺序依次尝试，返回 (地址, 音源名)；全部失败时抛出汇总错误。"""
        errors: list[str] = []
        for name in self.ordered_names():
            try:
                return await self.resolve(name, query), name
            except Exception as exc:
                errors.append(f'{name}: {str(exc)[:100]}')
        if not errors:
            raise RuntimeError('尚未配置可用音源，请先在「音源管理」中添加')
        raise RuntimeError('没有音源能解析该歌曲：' + '；'.join(errors[-3:]))

    def _note(self, name: str, status: str, detail: str) -> None:
        cached = self._health_cache.get(name, {})
        self._health_cache[name] = {**cached, 'status': status, 'detail': detail,
                                    'last_check': datetime.now(timezone.utc).isoformat()}

    @staticmethod
    def _classify_failure(text: str) -> str | None:
        """区分「音源本身故障」与「单曲解析失败」。单曲失败不应让音源被降级，避免顺序抖动。"""
        lowered = text.lower()
        # 宿主不可用 / 脚本初始化失败：一定是音源层面的故障
        if 'lx_host_error' in lowered:
            return 'error'
        # 鉴权失败（密钥过期、令牌无效）也是音源故障，故障切换应该跳过它
        if any(key in text for key in ('鉴权', '密钥', '令牌', '已失效', '已过期')) \
                or any(key in lowered for key in ('unauthorized', 'invalid key', 'apikey', 'api key')):
            return 'forbidden'
        if text.startswith('source_error:') and 'lx_http_' not in lowered:
            return None  # 歌曲层面的失败，保持原状态
        if '403' in lowered or 'forbidden' in lowered or '401' in lowered:
            return 'forbidden'
        if 'timeout' in lowered or 'timed out' in lowered or '超时' in text:
            return 'timeout'
        if any(key in lowered for key in ('connect', 'network:', 'lx_http_', 'name or service')):
            return 'error'
        return None

    # ----------------------------------------------------------------- 调用
    async def resolve(self, name: str, query: TrackQuery) -> str:
        item = self.get(name)
        if not item:
            raise KeyError(name)
        try:
            url = await item[1].resolve(query)
        except Exception as exc:
            status = self._classify_failure(str(exc))
            if status:
                self._note(name, status, str(exc)[:200])
            raise
        self._note(name, 'ready', '最近一次解析成功')
        return url

    async def search(self, name: str, keyword: str, platform: str = 'netease') -> list[dict]:
        item = self.get(name)
        if not item: raise KeyError(name)
        return await item[1].search(keyword, platform)

    async def health(self) -> list[dict]:
        now = datetime.now(timezone.utc).isoformat()

        async def check(definition: SourceDefinition, runtime) -> dict:
            result = await runtime.health()
            result.update({'name': definition.name, 'enabled': definition.enabled, 'last_check': now})
            self._health_cache[definition.name] = result
            return result

        return list(await asyncio.gather(*(check(definition, runtime)
                                           for definition, runtime in self._sources.values())))

    async def probe(self, only_enabled: bool = False, persist=None, names: list[str] | None = None) -> list[dict]:
        """主动健康探测（多源自动测试）。persist(name, result) 可用于写回数据库。"""
        targets = [item for item in self._sources.values()
                   if (names is None or item[0].name in names) and (not only_enabled or item[0].enabled)]
        now = datetime.now(timezone.utc).isoformat()

        async def check(definition: SourceDefinition, runtime) -> dict:
            try:
                result = await runtime.health()
            except Exception as exc:
                result = {'status': 'error', 'detail': str(exc)[:200]}
            result.update({'name': definition.name, 'enabled': definition.enabled, 'last_check': now})
            self._health_cache[definition.name] = result
            if persist:
                try:
                    persist(definition.name, result)
                except Exception:
                    pass
            return result

        return list(await asyncio.gather(*(check(definition, runtime) for definition, runtime in targets)))

    def runtime(self, name: str):
        item = self._sources.get(name)
        return item[1] if item else None

    def health_snapshot(self) -> list[dict]:
        """返回最近一次探测结果，不发起任何网络请求（刷新页面不再打音源网络）。"""
        rows = []
        for definition, _ in self._sources.values():
            cached = dict(self._health_cache.get(definition.name) or {})
            rows.append({**cached, 'name': definition.name, 'enabled': definition.enabled,
                         'status': cached.get('status', 'unknown'),
                         'detail': cached.get('detail') or '尚未检测',
                         'last_check': cached.get('last_check')})
        return rows
