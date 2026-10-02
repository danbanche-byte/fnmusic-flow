import os
from enum import StrEnum
from pydantic import BaseModel, Field

# ── 统一音质层（2026-09-29 从 worker.py 上移，供全模块共用） ─────────────
# 用户诉求：**有损是无损都拿不到时的最后选择**。历史上 quality 默认硬编码
# '320k'，导致明明有无损音源也只下 mp3。
DEFAULT_QUALITY = (os.getenv('FNMUSIC_DEFAULT_QUALITY', 'hires').strip() or 'hires')

# 无损（含高解析）音质集合 —— 决定落盘扩展名，也决定「是否算无损」。
# ⚠️ 旧代码只判断 {'flac','lossless','hires'}，漏了 master/atmos*/flac24bit。
LOSSLESS_QUALITIES = {
    'master', 'atmos_plus', 'atmos', 'hires',
    'flac24bit', 'flac', 'lossless', 'sq', 'alac', 'ape', 'wav',
}

# 从高到低的降级顺序（与 lx_source.QUALITY_ORDER 保持一致）
QUALITY_ORDER = [
    'master', 'atmos_plus', 'atmos', 'hires', 'flac24bit', 'flac', '320k', '128k',
]


def is_lossless_quality(quality: object) -> bool:
    """该音质档位是否属于无损（含高解析）。"""
    return str(quality or '').strip().lower() in LOSSLESS_QUALITIES


def quality_extension(quality: object) -> str:
    """按音质档位给出正确的文件扩展名：无损 .flac，有损 .mp3。"""
    return '.flac' if is_lossless_quality(quality) else '.mp3'


class SourceKind(StrEnum):
    # 原生 lx-music 自定义源脚本（globalThis.lx 回调型），交给内置 Node 宿主执行
    LX_CUSTOM = "lx_custom"
    # 私有 REST 协议：GET {API_URL}/url，鉴权 X-API-Key
    LX_SCRIPT = "lx_script"
    # 自建中转 API 协议：POST {API_URL}/music/url，鉴权 X-Api-Key
    LX_API = "lx_api"
    # 标准 LX Server 协议：GET {url}/api/music/*，鉴权 x-user-token
    LX_SERVER = "lx_server"


class SourceDefinition(BaseModel):
    name: str = Field(min_length=1, max_length=80)
    kind: SourceKind = SourceKind.LX_SCRIPT
    endpoint: str | None = None
    sort_order: int = 100
    script_path: str | None = None
    script_content: str | None = None
    enabled: bool = True


class TrackQuery(BaseModel):
    platform: str
    song_id: str
    # 2026-09-28：默认从最高音质档位开始请求（hires），音源不支持时由
    # lx_source.QUALITY_ORDER 逐级降级到 flac/320k/128k。
    # 旧默认 '320k' 会让「明明有无损音源」也直接下 mp3。
    quality: str = "hires"

