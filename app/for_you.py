"""「为我推荐」：基于口味画像的双平台公开歌单发现、两级筛选与每日轮换。

边界约定（00-READ-FIRST-ZCODE.md / 01-需求与方案框架.md）：

- 只推荐网易云音乐 / QQ 音乐的**真实公开歌单**，候选发现与曲目详情一律
  复用 ``app/platforms.py`` 的现有连接器；不拼装自建歌单，不新增 Cookie
  保存逻辑，也不在推荐链路里记录 Cookie。
- 口味画像 ``/config/taste-profile.json`` 是私密运行时配置：本模块不打印、
  不写日志、不落盘画像内容；错误信息只包含字段名与失败原因，不包含画像文本。
- 换血语义：先在内存中完成整轮抓取/评分/淘汰，再用**单个 SQLite 事务**
  完成入池与出池；上游异常、画像非法、候选为空时旧池原样保留。
- 每天北京时间 06:00（``FOR_YOU_ROTATION_TIME`` 可配置）触发一次轮换，
  全局每日换入上限 ``daily_rotation``（默认 15，手动刷新共享该预算）；
  淘汰条目进入 ``for_you_history`` 冷却 14 天（可配置），用户标记
  「不感兴趣」的进入 ``for_you_blacklist``，可通过 API 恢复。

评分模型（初始参数，均可通过画像的 ``for_you`` 配置节调整）：

- 曲目级判定（纯函数 ``judge_track``）输出四类信号：硬约束、正向口味、
  负向口味、版本标记。
- ``coverage`` = 四类信号任一命中的曲目占比（可判定覆盖率），与
  ``taste_rate``（正向命中占比）不重复计分：仅命中版本标记的曲目只进入
  coverage 分母，不进入 taste_rate 分子。
- 曲目层得分 ``deep_score = 0.7*taste_rate + 0.2*(1-hard_rate) + 0.1*coverage
  - 0.3*dislike_rate``。
- 粗筛 ``coarse_score = 0.45*关键词 + 0.35*热度 + 0.2*新鲜度``（纯元数据，
  不发额外请求）；总分 ``total = 0.4*coarse + 0.6*deep``。
- 淘汰规则：``hard_rate > 0.15`` 或硬命中数 ≥3 淘汰；可读曲目 ≥20 且
  ``taste_rate < 0.2`` 淘汰；曲目数 <10 或 >2000 粗筛直接淘汰；QQ/网易云
  详情返回 0 首时该候选跳过并记录，不算成功歌单。
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import re
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from .platforms import NeteaseConnector, QQMusicConnector, PlatformError

try:
    from opencc import OpenCC
    _TO_SIMPLIFIED = OpenCC('t2s')
except ImportError:  # 与 track_match.py 相同的降级策略：开发环境缺 opencc 仍可运行
    _TO_SIMPLIFIED = None


BEIJING = timezone(timedelta(hours=8))
SUPPORTED_PROVIDERS = ('netease', 'qq')
SUPPORTED_SCHEMA_VERSION = 1
STALE_RUN_SECONDS = 3600              # 'running' 超过 1 小时视为进程中断遗留/僵死
SCHEDULED_RETRY_SECONDS = 3600        # 当日失败后调度重试的最小间隔
DEFAULT_PROFILE_PATH = '/config/taste-profile.json'

DEFAULT_CONFIG: dict[str, Any] = {
    'max_pool': 50,
    'daily_rotation': 15,
    'deep_check_top_n': 50,
    'cooldown_days': 14,
    'max_seed_keywords': 10,
    'playlists_per_keyword': 15,
    'search_page_size': 30,
    'min_track_count': 10,
    'max_track_count': 2000,
    # 质量与多样性（修订要求 2026-09-27，均为首轮实验默认值）
    'artist_focused_daily_cap': 5,        # 每日新名额中歌手专题型上限
    'same_dominant_artist_daily_cap': 2,  # 同一主导歌手每日最多入选数
    'artist_dominance_ratio': 0.6,        # 单歌手占可读曲目 ≥ 此比例 → 歌手主导
    'variety_min_distinct_artists': 5,    # 综合型：不同歌手数下限
    'variety_max_top_artist_ratio': 0.4,  # 综合型：第一歌手占比上限
    'diversity_selection_weight': 0.15,   # 同歌手/同类型的边际分数惩罚
    'coarse_soft_penalty_words': ['bgm', '神曲', '短视频'],  # 宽泛词：降权不拒绝
    'thresholds': {'hard_rate_limit': 0.15, 'hard_hits_limit': 3,
                   'taste_rate_limit': 0.20, 'taste_rate_min_tracks': 20},
    'weights': {'taste': 0.7, 'not_hard': 0.2, 'coverage': 0.1, 'dislike_penalty': 0.3,
                'coarse': 0.4, 'deep': 0.6,
                'keyword': 0.45, 'popularity': 0.35, 'freshness': 0.2},
    'request_concurrency': {'netease': 4, 'qq': 3},
    'platform_balance': True,
    'seed_keywords': [],
    'hard_keywords': [],
    'negative_keywords': [],
    'negative_artists': [],
    'positive_keywords': [],
}

# ---------------------------------------------------------------------------
# 文本规范化（与 track_match.normalize_text 同一策略：NFKC + casefold + 繁→简）
# ---------------------------------------------------------------------------

def normalize_text(value: object) -> str:
    """匹配用规范化：全角/繁体/大小写归一，保留空格供英文词边界匹配。"""
    text = unicodedata.normalize('NFKC', str(value or '')).casefold()
    if _TO_SIMPLIFIED:
        text = _TO_SIMPLIFIED.convert(text)
    text = re.sub(r'[’‘`]', "'", text)
    return re.sub(r'\s+', ' ', text).strip()


def compact_text(value: object) -> str:
    """中文子串匹配用：去掉全部非字母数字字符（含空格）。"""
    return ''.join(ch for ch in normalize_text(value) if ch.isalnum())


# 标题主体 = 去掉括号版本说明后的部分（「Faded (Remix)」→「Faded」）
_TITLE_BRACKETS_RE = re.compile(r"[\(\[\{（【《].*?[\)\]\}）】》]")


def _latin_keyword_re(keyword: str) -> re.Pattern:
    """英文关键词按「两侧非 ASCII 字母数字」定界，兼容中英混排。

    用自定义边界而非 ``\\b``：Python 的 ``\\w`` 把汉字视为词字符，
    ``\\bdj\\b`` 匹配不到 "DJ版"。改成前后不能紧邻 [a-z0-9] 后，
    "dj版" 命中而 "discover" 中的 "cover" 不命中。
    """
    return re.compile(rf'(?<![a-z0-9]){re.escape(keyword)}(?![a-z0-9])')


@dataclass(frozen=True)
class Keyword:
    text: str      # 展示用原文（仅进入业务库与 UI，不进日志）
    norm: str      # 规范化小写形式
    compact: str   # 中文子串匹配形式
    pattern: re.Pattern | None = None  # 含英文字母时使用词边界 pattern
    exact: bool = False  # 作品级匹配：仅与标题/专辑主体等值命中，不做子串


def _keyword(text: str, exact: bool = False) -> Keyword:
    norm = normalize_text(text)
    has_latin = any('a' <= ch <= 'z' for ch in norm)
    return Keyword(str(text).strip(), norm, compact_text(text),
                   _latin_keyword_re(norm) if has_latin else None, exact)


def _keyword_list(values: object, exact: bool = False) -> tuple[Keyword, ...]:
    if isinstance(values, (str, Keyword)):
        values = [values]
    if not isinstance(values, (list, tuple)):
        return ()
    seen: set[str] = set()
    result = []
    for value in values:
        text = str(getattr(value, 'text', value) or '').strip()
        if len(text) < 2 or text in seen:
            continue
        seen.add(text)
        result.append(_keyword(text, exact))
    return tuple(result)


# 硬约束类别：固定四类（产品硬约束），关键词按中英文与常见版本词覆盖。
# 匹配范围：歌名 + 专辑；DJ/喊麦/车载另查歌手名（"DJ XXX" 类艺人即改编号）。
HARD_CATEGORIES: dict[str, dict[str, Any]] = {
    'instrumental': {
        'label': '纯音乐/器乐',
        'keywords': ['伴奏', '纯音乐', '无人声', '轻音乐', '钢琴版', '钢琴曲', '钢琴独奏',
                     '钢琴弹奏', '口琴版', '口哨版', '小提琴版', '吉他版', '纯吉他', '萨克斯版',
                     '八音盒', '音乐盒', '背景音乐', '纯器乐', '器乐版', '演奏版',
                     'instrumental', 'piano ver', 'piano version', 'piano solo', 'piano cover',
                     'off vocal', 'karaoke', 'music box', '8d', 'bgm'],
    },
    'dj': {
        'label': 'DJ/车载/喊麦',
        'keywords': ['dj', 'dj版', '车载', '喊麦', '串烧', '慢摇', '咚鼓', '鼓点版', '嗨曲', '神曲'],
        'match_artist': True,
    },
    'opera': {
        'label': '戏曲',
        'keywords': ['戏曲', '京剧', '越剧', '黄梅戏', '豫剧', '昆曲', '秦腔', '评剧', '粤剧',
                     '晋剧', '梆子', '沪剧', '锡剧', '二人转'],
    },
    'children': {
        'label': '儿歌',
        'keywords': ['儿歌', '童谣', '童歌', '少儿歌曲', '儿童歌曲', '宝宝巴士', 'nursery'],
    },
}

# 版本标记（中性信号，不淘汰）：默认偏录音室原版，但 Live/翻唱演绎好就听。
VERSION_MARKERS = ['live', '现场', '演唱会', '翻唱', 'cover', '深情版', '修音版', 'demo',
                   'remix', 'remaster', '重制', 'acoustic', '不插电', '弹唱']

# 粗筛阶段标题/简介硬排斥词（修订要求 6：只保留「整张歌单主题明确违规」的类型；
# 宽泛词 bgm/神曲/短视频 移入降权表，不再一票否决，由曲目构成在深筛兜底）
_COARSE_REJECT_WORDS = ['dj', 'dj版', '车载', '喊麦', '串烧', '慢摇', '戏曲',
                        '儿歌', '童谣', '纯音乐', '轻音乐', '钢琴曲', '伴奏',
                        '胎教', '助眠', '白噪音', '哄睡']
_COARSE_REJECT = _keyword_list(_COARSE_REJECT_WORDS)
# 宽泛降权词（默认表；可在画像 for_you.coarse_soft_penalty_words 覆盖）
_COARSE_SOFT_DEFAULT = ['bgm', '神曲', '短视频']


# ---------------------------------------------------------------------------
# 画像加载与校验
# ---------------------------------------------------------------------------

class ProfileError(RuntimeError):
    """画像缺失/非法。错误文本只包含字段名，不包含画像内容。"""


def _extract_works(texts: list[str]) -> list[str]:
    works = []
    for text in texts:
        works.extend(m.strip() for m in re.findall(r'《([^《》]{1,40})》', str(text or '')))
    return works


_STRIP_CHARS = ' 　·—\\-–~!！?？。，,、；;：:()（）[]【】"\'“”‘’·等'
_NEG_SUFFIXES = ['均无感', '无感', '不听', '不喜欢', '不循环', '不反感', '不接受', '排斥',
                 '除外', '均不', '不再听', '避雷']
_NEG_PREFIXES = ['不听', '不喜欢', '排斥', '禁止', '排除']
_TOKEN_SUFFIXES = ['系列', '向歌曲', '风歌曲', '歌曲', '向', 'ost', 'OST']
# 枚举分隔符（含破折号与括号；括号仅在"整段切分"之外使用）
_ENUM_SPLIT_RE = re.compile(r'[、，,;；/（）()\n—－]')


def _strip_scaffolding(token: str) -> str:
    """去掉画像口语中的评价性前后缀，只留实体词。"""
    token = token.strip(_STRIP_CHARS)
    changed = True
    while changed and token:
        changed = False
        for pre in _NEG_PREFIXES:
            if token.startswith(pre) and len(token) > len(pre):
                token = token[len(pre):].strip(_STRIP_CHARS)
                changed = True
        for suf in _NEG_SUFFIXES:
            if token.endswith(suf) and len(token) > len(suf):
                token = token[:-len(suf)].strip(_STRIP_CHARS)
                changed = True
    changed = True
    while changed and token:
        changed = False
        for suf in _TOKEN_SUFFIXES:
            if token.casefold().endswith(suf.casefold()) and len(token) > len(suf) + 1:
                token = token[:-len(suf)].strip(_STRIP_CHARS)
                changed = True
                break
    return token


_GENERIC_TOKENS = {'均无感', '无感', '不听', '不喜欢', '不循环', '不反感', '除外', '接受', '喜欢',
                   '很多', '有些', '个别', '全部', '所有', '但', '即使', '如', '例如', '包括',
                   '以及', '或者', '首', '首歌', '边界', '参照', '验证', '面广', '会主动听',
                   '主动听', '情感向', '励志向', '玩梗向', '情感', '情绪', '场景', '节奏',
                   '人声', '演绎', '质量', '新歌', '新作', '原版', '年代', '不设限', '偏快',
                   '偏中慢', '平均分布', '整体', '看心情', '切歌', '触发', '核心高频', '高频循环',
                   '近期高频循环', '歌手分层', '歌曲导向', '歌手级喜欢', '广锚点',
                   # 场景/描述词（P1 修复：不得成为歌手或种子）
                   '通勤', '运动', '工作', '学习', '深夜', '独处', '夜晚', '夜间', '睡前',
                   '开车', '驾驶', '旅行', '有力量', '有情感', '治愈', '放松', '燃', '燃的',
                   '安静', '舒缓', '欧美核心', '华语核心', '热门歌手', '名单见', '如何', '示例'}

# 英文实体边界：两侧不能紧邻字母/数字/下划线/点——"genre_preferences" 里的
# "genre"、"preferences" 因此不再被拆出来当歌手。
_LATIN_NAME_RE = re.compile(
    r"(?<![A-Za-z0-9_.])[A-Za-z][A-Za-z0-9.&'’\- ]{1,38}[A-Za-z0-9)](?![A-Za-z0-9_.])")
# 画像字段名/标识符与泛指词，永远不当实体
_LATIN_STOPWORDS = {'genre', 'genres', 'preferences', 'preference', 'version', 'versions',
                    'level', 'notes', 'note', 'tempo', 'vocal', 'vocals', 'mood', 'moods',
                    'era', 'language', 'languages', 'account', 'constraints', 'constraint',
                    'hard', 'soft', 'dislikes', 'dislike', 'list', 'lists', 'ost', 'edm',
                    'bgm', 'dj', 'kpop', 'jpop', 'feat', 'etc'}
_NON_ARTIST_CHARS_RE = re.compile(r'[的了是不都均要括如即很太更最又还再比被把与或及为以在]')
# 含这些子串的枚举词是评价语/描述语/体裁词而非人名（如「歌手无所谓」「舞曲」）
_ARTIST_BAN_SUBSTRINGS = ('歌手', '歌曲', '音乐', '喜欢', '循环', '不听', '演绎', '曲风',
                          '风格', '高频', '核心', '年代', '类型', '节奏', '情绪', '唱腔',
                          '歌', '曲', '风', '舞', '声', '调', '戏', '版', '组', '团', '场景')
# 正向字段里的负向语境（命中后整段不产实体）
_NEG_CUE_RE = re.compile(r'无感|不爱|不喜欢|不循环|不反感|不接受|排斥|除外|均不|不听|避雷|绕开')


def _is_name_like(token: str) -> bool:
    if any(ban in token for ban in _ARTIST_BAN_SUBSTRINGS):
        return False
    if re.search(r'[A-Za-z0-9_]', token):
        return False  # 人名走 _latin_entities；中文人名不含拉丁/数字/下划线
    return 2 <= len(token) <= 6 and not _NON_ARTIST_CHARS_RE.search(token)


# 连接式枚举「A到B / A至B / A以及B」（复审 round1 P2：两个歌手被「到」连成
# 一个短语时，两侧各自通过人名过滤即拆为两个候选，短语本体不成为实体）
_RANGE_TOKEN_RE = re.compile(r'([\u4e00-\u9fa5A-Za-z]{2,6})(?:[到至]|以及)([\u4e00-\u9fa5A-Za-z]{2,6})')


def _artist_candidates(token: str) -> list[str]:
    """人名候选：连接式枚举先拆分，两侧各自通过人名过滤才拆；
    其余情况维持单候选（不合格时返回空）。"""
    matched = _RANGE_TOKEN_RE.fullmatch(token)
    if matched:
        first, second = matched.group(1), matched.group(2)
        if _is_name_like(first) and _is_name_like(second):
            return [first, second]
    return [token] if _is_name_like(token) else []


def _split_segments(text: str) -> list[str]:
    """按枚举分隔符切分，但《》对与其相邻文字保持同段（歌手《作品》不被拆散）。"""
    parts: list[str] = []
    buf: list[str] = []
    depth = 0
    for ch in text:
        if ch == '《':
            depth += 1
            buf.append(ch)
        elif ch == '》':
            depth = max(0, depth - 1)
            buf.append(ch)
        elif depth == 0 and ch in '、，,;；/':
            parts.append(''.join(buf))
            buf = []
        else:
            buf.append(ch)
    parts.append(''.join(buf))
    return [p.strip() for p in parts if p.strip()]


def _latin_entities(text: str) -> list[str]:
    """从一段文字中抽英文实体；字段名/标识符/泛指词被排除。"""
    result = []
    for match in _LATIN_NAME_RE.findall(text):
        token = match.strip(" .&'’-)")
        if len(token) >= 2 and token.casefold() not in _LATIN_STOPWORDS:
            result.append(token)
    return result


def _entities_from_lines(lines: list[str], *, negative: bool = False) -> tuple[list[str], list[str], list[str]]:
    """从画像句子中抽取（艺术家、作品、关键词）三类实体。

    ``negative=True`` 处理 dislikes/hard_constraints（整行负向语境）；
    ``negative=False`` 处理正向字段，此时段内/括注内出现负向语气词
    （无感/不喜欢/不循环…）的整段跳过，避免「…不爱（Perfect、Shallow）」
    之类的叙述被抽成正向实体。

    防污染规则：
    - 《歌手《作品》》形式的枚举只抽作品，歌手名不进入实体（防止核心歌手
      的个别无感作品把歌手整体误杀/误标）。
    - 英文实体两侧不得紧邻下划线/字母数字（字段名 genre_preferences 拆不出
      genre/preferences），且命中字段名/泛指词停用表的一律丢弃。
    - 场景/描述词（通勤、工作、有力量…）与体裁词（舞曲、快歌…）不是人名。
    - 括号内的顿号/逗号枚举才可能是歌手清单；单词括注（亲认/面广）不是。
    - 冒号前主干与去骨架后的短语走关键词通道。
    """
    artists: list[str] = []
    works: list[str] = []
    keywords: list[str] = []
    for line in lines:
        line = str(line or '').strip()
        if not line:
            continue
        pieces = re.split(r'[：:]', line, maxsplit=1)
        head = pieces[0]
        body = pieces[1] if len(pieces) > 1 else None

        # 冒号前主干短语 → 关键词（如「仙剑系列 OST」「港乐快歌经典」）
        for token in _ENUM_SPLIT_RE.split(head):
            token = _strip_scaffolding(token)
            if token and token not in _GENERIC_TOKENS and 2 <= len(token) <= 10 \
                    and not _NON_ARTIST_CHARS_RE.search(token):
                keywords.append(token)

        # 无冒号行：括号枚举与分段的实体从整行提取（如「示例乐队A、示例乐队B、…（歌手级喜欢）」）
        source = line if body is None else body
        # 括号内顿号/逗号枚举 → 歌手清单
        for group in re.findall(r'[（(]([^（）()]{2,200})[)）]', source):
            if not re.search(r'[、，,]', group):
                continue  # 单词括注（如「亲认」「面广」）不是枚举
            if not negative and _NEG_CUE_RE.search(group):
                continue  # 正向字段里的负向括注（…不爱（A、B 均不喜欢））
            for token in _ENUM_SPLIT_RE.split(group):
                if '《' in token or '》' in token:
                    continue  # 作品对由作品通道处理
                token = _strip_scaffolding(token).strip(_STRIP_CHARS)
                if not token or token in _GENERIC_TOKENS:
                    continue
                if re.fullmatch(r"[A-Za-z][A-Za-z0-9 .&'’-]*", token):
                    token = token.strip()
                    if token.casefold() not in _LATIN_STOPWORDS and 2 <= len(token) <= 40:
                        artists.append(token)
                else:
                    artists.extend(_artist_candidates(token))

        # 分段：含《》的段只出作品（歌手《作品》成对样例不抽歌手）；正向字段里
        # 带负向语气的段整段跳过。
        for segment in _split_segments(source):
            if '《' in segment or '》' in segment:
                if negative or not _NEG_CUE_RE.search(segment):
                    works.extend(_extract_works([segment]))
                continue
            if not negative and _NEG_CUE_RE.search(segment):
                continue
            for token in _latin_entities(segment):
                artists.append(token)
            # 去掉英文与未闭合括注后处理中文部分（如「黑豹（歌手级喜欢」→「黑豹」）
            remainder = re.sub(r"[A-Za-z][A-Za-z0-9 .&'’-]*", ' ', segment)
            remainder = re.sub(r'[（(][^（）()]*$', ' ', remainder)
            stripped = _strip_scaffolding(remainder).strip(_STRIP_CHARS)
            if not stripped or stripped in _GENERIC_TOKENS:
                continue
            candidates = _artist_candidates(stripped)
            if candidates:
                artists.extend(candidates)
            elif 2 <= len(stripped) <= 10:
                keywords.append(stripped)
    return artists, works, keywords


def _genre_weight(level: object) -> float:
    return {'核心': 1.0, '高接受': 0.9, '接受': 0.7, '分化': 0.4, '极窄': 0.2}.get(str(level or ''), 0.5)


@dataclass
class TasteProfile:
    path: str
    version: str                      # 文件 sha256 前 12 位，用于运行记录
    raw: dict[str, Any]
    config: dict[str, Any]
    hard: dict[str, tuple[Keyword, ...]]                 # category -> keywords
    hard_labels: dict[str, str]
    hard_match_artist: set[str]
    version_markers: tuple[Keyword, ...]
    negative_artists: tuple[Keyword, ...]
    negative_works: tuple[Keyword, ...]
    negative_keywords: tuple[Keyword, ...]
    core_artists: tuple[Keyword, ...]
    known_artists: tuple[Keyword, ...]
    positive_works: tuple[Keyword, ...]
    positive_genres: tuple[tuple[Keyword, float], ...]
    positive_keywords: tuple[Keyword, ...]
    languages: tuple[str, ...]
    seed_plan: tuple[tuple[str, str], ...]   # (搜索词, 类型)：artist/genre/artist_genre/work/language/custom
    seed_keywords: tuple[str, ...]

    # -- 匹配辅助 ---------------------------------------------------------
    def hits(self, keywords: tuple[Keyword, ...], *texts: str) -> list[Keyword]:
        """关键词命中。``kw.exact`` 的作品级词条要求与某个字段的主体等值：
        整字段或去括号后的主标题（如「Faded (Remix)」→「Faded」），
        而不是子串——「Faded Love」「Alone Again」因此不会误命中。"""
        texts = [str(t) for t in texts if t]
        joined_norm = ' '.join(normalize_text(t) for t in texts)
        joined_compact = compact_text(joined_norm)
        exact_variants: set[str] | None = None
        found = []
        for kw in keywords:
            if kw.exact:
                if exact_variants is None:
                    exact_variants = set()
                    for text in texts:
                        exact_variants.add(compact_text(text))
                        exact_variants.add(compact_text(_TITLE_BRACKETS_RE.sub(' ', text)))
                if kw.compact and kw.compact in exact_variants:
                    found.append(kw)
            elif kw.pattern is not None:
                if kw.pattern.search(joined_norm):
                    found.append(kw)
            elif kw.compact and kw.compact in joined_compact:
                found.append(kw)
        return found


_PROFILE_CACHE: dict[str, tuple[float, int, TasteProfile]] = {}


def parse_profile(raw: object, path: str = '', file_hash: str = '') -> TasteProfile:
    """校验并解析画像 JSON。非法时抛 ProfileError（信息不含画像内容）。"""
    if not isinstance(raw, dict):
        raise ProfileError('profile_invalid: 根节点必须是 JSON 对象')
    schema_version = raw.get('schema_version', 1)
    if not isinstance(schema_version, int) or schema_version > SUPPORTED_SCHEMA_VERSION:
        raise ProfileError(f'profile_invalid: schema_version 不受支持（当前支持 ≤{SUPPORTED_SCHEMA_VERSION}）')
    hard_constraints = raw.get('hard_constraints')
    if not isinstance(hard_constraints, list) or not hard_constraints \
            or not all(isinstance(x, str) for x in hard_constraints):
        raise ProfileError('profile_invalid: hard_constraints 必须是非空字符串数组')
    if not any(isinstance(raw.get(key), list) and raw.get(key)
               for key in ('soft_preferences', 'dislikes', 'genre_preferences', 'language_preferences')):
        raise ProfileError('profile_invalid: 缺少可用的偏好字段'
                           '（soft_preferences/dislikes/genre_preferences/language_preferences）')
    section = raw.get('for_you') if isinstance(raw.get('for_you'), dict) else {}

    def section_dict(key: str) -> dict:
        return section.get(key) if isinstance(section.get(key), dict) else {}

    config: dict[str, Any] = {
        **{k: v for k, v in DEFAULT_CONFIG.items()
           if k not in ('thresholds', 'weights', 'request_concurrency')},
        'thresholds': {**DEFAULT_CONFIG['thresholds'], **section_dict('thresholds')},
        'weights': {**DEFAULT_CONFIG['weights'], **section_dict('weights')},
        'request_concurrency': {**DEFAULT_CONFIG['request_concurrency'], **section_dict('request_concurrency')},
    }
    list_keys = ('seed_keywords', 'hard_keywords', 'negative_keywords', 'negative_artists',
                 'positive_keywords', 'coarse_soft_penalty_words')
    for key in list_keys:
        if isinstance(section.get(key), list):
            config[key] = [str(x).strip() for x in section[key] if str(x).strip()]
    # 标量配置覆盖（数值/布尔键：多样性阈值、配额、开关等）
    for key, default_value in DEFAULT_CONFIG.items():
        if key not in section or isinstance(section[key], (dict, list)):
            continue
        if isinstance(default_value, bool):
            if isinstance(section[key], bool):
                config[key] = section[key]
            continue
        if isinstance(default_value, (int, float)):
            try:
                config[key] = type(default_value)(section[key])
            except (TypeError, ValueError):
                continue

    # -- 实体抽取 ---------------------------------------------------------
    # 结构化字段（genre_preferences）优先；叙述文本（soft_preferences/dislikes）
    # 只提供实体与关键词，且经过防污染过滤（字段名/场景词/负向语境不入正向）。
    dislikes = [str(x) for x in raw.get('dislikes') or [] if isinstance(x, str)]
    soft = [str(x) for x in raw.get('soft_preferences') or [] if isinstance(x, str)]
    neg_artists, neg_works, neg_keywords = _entities_from_lines(dislikes, negative=True)
    pos_artists, pos_works, pos_keywords = _entities_from_lines(soft, negative=False)

    genres: list[tuple[str, float]] = []
    for entry in raw.get('genre_preferences') or []:
        if not isinstance(entry, dict):
            continue
        weight = _genre_weight(entry.get('level'))
        for token in re.split(r'[/、，,]', str(entry.get('genre') or '')):
            token = token.strip()
            if len(token) >= 2:
                genres.append((token, weight))

    languages: list[str] = []
    for line in raw.get('language_preferences') or []:
        if isinstance(line, str):
            for token in re.findall(r'[\u4e00-\u9fa5]{2,4}|[A-Za-z][A-Za-z-]{1,10}', line):
                if token in {'中文', '国语', '华语', '粤语', '英语', '日语', '韩语'}:
                    languages.append('国语' if token in ('中文', '华语') else token)

    # -- 搜索种子：显式配置优先，否则按类型配比生成（修订要求 5）----------
    # 覆盖核心歌手、曲风/语言、核心歌手+曲风组合、画像喜欢的作品，
    # 避免候选来源只由前几位核心歌手决定；类型随候选写入诊断。
    seed_plan: list[tuple[str, str]] = [(str(x).strip(), 'custom') for x in config.get('seed_keywords') or []]
    if not seed_plan:
        genre_tokens = [token for token, _ in sorted(genres, key=lambda x: -x[1])]
        core_texts = [kw.text for kw in _keyword_list(pos_artists[:4])]
        work_texts = [kw.text for kw in _keyword_list(pos_works)[:2]]
        for text in core_texts[:3]:
            seed_plan.append((text, 'artist'))
        for text in genre_tokens[:4]:
            seed_plan.append((text, 'genre'))
        if core_texts and genre_tokens:
            seed_plan.append((f'{core_texts[0]} {genre_tokens[0]}', 'artist_genre'))
            if len(core_texts) > 1 and len(genre_tokens) > 1:
                seed_plan.append((f'{core_texts[1]} {genre_tokens[1]}', 'artist_genre'))
        for text in work_texts:
            seed_plan.append((text, 'work'))
        for text in languages[:1]:
            seed_plan.append((text, 'language'))
        seen_seeds: set[str] = set()
        seed_plan = [(text, kind) for text, kind in seed_plan
                     if text.strip() and not (text in seen_seeds or seen_seeds.add(text))]
    seed_plan = seed_plan[: config['max_seed_keywords'] * 2]
    if not seed_plan:
        seed_plan = [('华语流行', 'genre')]

    # -- 硬约束：固定四类 + 画像 hard_constraints 中的作品/关键词/歌手 -----
    # 如「…如 Alan Walker《Faded》、Marshmello《Alone》」会抽出作品级硬约束，
    # 与固定词表一起参与 judge_track 判定。作品词条用 exact 等值匹配
    # （「Faded Love」「Alone Again」不会因子串被误判），短语/关键词仍为子串。
    hard_artists, hard_works, hard_phrase = _entities_from_lines(
        [str(x) for x in hard_constraints], negative=True)
    hard: dict[str, tuple[Keyword, ...]] = {}
    hard_labels: dict[str, str] = {}
    hard_match_artist: set[str] = set()
    for category, spec in HARD_CATEGORIES.items():
        hard[category] = _keyword_list(list(spec['keywords']))
        hard_labels[category] = str(spec['label'])
        if spec.get('match_artist'):
            hard_match_artist.add(category)
    profile_works = _keyword_list(hard_works, exact=True) + _keyword_list(
        list(dict.fromkeys(list(config.get('hard_keywords') or []) + hard_phrase)))
    if profile_works:
        hard['profile_works'] = profile_works
        hard_labels['profile_works'] = '画像硬约束(作品/关键词)'
    if hard_artists:
        hard['profile_artists'] = _keyword_list(hard_artists)
        hard_labels['profile_artists'] = '画像硬约束(歌手)'
        hard_match_artist.add('profile_artists')

    core_list = list(dict.fromkeys(pos_artists))[:6]
    known_list = list(dict.fromkeys(pos_artists))[6:40]
    # for_you 覆盖项真正并入判定词表（P2 修复）：
    # negative_artists/negative_keywords/positive_keywords 与自动抽取结果合并。
    return TasteProfile(
        path=str(path or DEFAULT_PROFILE_PATH),
        version=(file_hash or hashlib.sha256(
            json.dumps(raw, ensure_ascii=False, sort_keys=True).encode('utf-8')).hexdigest()[:12]),
        raw=raw,
        config=config,
        hard=hard,
        hard_labels=hard_labels,
        hard_match_artist=hard_match_artist,
        version_markers=_keyword_list(VERSION_MARKERS),
        negative_artists=_keyword_list(neg_artists + (config.get('negative_artists') or [])),
        negative_works=_keyword_list(neg_works, exact=True),
        negative_keywords=_keyword_list(neg_keywords + (config.get('negative_keywords') or [])),
        core_artists=_keyword_list(core_list),
        known_artists=_keyword_list(known_list),
        positive_works=_keyword_list(pos_works, exact=True),
        positive_genres=tuple((_keyword(token), weight) for token, weight in genres),
        positive_keywords=_keyword_list(
            list(dict.fromkeys(pos_keywords + languages)) + (config.get('positive_keywords') or [])),
        languages=tuple(dict.fromkeys(languages)),
        seed_plan=tuple(seed_plan),
        seed_keywords=tuple(text for text, _kind in seed_plan),
    )


def load_profile(path: str | None = None, force: bool = False) -> TasteProfile:
    """读取画像（带 mtime/size 缓存热加载）。缺失或 JSON 非法抛 ProfileError。"""
    resolved = str(path or os.getenv('TASTE_PROFILE_PATH') or DEFAULT_PROFILE_PATH)
    try:
        stat = os.stat(resolved)
    except OSError as exc:
        raise ProfileError(f'profile_missing: 无法读取画像文件（{type(exc).__name__}）') from exc
    cached = _PROFILE_CACHE.get(resolved)
    if not force and cached and cached[0] == stat.st_mtime_ns and cached[1] == stat.st_size:
        return cached[2]
    try:
        with open(resolved, encoding='utf-8') as handle:
            raw = json.load(handle)
    except (ValueError, OSError) as exc:
        raise ProfileError(f'profile_invalid: JSON 解析失败（{type(exc).__name__}）') from exc
    with open(resolved, 'rb') as handle:
        digest = hashlib.sha256(handle.read()).hexdigest()[:12]
    profile = parse_profile(raw, resolved, digest)
    _PROFILE_CACHE[resolved] = (stat.st_mtime_ns, stat.st_size, profile)
    return profile


# ---------------------------------------------------------------------------
# 曲目级判定（纯函数）
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TrackVerdict:
    hard: tuple[str, ...] = ()
    taste: tuple[str, ...] = ()
    dislike: tuple[str, ...] = ()
    version: tuple[str, ...] = ()

    @property
    def covered(self) -> bool:
        return bool(self.hard or self.taste or self.dislike or self.version)


def judge_track(profile: TasteProfile, title: str, artist: str, album: str) -> TrackVerdict:
    """对单首曲目给出硬约束/正向/负向/版本判定（只看标题、歌手、专辑文字）。"""
    hard: list[str] = []
    for category, keywords in profile.hard.items():
        texts = (title, album, artist) if category in profile.hard_match_artist else (title, album)
        if profile.hits(keywords, *texts):
            hard.append(category)
    taste: list[str] = []
    for kw in profile.hits(profile.core_artists, artist):
        taste.append(f'核心歌手:{kw.text}')
    if not taste:
        for kw in profile.hits(profile.known_artists, artist):
            taste.append(f'歌手:{kw.text}')
    for kw in profile.hits(profile.positive_works, title, album):
        taste.append(f'作品:{kw.text}')
    dislike: list[str] = []
    for kw in profile.hits(profile.negative_artists, artist):
        dislike.append(f'负向歌手:{kw.text}')
    for kw in profile.hits(profile.negative_works, title, album):
        dislike.append(f'负向作品:{kw.text}')
    for kw in profile.hits(profile.negative_keywords, title, album):
        dislike.append(f'负向词:{kw.text}')
    version = [kw.text for kw in profile.hits(profile.version_markers, title, album)]
    return TrackVerdict(tuple(dict.fromkeys(hard)), tuple(dict.fromkeys(taste)),
                        tuple(dict.fromkeys(dislike)), tuple(version))


# ---------------------------------------------------------------------------
# 粗筛（歌单元数据，不额外请求）
# ---------------------------------------------------------------------------

@dataclass
class CoarseResult:
    score: float = 0.0
    keyword_score: float = 0.0
    popularity_score: float = 0.0
    freshness_score: float = 0.0
    rejected: str | None = None
    reasons: list[dict] = field(default_factory=list)
    soft_penalties: list[dict] = field(default_factory=list)


def parse_update_time(value: object) -> datetime | None:
    text = str(value or '').strip()
    if not text:
        return None
    if text.isdigit():
        stamp = int(text)
        if stamp > 10 ** 12:
            stamp //= 1000
        try:
            return datetime.fromtimestamp(stamp, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%Y/%m/%d'):
        try:
            return datetime.strptime(text[:19], fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def coarse_score(profile: TasteProfile, meta: dict, now: datetime | None = None) -> CoarseResult:
    """第一层：标题/简介偏好词 + 播放量对数分 + 曲目数合理性 + 更新时间。"""
    weights = profile.config['weights']
    result = CoarseResult()
    title = str(meta.get('title') or '')
    description = str(meta.get('description') or '')
    item_count = int(meta.get('item_count') or 0)
    if item_count < profile.config['min_track_count']:
        result.rejected = 'too_few_tracks'
        return result
    if item_count > profile.config['max_track_count']:
        result.rejected = 'too_many_tracks'
        return result
    for kw in profile.hits(_COARSE_REJECT, title):
        result.rejected = f'hard_title:{kw.text}'
        return result
    for kw in profile.hits(_COARSE_REJECT, description[:120]):
        result.rejected = f'hard_desc:{kw.text}'
        return result

    keyword_raw = 0.0
    seen_labels: set[str] = set()
    for kw in profile.hits(profile.core_artists, title, description):
        keyword_raw += 1.0
        seen_labels.add(f'偏好歌手:{kw.text}')
    for kw in profile.hits(profile.known_artists, title, description):
        keyword_raw += 0.7
        seen_labels.add(f'偏好歌手:{kw.text}')
    for kw, weight in profile.positive_genres:
        if profile.hits((kw,), title, description):
            keyword_raw += 0.6 * weight
            seen_labels.add(f'曲风:{kw.text}')
    for kw in profile.hits(profile.positive_keywords, title, description):
        keyword_raw += 0.4
        seen_labels.add(f'偏好词:{kw.text}')
    result.keyword_score = min(1.0, keyword_raw / 2.0)
    result.reasons = [{'label': label, 'kind': 'coarse'} for label in sorted(seen_labels)]

    play_count = int(meta.get('play_count') or 0)
    result.popularity_score = min(1.0, math.log10(max(play_count, 10)) / 7.0)
    updated = parse_update_time(meta.get('update_time'))
    if updated is None:
        result.freshness_score = 0.5
    else:
        age_days = max(0.0, ((now or datetime.now(timezone.utc)) - updated).total_seconds() / 86400)
        result.freshness_score = max(0.0, 1.0 - age_days / (365 * 5))
    result.score = (weights['keyword'] * result.keyword_score
                    + weights['popularity'] * result.popularity_score
                    + weights['freshness'] * result.freshness_score)
    # 宽泛词降权（修订要求 6）：bgm/神曲/短视频 单词不再整单拒绝，只降低粗筛分，
    # 并记录为风险提示；真正的负向类型仍由硬排斥词与深筛曲目兜底。
    # 注意：显式空列表 = 禁用降权（不能用 or 回落默认，否则清空配置失效）。
    soft_words = profile.config.get('coarse_soft_penalty_words')
    if soft_words is None:
        soft_words = _COARSE_SOFT_DEFAULT
    soft_hits = profile.hits(_keyword_list(soft_words), title, description[:120])
    if soft_hits:
        penalty = min(0.2, 0.05 * len(soft_hits))
        result.score -= penalty
        result.soft_penalties = [{'label': f'标题含宽泛词:{kw.text}', 'kind': 'soft_penalty',
                                  'count': 1, 'samples': []} for kw in soft_hits]
    return result


# ---------------------------------------------------------------------------
# 深筛（曲目详情）
# ---------------------------------------------------------------------------

@dataclass
class DeepResult:
    status: str = 'ok'            # ok | error | empty
    error: str | None = None
    taste_hits: int = 0
    hard_hits: int = 0
    dislike_hits: int = 0
    version_only: int = 0
    scored_tracks: int = 0
    taste_rate: float = 0.0
    hard_rate: float = 0.0
    dislike_rate: float = 0.0
    coverage: float = 0.0
    deep_score: float = 0.0
    rejected: str | None = None
    reasons: list[dict] = field(default_factory=list)
    risks: list[dict] = field(default_factory=list)
    items: list[dict] = field(default_factory=list)
    # 构成/类型（修订要求 4：曲目构成为主，标题只作辅助）
    playlist_type: str = 'general'    # artist_focused | variety | general
    dominant_artist: str = ''
    dominant_ratio: float = 0.0
    distinct_artists: int = 0
    matched_artists_count: int = 0


def _netease_playlist_tracks(payload: object) -> list[dict]:
    """把网易云 playlist/detail 的曲目规范化为统一条目（复用连接器返回值）。"""
    if not isinstance(payload, dict):
        return []
    playlist = payload.get('playlist') if isinstance(payload.get('playlist'), dict) else {}
    rows = playlist.get('tracks') or []
    items = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict) or not row.get('name'):
            continue
        artists = ' / '.join(x.get('name', '') for x in row.get('ar') or row.get('artists') or []
                             if isinstance(x, dict))
        album = row.get('al') or row.get('album') or {}
        items.append({'title': str(row.get('name') or ''),
                      'artist': artists,
                      'album': album.get('name', '') if isinstance(album, dict) else str(album)})
    return items


def _qq_playlist_tracks(payload: object) -> list[dict]:
    items = (payload.get('items') or []) if isinstance(payload, dict) else []
    return [{'title': str(x.get('title') or ''), 'artist': str(x.get('artist') or ''),
             'album': str(x.get('album') or '')}
            for x in items if isinstance(x, dict) and x.get('title')]


async def deep_check(profile: TasteProfile, provider: str, connector: Any,
                     playlist_id: str, title: str = '') -> DeepResult:
    """第二层：读取曲目详情并按曲目判定聚合。失败/空结果不抛异常，用 status 表达。"""
    try:
        raw = await connector.playlist(playlist_id)
    except PlatformError as exc:
        return DeepResult(status='error', error=f'{provider}_detail: {str(exc)[:120]}')
    except Exception as exc:  # httpx 超时等非 PlatformError 也按候选级失败处理
        return DeepResult(status='error', error=f'{provider}_detail: {type(exc).__name__}')
    items = _netease_playlist_tracks(raw) if provider == 'netease' else _qq_playlist_tracks(raw)
    if not items:
        # QQ 详情返回 0 首是已知分支：该候选跳过并记录，不得当成成功歌单。
        return DeepResult(status='empty')
    return _score_tracks(profile, DeepResult(items=items), items, title=title)


def _classify_playlist(profile: TasteProfile, title: str, dominant_artist: str,
                       dominant_ratio: float, distinct_artists: int,
                       matched_artists_count: int, config: dict) -> tuple[str, str]:
    """按曲目构成识别歌单类型（修订要求 4：标题只作辅助）。

    - artist_focused：第一歌手占可读曲目 ≥ ``artist_dominance_ratio``；或标题
      明确含画像歌手且该歌手就是第一歌手（阈值放宽 25%，标题主题补足曲目构成的
      临界情形）。
    - variety：不同歌手数 ≥ ``variety_min_distinct_artists`` 且第一歌手占比
      ≤ ``variety_max_top_artist_ratio``。
    - 其余为 general。合作曲按参与歌手分别计数。
    """
    if dominant_ratio >= float(config['artist_dominance_ratio']):
        return 'artist_focused', dominant_artist
    if dominant_artist and dominant_ratio >= float(config['artist_dominance_ratio']) * 0.75:
        for kw in profile.hits(profile.core_artists + profile.known_artists, title):
            if compact_text(kw.text) and compact_text(kw.text) in compact_text(dominant_artist):
                return 'artist_focused', dominant_artist
    if distinct_artists >= int(config['variety_min_distinct_artists']) \
            and dominant_ratio <= float(config['variety_max_top_artist_ratio']):
        return 'variety', ''
    return 'general', (dominant_artist if dominant_ratio >= 0.3 else '')


def _score_tracks(profile: TasteProfile, result: DeepResult, items: list[dict],
                  title: str = '') -> DeepResult:
    thresholds = profile.config['thresholds']
    weights = profile.config['weights']
    verdicts = [judge_track(profile, item.get('title', ''), item.get('artist', ''), item.get('album', ''))
                for item in items]
    result.scored_tracks = len(verdicts)
    result.taste_hits = sum(1 for v in verdicts if v.taste)
    result.hard_hits = sum(1 for v in verdicts if v.hard)
    result.dislike_hits = sum(1 for v in verdicts if v.dislike)
    result.version_only = sum(1 for v in verdicts if v.version and not (v.taste or v.dislike or v.hard))
    covered = sum(1 for v in verdicts if v.covered)
    result.taste_rate = result.taste_hits / result.scored_tracks
    result.hard_rate = result.hard_hits / result.scored_tracks
    result.dislike_rate = result.dislike_hits / result.scored_tracks
    result.coverage = covered / result.scored_tracks

    hard_samples: dict[str, list[str]] = {}
    taste_samples: dict[str, list[str]] = {}
    dislike_samples: dict[str, list[str]] = {}
    for item, verdict in zip(items, verdicts):
        label = f'{item.get("title", "")} · {item.get("artist", "")}'.strip(' ·')
        for category in verdict.hard:
            hard_samples.setdefault(profile.hard_labels.get(category, category), []).append(label)
        for tag in verdict.taste[:1]:
            taste_samples.setdefault(tag.rsplit(':', 1)[0], []).append(label)
        for tag in verdict.dislike[:1]:
            dislike_samples.setdefault(tag.rsplit(':', 1)[0], []).append(label)
    result.reasons = [{'label': label, 'kind': 'taste', 'count': len(samples), 'samples': samples[:3]}
                      for label, samples in sorted(taste_samples.items(), key=lambda x: -len(x[1]))[:5]]
    result.risks = [{'label': label, 'kind': 'hard', 'count': len(samples), 'samples': samples[:3]}
                    for label, samples in sorted(hard_samples.items(), key=lambda x: -len(x[1]))]
    result.risks += [{'label': label, 'kind': 'dislike', 'count': len(samples), 'samples': samples[:3]}
                     for label, samples in sorted(dislike_samples.items(), key=lambda x: -len(x[1]))]

    # 构成统计（修订要求 4）：合作曲按参与歌手分别计数，空歌手名不入集合
    artist_track_count: dict[str, int] = {}
    matched_artist_set: set[str] = set()
    for item, verdict in zip(items, verdicts):
        for name in (part.strip() for part in str(item.get('artist') or '').split('/')):
            if name:
                artist_track_count[name] = artist_track_count.get(name, 0) + 1
        for tag in verdict.taste:
            if tag.startswith(('核心歌手:', '歌手:')):
                matched_artist_set.add(tag.split(':', 1)[1])
    if artist_track_count:
        dominant_name, dominant_count = max(artist_track_count.items(), key=lambda kv: kv[1])
        result.dominant_artist = dominant_name
        result.dominant_ratio = dominant_count / result.scored_tracks
    result.distinct_artists = len(artist_track_count)
    result.matched_artists_count = len(matched_artist_set)
    result.playlist_type, result.dominant_artist = _classify_playlist(
        profile, title, result.dominant_artist, result.dominant_ratio,
        result.distinct_artists, result.matched_artists_count, profile.config)

    track_score = (weights['taste'] * result.taste_rate
                   + weights['not_hard'] * (1 - result.hard_rate)
                   + weights['coverage'] * result.coverage)
    result.deep_score = track_score - weights['dislike_penalty'] * result.dislike_rate

    if result.hard_hits >= thresholds['hard_hits_limit'] or \
            result.hard_rate > thresholds['hard_rate_limit']:
        result.rejected = 'hard_constraint'
    elif result.scored_tracks >= thresholds['taste_rate_min_tracks'] and \
            result.taste_rate < thresholds['taste_rate_limit']:
        result.rejected = 'low_match'
    return result


# ---------------------------------------------------------------------------
# 候选发现与轮换
# ---------------------------------------------------------------------------

class RotationError(RuntimeError):
    """整轮失败（双平台都不可用/无合格候选）。旧池保持不变。"""


def _coerce_items(payload: object) -> list[dict]:
    rows = payload.get('items') if isinstance(payload, dict) else []
    return [row for row in rows if isinstance(row, dict) and row.get('playlist_id')]


async def discover_candidates(provider: str, connector: Any, profile: TasteProfile,
                              semaphore: asyncio.Semaphore, errors: list[dict]) -> list[dict]:
    """按种子计划搜索公开歌单（修订要求 5：类型配比，类型写入候选诊断）；
    单个关键词失败不拖垮整个平台。"""
    seeds = list(profile.seed_plan[: int(profile.config['max_seed_keywords'])])
    if not seeds:
        raise RotationError(f'{provider}: 没有可用的搜索种子')
    collected: dict[str, dict] = {}
    failed = 0
    for seed_text, seed_type in seeds:
        try:
            async with semaphore:
                payload = await connector.search_playlists(seed_text, 1, int(profile.config['search_page_size']))
            taken = 0
            for row in _coerce_items(payload):
                key = str(row['playlist_id'])
                if key in collected or taken >= int(profile.config['playlists_per_keyword']):
                    continue
                collected[key] = {**row, 'provider': provider,
                                  'seed_keyword': seed_text, 'seed_type': seed_type}
                taken += 1
        except Exception as exc:  # 单关键词失败只记错误（seed 只记序号，不落画像文本）
            failed += 1
            errors.append({'provider': provider, 'stage': 'search', 'seed_index': seeds.index((seed_text, seed_type)),
                           'error': str(exc)[:160]})
    if failed and not collected:
        raise RotationError(f'{provider}: 全部 {failed} 个关键词搜索失败')
    return list(collected.values())


def _rotation_time_parts() -> tuple[int, int]:
    value = os.getenv('FOR_YOU_ROTATION_TIME', '06:00').strip()
    try:
        hour, minute = (int(part) for part in value.split(':', 1))
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError
        return hour, minute
    except (TypeError, ValueError):
        return 6, 0


def next_rotation_time(now: datetime) -> str:
    """下一次轮换时刻（北京时间锚点，默认 06:00），存 UTC 字符串。"""
    hour, minute = _rotation_time_parts()
    local = now.astimezone(BEIJING)
    candidate = local.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if candidate <= local:
        candidate += timedelta(days=1)
    return candidate.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')


_ROTATION_LOCK = asyncio.Lock()


def _recover_stale_runs(db) -> None:
    row = db.fetchone("select id,started_at from for_you_runs where status='running' order by id desc limit 1")
    if not row:
        return
    started = _parse_db_time(row.get('started_at'))
    if started is None or (datetime.now(timezone.utc) - started).total_seconds() > STALE_RUN_SECONDS:
        db.execute("update for_you_runs set status='failed',finished_at=CURRENT_TIMESTAMP,"
                   "errors=? where id=? and status='running'",
                   ('[{"stage":"run","error":"run_interrupted_by_restart"}]', row['id']))


def _today(now: datetime) -> str:
    return now.astimezone(BEIJING).strftime('%Y-%m-%d')


def _today_runs(db, now: datetime) -> list[dict]:
    return db.fetchall('select * from for_you_runs where run_date=? order by id desc', (_today(now),))


def _remaining_budget(db, now: datetime, daily_rotation: int) -> int:
    swapped = sum(int(row.get('swapped_in') or 0) for row in _today_runs(db, now)
                  if row.get('status') in ('success', 'partial'))
    return max(0, int(daily_rotation) - swapped)


def _cooldown_cutoff(now: datetime, cooldown_days: int) -> str:
    return (now - timedelta(days=int(cooldown_days))).strftime('%Y-%m-%d %H:%M:%S')


def _excluded_keys(db, now: datetime, cooldown_days: int) -> set[tuple[str, str]]:
    excluded: set[tuple[str, str]] = set()
    for row in db.fetchall('select provider,playlist_id from for_you_pool'):
        excluded.add((str(row['provider']), str(row['playlist_id'])))
    for row in db.fetchall('select provider,playlist_id from for_you_blacklist'):
        excluded.add((str(row['provider']), str(row['playlist_id'])))
    for row in db.fetchall('select provider,playlist_id,removed_at from for_you_history where removed_at>=?',
                           (_cooldown_cutoff(now, cooldown_days),)):
        excluded.add((str(row['provider']), str(row['playlist_id'])))
    return excluded


def build_connectors(db) -> dict[str, Any]:
    """按现有账号表组装连接器（公开搜索/详情不强依赖登录，Cookie 缺省为空）。"""
    connectors: dict[str, Any] = {}
    for provider, cls in (('netease', NeteaseConnector), ('qq', QQMusicConnector)):
        account = db.fetchone('select cookie from accounts where provider=?', (provider,)) or {}
        connectors[provider] = cls(account.get('cookie') or '')
    return connectors


def _parse_db_time(value: object) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except (TypeError, ValueError):
        return None


async def run_rotation(db, *, trigger: str = 'scheduled', connectors: dict | None = None,
                       profile_path: str | None = None, now: datetime | None = None) -> dict:
    """执行一轮「发现 → 粗筛 → 深筛 → 事务换血」。任何失败都保留旧池。"""
    moment = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    _recover_stale_runs(db)

    today_rows = _today_runs(db, moment)
    if trigger == 'scheduled':
        if any(row.get('status') in ('success', 'partial') for row in today_rows):
            return {'status': 'skipped', 'reason': 'already_ran_today'}
        last_failed = next((row for row in today_rows if row.get('status') == 'failed'), None)
        if last_failed:
            started = _parse_db_time(last_failed.get('started_at'))
            if started and (moment - started).total_seconds() < SCHEDULED_RETRY_SECONDS:
                return {'status': 'skipped', 'reason': 'recent_failure_backoff'}
    if _ROTATION_LOCK.locked():
        return {'status': 'skipped', 'reason': 'already_running'}

    async with _ROTATION_LOCK:
        # 原子认领（部分唯一索引保证跨进程互斥）：当天已有 running 轮次时
        # 返回 None，不再开出第二轮。
        run_id = _open_run(db, trigger, moment)
        if run_id is None:
            return {'status': 'skipped', 'reason': 'already_running'}
        try:
            profile = load_profile(profile_path)
        except ProfileError as exc:
            db.execute("update for_you_runs set status='failed',finished_at=CURRENT_TIMESTAMP,errors=? where id=?",
                       (json.dumps([{'stage': 'profile', 'error': str(exc)[:200]}], ensure_ascii=False), run_id))
            return {'status': 'failed', 'run_id': int(run_id), 'error': str(exc)[:200]}
        config = profile.config
        budget = _remaining_budget(db, moment, config['daily_rotation'])
        if budget <= 0:
            # 当日预算用尽：关闭本轮占位，不算失败。
            db.execute("update for_you_runs set status='skipped',finished_at=CURRENT_TIMESTAMP,"
                       "errors=? where id=?",
                       (json.dumps([{'stage': 'rotation', 'error': 'daily_budget_exhausted'}],
                                   ensure_ascii=False), run_id))
            return {'status': 'skipped', 'reason': 'daily_budget_exhausted'}

        db.execute('update for_you_runs set profile_version=? where id=?', (profile.version, run_id))
        errors: list[dict] = []
        try:
            connectors = connectors or build_connectors(db)
            active = {p: connectors[p] for p in SUPPORTED_PROVIDERS if connectors.get(p)}
            if not active:
                raise RotationError('no_platform: 没有可用的平台连接器')
            for provider in SUPPORTED_PROVIDERS:
                if provider not in active:
                    errors.append({'provider': provider, 'stage': 'platform_unavailable',
                                   'error': 'no_connector'})

            # -- 发现（双平台并行，单平台失败降级继续） -------------------
            discovery: dict[str, list[dict]] = {}

            async def _discover(provider: str) -> None:
                semaphore = asyncio.Semaphore(int(config['request_concurrency'][provider]))
                discovery[provider] = await discover_candidates(
                    provider, active[provider], profile, semaphore, errors)

            probe_results = await asyncio.gather(*(_discover(p) for p in active), return_exceptions=True)
            for provider, probe_result in zip(active, probe_results):
                if isinstance(probe_result, Exception):
                    errors.append({'provider': provider, 'stage': 'discover',
                                   'error': str(probe_result)[:160]})
            candidates = [row for rows in discovery.values() for row in rows]
            if not candidates:
                raise RotationError('no_candidates: 双平台均未返回候选')

            excluded = _excluded_keys(db, moment, config['cooldown_days'])
            fresh = [row for row in candidates
                     if (str(row.get('provider')), str(row.get('playlist_id'))) not in excluded]
            if not fresh:
                raise RotationError('no_fresh_candidates: 候选全部在池内/冷却/黑名单中')

            # -- 粗筛 -----------------------------------------------------
            coarse_rows = []
            for row in fresh:
                coarse = coarse_score(profile, row)
                if coarse.rejected:
                    continue
                coarse_rows.append((coarse, row))
            coarse_rows.sort(key=lambda pair: -pair[0].score)
            deep_queue = coarse_rows[: int(config['deep_check_top_n'])]

            # -- 深筛（详情按平台各自限流；失败/空曲目候选逐个跳过） -------
            semaphores = {p: asyncio.Semaphore(int(config['request_concurrency'][p])) for p in active}
            counters = {'hard_filtered': 0, 'low_match': 0, 'detail_errors': 0, 'empty_details': 0}
            qualified: list[dict] = []

            async def _deep(provider: str, coarse: CoarseResult, row: dict) -> None:
                async with semaphores[provider]:
                    deep = await deep_check(profile, provider, active[provider],
                                            str(row['playlist_id']), title=str(row.get('title') or ''))
                if deep.status == 'error':
                    counters['detail_errors'] += 1
                    errors.append({'provider': provider, 'stage': 'detail',
                                   'playlist_id': str(row['playlist_id']),
                                   'error': deep.error or 'detail_error'})
                    return
                if deep.status == 'empty':
                    counters['empty_details'] += 1
                    errors.append({'provider': provider, 'stage': 'detail_empty',
                                   'playlist_id': str(row['playlist_id']),
                                   'error': 'detail_empty_tracks'})
                    return
                if deep.rejected == 'hard_constraint':
                    counters['hard_filtered'] += 1
                    return
                if deep.rejected == 'low_match':
                    counters['low_match'] += 1
                    return
                weights = config['weights']
                qualified.append({
                    'provider': provider, 'playlist_id': str(row['playlist_id']),
                    'title': str(row.get('title') or ''),
                    'description': str(row.get('description') or '')[:500],
                    'cover_url': row.get('cover_url'), 'owner': str(row.get('owner') or ''),
                    'item_count': int(row.get('item_count') or 0),
                    'play_count': int(row.get('play_count') or 0),
                    'update_time': str(row.get('update_time') or ''),
                    'coarse_score': round(coarse.score, 4), 'deep_score': round(deep.deep_score, 4),
                    'total_score': round(weights['coarse'] * coarse.score + weights['deep'] * deep.deep_score, 4),
                    'taste_hits': deep.taste_hits, 'hard_hits': deep.hard_hits,
                    'dislike_hits': deep.dislike_hits, 'scored_tracks': deep.scored_tracks,
                    'taste_rate': round(deep.taste_rate, 4), 'hard_rate': round(deep.hard_rate, 4),
                    'dislike_rate': round(deep.dislike_rate, 4), 'coverage': round(deep.coverage, 4),
                    'reasons': json.dumps(coarse.reasons[:4] + deep.reasons, ensure_ascii=False),
                    'risks': json.dumps(coarse.soft_penalties + deep.risks, ensure_ascii=False),
                    'seed_keyword': str(row.get('seed_keyword') or ''),
                    'seed_type': str(row.get('seed_type') or 'custom'),
                    'playlist_type': deep.playlist_type,
                    'dominant_artist': deep.dominant_artist,
                    'dominant_ratio': round(deep.dominant_ratio, 4),
                    'distinct_artists': deep.distinct_artists,
                    'matched_artists_count': deep.matched_artists_count,
                })

            await asyncio.gather(*(_deep(row['provider'], coarse, row) for coarse, row in deep_queue))
            if not qualified:
                raise RotationError('no_qualified_candidates: 深筛后没有合格歌单')
            qualified.sort(key=lambda item: -item['total_score'])
            swap_in = _select_diverse(qualified, budget, config)

            # -- 换血（先算全量结果，再单事务原子替换） -------------------
            pool_rows = db.fetchall('select * from for_you_pool')
            drop_count = max(0, len(pool_rows) + len(swap_in) - int(config['max_pool']))
            victims = sorted(pool_rows, key=lambda row: (float(row.get('total_score') or 0),
                                                         str(row.get('added_at') or '')))[:drop_count]
            await asyncio.to_thread(_apply_swap, db, swap_in, victims, int(run_id), moment)
            summary = {
                'status': 'partial' if (errors or len(swap_in) < budget) else 'success',
                'run_id': int(run_id), 'trigger': trigger,
                'candidates': len(candidates), 'coarse_passed': len(deep_queue),
                'accepted': len(qualified), 'swapped_in': len(swap_in), 'swapped_out': len(victims),
                'hard_filtered': counters['hard_filtered'], 'low_match': counters['low_match'],
                'detail_errors': counters['detail_errors'], 'empty_details': counters['empty_details'],
                'pool_size': _pool_size(db), 'profile_version': profile.version,
                'errors': errors[:40],
            }
            db.execute(
                '''update for_you_runs set status=?,finished_at=CURRENT_TIMESTAMP,candidates=?,deep_checked=?,
                   accepted=?,swapped_in=?,swapped_out=?,hard_filtered=?,low_match=?,errors=?,next_rotation_at=?
                   where id=?''',
                (summary['status'], len(candidates), len(deep_queue), len(qualified),
                 len(swap_in), len(victims), counters['hard_filtered'], counters['low_match'],
                 json.dumps(errors[:40], ensure_ascii=False), next_rotation_time(moment), run_id),
            )
            return summary
        except (ProfileError, RotationError) as exc:
            db.execute(
                "update for_you_runs set status='failed',finished_at=CURRENT_TIMESTAMP,errors=? where id=?",
                (json.dumps([{'stage': 'rotation', 'error': str(exc)[:200]}], ensure_ascii=False), run_id),
            )
            return {'status': 'failed', 'run_id': int(run_id), 'error': str(exc)[:200]}
        except Exception as exc:  # 其它意外异常同样不能丢池
            db.execute(
                "update for_you_runs set status='failed',finished_at=CURRENT_TIMESTAMP,errors=? where id=?",
                (json.dumps([{'stage': 'rotation',
                              'error': f'{type(exc).__name__}: {str(exc)[:180]}'}], ensure_ascii=False), run_id),
            )
            return {'status': 'failed', 'run_id': int(run_id),
                    'error': f'{type(exc).__name__}: {str(exc)[:180]}'}


def _select_diverse(qualified: list[dict], budget: int, config: dict) -> list[dict]:
    """多样性选择（修订要求 2/3）：平台轮转 + 歌手专题/同歌手上限 + 边际惩罚。

    - 每轮按平台（netease、qq 字典序）轮转，从各平台剩余候选中取「边际分」最高者；
      边际分 = 总分 − ``diversity_selection_weight`` ×（已选同主导歌手数 + 已选歌手专题数，
      仅对歌手专题候选计类型惩罚）。
    - 硬上限：歌手专题入选数 ≤ ``artist_focused_daily_cap``；同一主导歌手
      ≤ ``same_dominant_artist_daily_cap``。被上限挡住的候选移入延迟池，不阻塞队列。
    - 预算未满且无候选可选（全部被上限挡住）时，从延迟池与剩余队列按总分回填
      （修订要求 3：合格综合歌单不足时允许回填空位，放宽上限但不放松质量门槛——
      回填候选仍来自深筛合格集合）。
    - 选中时在 reasons 里追加类型与选择理由（修订要求 3：记录类型与选择理由）。
    """
    artist_cap = int(config.get('artist_focused_daily_cap', 5))
    same_cap = int(config.get('same_dominant_artist_daily_cap', 2))
    weight = float(config.get('diversity_selection_weight', 0.15))
    balance = bool(config.get('platform_balance', True))

    queues: dict[str, list[dict]] = {}
    for item in qualified:  # 已按总分降序
        queues.setdefault(item['provider'], []).append(item)

    selected: list[dict] = []
    by_artist: dict[str, int] = {}
    artist_focused_count = 0
    type_labels = {'artist_focused': '歌手专题', 'variety': '综合型', 'general': '一般精选'}

    def adjusted(item: dict) -> float:
        penalty = weight * by_artist.get(item.get('dominant_artist') or '', 0)
        if item.get('playlist_type') == 'artist_focused':
            penalty += weight * artist_focused_count
        return item['total_score'] - penalty

    while len(selected) < budget and any(queues.values()):
        picked_any = False
        providers = sorted(queues) if balance else \
            [p for p, _ in sorted(((q[0]['total_score'], p) for p, q in queues.items() if q), reverse=True)]
        for provider in providers:
            if len(selected) >= budget or not queues[provider]:
                continue
            best_idx, best_adj = None, None
            blocked = True
            for idx, cand in enumerate(queues[provider]):
                if cand.get('playlist_type') == 'artist_focused' and artist_focused_count >= artist_cap:
                    continue
                dominant = cand.get('dominant_artist') or ''
                if dominant and by_artist.get(dominant, 0) >= same_cap:
                    continue
                blocked = False
                adj = adjusted(cand)
                if best_adj is None or adj > best_adj:
                    best_adj, best_idx = adj, idx
            if best_idx is None:
                continue  # 本平台当前全部被上限挡住，交给其它平台或回填
            chosen = queues[provider].pop(best_idx)
            dominant = chosen.get('dominant_artist') or ''
            if dominant:
                by_artist[dominant] = by_artist.get(dominant, 0) + 1
            if chosen.get('playlist_type') == 'artist_focused':
                artist_focused_count += 1
            note = f"类型:{type_labels.get(chosen.get('playlist_type'), chosen.get('playlist_type'))}"
            if chosen.get('playlist_type') == 'artist_focused' and dominant:
                note += f"；同歌手名额 {by_artist[dominant]}/{same_cap}"
            try:
                reasons = json.loads(chosen.get('reasons') or '[]')
            except ValueError:
                reasons = []
            reasons.append({'label': note, 'kind': 'selection', 'count': 1, 'samples': []})
            seed_type = chosen.get('seed_type')
            if seed_type:
                reasons.append({'label': f'搜索来源:{seed_type}', 'kind': 'seed_source',
                                'count': 1, 'samples': [chosen.get('seed_keyword') or '']})
            chosen['reasons'] = json.dumps(reasons, ensure_ascii=False)
            selected.append(chosen)
            picked_any = True
        if not picked_any and len(selected) < budget:
            break  # 两平台本轮均被上限挡住 → 进入回填

    if len(selected) < budget:
        # 回填放宽歌手专题总量上限，但同一主导歌手 ≤2 的约束仍然保持
        # （否则综合不足时会回填出一串同一歌手的专题歌单）。
        remaining = [item for queue in queues.values() for item in queue]
        remaining.sort(key=lambda item: -item['total_score'])
        for item in remaining:
            if len(selected) >= budget:
                break
            dominant = item.get('dominant_artist') or ''
            if dominant and by_artist.get(dominant, 0) >= same_cap:
                continue
            if dominant:
                by_artist[dominant] = by_artist.get(dominant, 0) + 1
            selected.append(item)
    return selected


def _open_run(db, trigger: str, moment: datetime) -> int | None:
    """原子认领一轮运行（审核 P2：跨进程互斥）。

    ``for_you_runs`` 上有部分唯一索引 (run_date) WHERE status='running'：
    当天已有进行中的轮次时 INSERT 因唯一冲突失败，返回 None——调用方按
    already_running 处理，而不是开出第二轮突破每日换入预算。
    进程内并发另由 _ROTATION_LOCK 把守；僵死行由 _recover_stale_runs 回收。
    """
    try:
        return int(db.execute(
            "insert into for_you_runs(trigger,status,run_date,started_at) values(?,'running',?,CURRENT_TIMESTAMP)",
            (trigger, _today(moment)),
        ))
    except Exception as exc:  # sqlite3.IntegrityError：当天已有 running 轮次
        if 'UNIQUE' in str(exc).upper() or 'idx_for_you_runs_running_day' in str(exc):
            return None
        raise


def _apply_swap(db, swap_in: list[dict], victims: list[dict], run_id: int, moment: datetime) -> int:
    """单事务完成入池/出池/冷却历史；任何一步失败整体回滚，旧池原样保留。"""
    with db.transaction() as conn:
        for victim in victims:
            conn.execute('delete from for_you_pool where provider=? and playlist_id=?',
                         (victim['provider'], str(victim['playlist_id'])))
            conn.execute(
                '''insert into for_you_history(provider,playlist_id,title,added_at,removed_at,batch_id,note,rotation_count)
                   values(?,?,?,?,?,?,?,1)
                   on conflict(provider,playlist_id) do update set
                     title=excluded.title,removed_at=excluded.removed_at,batch_id=excluded.batch_id,
                     note=excluded.note,rotation_count=for_you_history.rotation_count+1''',
                (victim['provider'], str(victim['playlist_id']), str(victim.get('title') or ''),
                 str(victim.get('added_at') or ''), moment.strftime('%Y-%m-%d %H:%M:%S'),
                 run_id, 'rotation'),
            )
        for item in swap_in:
            conn.execute(
                '''insert into for_you_pool(provider,playlist_id,title,description,cover_url,owner,item_count,
                                            play_count,update_time,coarse_score,deep_score,total_score,
                                            taste_hits,hard_hits,dislike_hits,scored_tracks,taste_rate,
                                            hard_rate,dislike_rate,coverage,reasons,risks,seed_keyword,batch_id,
                                            playlist_type,dominant_artist,added_at)
                   values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
                   on conflict(provider,playlist_id) do update set
                     title=excluded.title,description=excluded.description,cover_url=excluded.cover_url,
                     owner=excluded.owner,item_count=excluded.item_count,play_count=excluded.play_count,
                     update_time=excluded.update_time,coarse_score=excluded.coarse_score,
                     deep_score=excluded.deep_score,total_score=excluded.total_score,
                     taste_hits=excluded.taste_hits,hard_hits=excluded.hard_hits,
                     dislike_hits=excluded.dislike_hits,scored_tracks=excluded.scored_tracks,
                     taste_rate=excluded.taste_rate,hard_rate=excluded.hard_rate,
                     dislike_rate=excluded.dislike_rate,coverage=excluded.coverage,
                     reasons=excluded.reasons,risks=excluded.risks,seed_keyword=excluded.seed_keyword,
                     batch_id=excluded.batch_id,playlist_type=excluded.playlist_type,
                     dominant_artist=excluded.dominant_artist,added_at=CURRENT_TIMESTAMP''',
                (item['provider'], item['playlist_id'], item['title'], item['description'],
                 item['cover_url'], item['owner'], item['item_count'], item['play_count'],
                 item['update_time'], item['coarse_score'], item['deep_score'], item['total_score'],
                 item['taste_hits'], item['hard_hits'], item['dislike_hits'], item['scored_tracks'],
                 item['taste_rate'], item['hard_rate'], item['dislike_rate'], item['coverage'],
                 item['reasons'], item['risks'], item.get('seed_keyword', ''), run_id,
                 item.get('playlist_type', 'general'), item.get('dominant_artist', '')),
            )
    return len(swap_in)


def _pool_size(db) -> int:
    return int((db.fetchone('select count(*) count from for_you_pool') or {'count': 0})['count'])


# ---------------------------------------------------------------------------
# 查询与反馈（供 API 层调用）
# ---------------------------------------------------------------------------

def _parse_json_column(value: object) -> list:
    try:
        parsed = json.loads(str(value or ''))
        return parsed if isinstance(parsed, list) else []
    except ValueError:
        return []


def pool_items(db) -> list[dict]:
    rows = db.fetchall('select * from for_you_pool order by total_score desc, added_at desc')
    for row in rows:
        row['reasons'] = _parse_json_column(row.get('reasons'))
        row['risks'] = _parse_json_column(row.get('risks'))
        row['provider_label'] = 'QQ 音乐' if row.get('provider') == 'qq' else '网易云音乐'
    return rows


def pool_item(db, item_id: int) -> dict | None:
    row = db.fetchone('select * from for_you_pool where id=?', (item_id,))
    if not row:
        return None
    row['reasons'] = _parse_json_column(row.get('reasons'))
    row['risks'] = _parse_json_column(row.get('risks'))
    row['provider_label'] = 'QQ 音乐' if row.get('provider') == 'qq' else '网易云音乐'
    return row


def latest_run(db) -> dict | None:
    return db.fetchone('select * from for_you_runs order by id desc limit 1')


def pool_status(db, profile_path: str | None = None, now: datetime | None = None) -> dict:
    """池状态概览。注意：不得包含画像内容，只包含结构化计数与错误。"""
    moment = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    profile_error: str | None = None
    profile_version: str | None = None
    max_pool, daily_rotation, cooldown_days = (int(DEFAULT_CONFIG['max_pool']),
                                               int(DEFAULT_CONFIG['daily_rotation']),
                                               int(DEFAULT_CONFIG['cooldown_days']))
    try:
        profile = load_profile(profile_path)
        profile_version = profile.version
        max_pool = int(profile.config['max_pool'])
        daily_rotation = int(profile.config['daily_rotation'])
        cooldown_days = int(profile.config['cooldown_days'])
    except ProfileError as exc:
        profile_error = str(exc)
    run = latest_run(db)
    if run:
        run['errors'] = _parse_json_column(run.get('errors'))
    return {
        'pool_size': _pool_size(db), 'max_pool': max_pool,
        'daily_rotation': daily_rotation, 'cooldown_days': cooldown_days,
        'today_swapped_in': daily_rotation - _remaining_budget(db, moment, daily_rotation),
        'last_run': run,
        # 下一锚点直接现算：不依赖最近一条运行行的状态（skipped/failed 也应显示）
        'next_rotation_at': next_rotation_time(moment),
        'blacklist_count': int((db.fetchone('select count(*) count from for_you_blacklist')
                                or {'count': 0})['count']),
        'profile': {'ok': profile_error is None, 'error': profile_error,
                    'version': profile_version,
                    'path': str(profile_path or os.getenv('TASTE_PROFILE_PATH') or DEFAULT_PROFILE_PATH)},
    }


def dislike_item(db, item_id: int) -> dict:
    """「不感兴趣」：出池 + 记冷却历史 + 进黑名单。黑名单可通过 restore 恢复。"""
    row = db.fetchone('select * from for_you_pool where id=?', (item_id,))
    if not row:
        raise LookupError('for_you_item_not_found')
    now = datetime.now(timezone.utc)
    with db.transaction() as conn:
        conn.execute('delete from for_you_pool where id=?', (item_id,))
        conn.execute(
            '''insert into for_you_history(provider,playlist_id,title,added_at,removed_at,note,rotation_count)
               values(?,?,?,?,?,?,1)
               on conflict(provider,playlist_id) do update set removed_at=excluded.removed_at,
                 note=excluded.note,rotation_count=for_you_history.rotation_count+1''',
            (row['provider'], str(row['playlist_id']), str(row.get('title') or ''),
             str(row.get('added_at') or ''), now.strftime('%Y-%m-%d %H:%M:%S'), 'user_dislike'),
        )
        conn.execute(
            '''insert into for_you_blacklist(provider,playlist_id,title,reason,created_at)
               values(?,?,?,?,CURRENT_TIMESTAMP)
               on conflict(provider,playlist_id) do update set title=excluded.title,
                 reason=excluded.reason,created_at=CURRENT_TIMESTAMP''',
            (row['provider'], str(row['playlist_id']), str(row.get('title') or ''), 'user_feedback'),
        )
    return {'disliked': True, 'provider': row['provider'], 'playlist_id': str(row['playlist_id'])}


def restore_item(db, provider: str, playlist_id: str) -> dict:
    """把误标的歌单移出黑名单；冷却期结束后将重新参与候选。"""
    row = db.fetchone('select id from for_you_blacklist where provider=? and playlist_id=?',
                      (provider, str(playlist_id)))
    if not row:
        raise LookupError('blacklist_item_not_found')
    db.execute('delete from for_you_blacklist where id=?', (row['id'],))
    return {'restored': True, 'provider': provider, 'playlist_id': str(playlist_id)}


def blacklist_rows(db) -> list[dict]:
    return db.fetchall('select * from for_you_blacklist order by created_at desc')
