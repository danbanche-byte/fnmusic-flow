from __future__ import annotations

import os
import re
import unicodedata
from typing import Any

try:
    from opencc import OpenCC
    _TO_SIMPLIFIED = OpenCC('t2s')
except ImportError:  # Development installs created before v38 remain usable.
    _TO_SIMPLIFIED = None


_BRACKETS = re.compile(r"[\(\[\{（【《](.*?)[\)\]\}）】》]")
_SEPARATORS = re.compile(r"\s*(?:/|&|,|，|、|;|；| feat\.? | featuring )\s*", re.IGNORECASE)
_VERSION_ONLY = re.compile(
    r"^(?:live(?:版|version)?|现场版?|录音室版|伴奏|纯音乐|instrumental|"
    r"remaster(?:ed)?(?:\s*\d{2,4})?|重制版?|新版|原版|完整版|特别版|"
    r"single\s*version|album\s*version|radio\s*edit|demo|cover)$",
    re.IGNORECASE,
)


def normalize_text(value: object) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).casefold()
    if _TO_SIMPLIFIED:
        text = _TO_SIMPLIFIED.convert(text)
    text = text.replace("’", "'").replace("‘", "'").replace("`", "'")
    return "".join(char for char in text if char.isalnum())


def title_variants(value: object) -> set[str]:
    text = unicodedata.normalize("NFKC", str(value or "")).strip()
    variants = {normalize_text(text)}
    outside = _BRACKETS.sub(" ", text).strip()
    if outside:
        variants.add(normalize_text(outside))
    for content in _BRACKETS.findall(text):
        # A version marker such as "Live版" identifies an edition, not a
        # song. Treating it as a standalone title makes unrelated live tracks
        # tie with each other and forces the ambiguity guard to reject both.
        if content and not _VERSION_ONLY.fullmatch(content.strip()):
            variants.add(normalize_text(content))
    return {item for item in variants if item}


def title_search_terms(value: object) -> list[str]:
    text = unicodedata.normalize("NFKC", str(value or "")).strip()
    terms = [text]
    outside = re.sub(r"[\(\[\{（【《].*?[\)\]\}）】》]", " ", text).strip()
    if outside:
        terms.append(outside)
    terms.extend(content.strip() for content in _BRACKETS.findall(text)
                 if content.strip() and not _VERSION_ONLY.fullmatch(content.strip()))
    return list(dict.fromkeys(term for term in terms if term))


def artist_variants(value: object) -> set[str]:
    if isinstance(value, list):
        value = " / ".join(
            str(item.get("name") or "") if isinstance(item, dict) else str(item)
            for item in value
        )
    text = unicodedata.normalize("NFKC", str(value or "")).strip()
    parts = [text, *_SEPARATORS.split(text)]
    variants: set[str] = set()
    for item in parts:
        variants.update(title_variants(item))
    return variants


def candidate_artist(row: dict[str, Any]) -> str:
    artists = row.get("artists") or row.get("artist") or row.get("singer") or ""
    if isinstance(artists, list):
        return " / ".join(
            str(item.get("name") or "") if isinstance(item, dict) else str(item)
            for item in artists
        )
    return str(artists or "")


def candidate_path(row: dict[str, Any]) -> str:
    """飞牛搜索结果里带出的真实文件路径（audioSpec.path）。"""
    spec = row.get("audioSpec") or row.get("audio_spec") or {}
    return str(spec.get("path") or "") if isinstance(spec, dict) else ""


def filename_key(value: object) -> str:
    """文件名的「同文件」归一化键：只取主干，去掉目录与扩展名。

    刻意只保留整段主干而不拆「标题 / 歌手」：整段主干是高度特异的信息，
    拿它判断「这两个候选其实是同一个文件」几乎不会误判；
    一旦拆开就退化成普通标题匹配，反而会引入新的歧义。
    """
    text = str(value or "").replace("\\", "/").strip()
    if not text:
        return ""
    name = text.rsplit("/", 1)[-1]
    stem = name.rsplit(".", 1)[0] if "." in name else name
    return normalize_text(stem)


def music_library_roots() -> list[str]:
    """本项目视角下的「音乐库根」候选（容器内路径）。"""
    roots: list[str] = []
    for name in ("MUSIC_ROOT", "MUSIC_LIBRARY_ROOT", "MUSIC_HOST_MOUNT"):
        value = os.getenv(name, "").strip().replace("\\", "/").rstrip("/")
        if value:
            roots.append(value)
    if "/music" not in roots:
        roots.append("/music")
    return roots


def library_relative_path(file_path: object, roots: list[str] | None = None) -> str:
    """把本地文件路径转成「相对音乐库根」的路径；不在库内时返回空串。

    为什么需要它：我们落盘的文件在容器里是 ``/music/Library/...``，
    而飞牛 music.db 里同一条音频记的是 ``/volN/…/音乐/Library/...``（宿主管径各不相同）。
    两者只差一个挂载根前缀，剥掉根之后就是完全相同的字符串。
    这个「相对后缀」正是跨系统定位同一个文件的可靠钥匙 ——
    比标题/歌手标签稳得多，因为它由本项目自己生成，不会被第三方音源改写。
    """
    text = str(file_path or "").replace("\\", "/").strip()
    if not text:
        return ""
    for root in (roots if roots is not None else music_library_roots()):
        root = str(root or "").replace("\\", "/").rstrip("/")
        if not root or root == "/":
            continue
        if text.startswith(root + "/"):
            return text[len(root) + 1:]
        if text == root:
            return ""
    return ""


def is_inside_library(file_path: object, roots: list[str] | None = None) -> bool:
    return bool(library_relative_path(file_path, roots))


def match_score(source: dict[str, Any], candidate: dict[str, Any]) -> int:
    source_titles = title_variants(source.get("title"))
    candidate_titles = title_variants(candidate.get("title") or candidate.get("name"))
    audio_spec = candidate.get("audioSpec") or {}
    path = str(audio_spec.get("path") or "") if isinstance(audio_spec, dict) else ""
    path_titles: set[str] = set()
    path_artists: set[str] = set()
    if path:
        filename = path.replace("\\", "/").rsplit("/", 1)[-1].rsplit(".", 1)[0]
        path_titles.update(title_variants(filename))
        # Downloads use "title - artist.ext".  Extracting the title portion
        # lets fnOS files retain the source title even when its embedded tag
        # was rewritten by another provider (for example MAMA or 公子版).
        filename_parts = re.split(r"\s+-\s+", filename, maxsplit=1)
        filename_title = filename_parts[0].strip()
        if filename_title and filename_title != filename:
            path_titles.update(title_variants(filename_title))
        if len(filename_parts) > 1:
            # Managed library paths keep source artists even when audio tags
            # are empty or incorrect. Some downloaders use underscores as
            # filename-safe separators, so normalize those before matching.
            filename_artist = re.sub(r"\s+_\s+", " / ", filename_parts[1]).strip()
            path_artists.update(artist_variants(filename_artist))
        candidate_titles.update(path_titles)
    common_titles = source_titles & candidate_titles
    source_artists = artist_variants(source.get("artist"))
    candidate_artists = artist_variants(candidate_artist(candidate))
    candidate_artists.update(path_artists)
    artist_match = bool(source_artists & candidate_artists) or (
        bool(source_artists and candidate_artists)
        and any(left in right or right in left for left in source_artists for right in candidate_artists)
    )
    duration_match = False
    try:
        source_duration = int(source.get("duration_ms") or 0)
        candidate_duration = int(candidate.get("duration_ms") or candidate.get("duration") or 0)
        if candidate_duration and candidate_duration < 10000:
            candidate_duration *= 1000
        duration_match = bool(source_duration and candidate_duration and abs(source_duration - candidate_duration) <= 5000)
    except (TypeError, ValueError):
        pass
    full_source = normalize_text(source.get("title"))
    full_candidate = normalize_text(candidate.get("title") or candidate.get("name"))
    if common_titles:
        score = 100 if full_source == full_candidate else 75
    elif min(len(full_source), len(full_candidate)) >= 4 and (full_source in full_candidate or full_candidate in full_source):
        score = 55
    elif artist_match and duration_match:
        score = 45
    else:
        return -1
    # Prefer an exact normalized title over an edition suffix or filename
    # fallback when duration metadata is slightly different. This resolves
    # common fnOS duplicates such as a studio track and its Live edition.
    if full_source == full_candidate:
        score += 10
    if source_artists and candidate_artists:
        if source_artists & candidate_artists:
            score += 55
        elif artist_match:
            score += 20
        else:
            # A repeated title with a similar duration is common in a large
            # library. Do not let an unrelated artist win merely because the
            # title is exact; this was the cause of the QQ playlist items
            # being reported as ambiguous.
            score -= 60
    source_album = normalize_text(source.get("album"))
    candidate_album = candidate.get("album") or candidate.get("albumName") or ""
    if isinstance(candidate_album, dict):
        candidate_album = candidate_album.get("name") or ""
    if source_album and source_album == normalize_text(candidate_album):
        score += 8
    if duration_match:
        score += 25
    if path_titles:
        if full_source in path_titles:
            score += 40
        elif source_titles & path_titles:
            score += 15
    # 最强信号：候选人在飞牛曲库里索引到的文件，与我们本地已下载的那个文件同名。
    # 下载文件名由本项目自己生成（「标题 - 歌手.ext」），飞牛索引的是同一批真实文件，
    # 因此同名基本等价于「同一首」——它能绕过标题/歌手标签被第三方改写导致的匹配失败。
    source_stem = filename_key(source.get("file_path"))
    if source_stem and source_stem in path_titles:
        score += 60
    return score


def choose_match(source: dict[str, Any], candidates: list[dict[str, Any]],
                 expected_filename: str | None = None) -> dict[str, Any] | None:
    """在候选里选唯一匹配。

    expected_filename：本地已下载文件的路径或文件名。给了它就先用「同文件」规则定夺 ——
    飞牛曲库里同一个文件只会有一条索引，因此按文件名比对天然唯一，
    正好治「文件已下载、标签被改、按标题/歌手打分打平」这一类匹配失败。
    """
    wanted = filename_key(expected_filename)
    if wanted:
        same_file = [row for row in candidates if filename_key(candidate_path(row)) == wanted]
        if same_file:
            return same_file[0]
    ranked = [(match_score(source, row), index, row) for index, row in enumerate(candidates)]
    ranked = [item for item in ranked if item[0] >= 0]
    if not ranked:
        return None
    ranked.sort(key=lambda item: (-item[0], item[1]))
    best_score = ranked[0][0]
    best = [item[2] for item in ranked if item[0] == best_score]
    if len(best) == 1:
        return best[0]
    signatures = {
        (normalize_text(row.get("title") or row.get("name")), normalize_text(candidate_artist(row)))
        for row in best
    }
    if len(signatures) == 1:
        return best[0]
    # 同一路径被索引了多次（重复扫描等）不算歧义：文件是同一个，随便取哪条都行。
    paths = {filename_key(candidate_path(row)) for row in best}
    paths.discard("")
    if len(paths) == 1:
        return best[0]
    return None


def choose_library_entry(file_path: object, entries: list[dict[str, Any]],
                         *, inside_library: bool | None = None) -> str | None:
    """在飞牛 music.db 的候选里，按真实文件定位唯一 track guid。

    ``entries`` 来自本地文件名归一化键命中的行，每项形如
    ``{"path": 飞牛绝对路径, "guid": track guid, "deleted": 是否已标记物理删除}``。

    两级证据，从强到弱：
      1) **完整相对路径后缀命中** —— 本地 ``/music/Library/A/B/C.mp3`` 与
         飞牛 ``/volN/…/音乐/Library/A/B/C.mp3`` 剥掉各自的库根后完全一致。
         目录结构 + 文件名都对上，等价于「同一个文件」，几乎不可能误判。
      2) **文件名在全库唯一** —— 文件名是本项目自造的「标题 - 歌手.ext」，
         特异度足够；但必须在文件确实位于音乐库内时才采信，
         否则挂载错配的情况下可能把别的目录里的同名文件认错。
    """
    if not entries:
        return None
    if inside_library is None:
        inside_library = bool(library_relative_path(file_path))
    alive = [item for item in entries if not item.get("deleted")]

    relative = library_relative_path(file_path)
    if relative and "/" in relative:
        suffix = "/" + relative
        hits = [item for item in entries
                if str(item.get("path") or "").endswith(suffix)]
        if not hits:
            lowered = suffix.casefold()
            hits = [item for item in entries
                    if str(item.get("path") or "").casefold().endswith(lowered)]
        if hits:
            # 2026-09-18 事故修复：候选全部是 is_physical_file_deleted=1 的死索引时，
            # 绝不能把死 guid 推进歌单 —— 飞牛端会显示「歌曲文件不存在」，
            # 而推送却报成功。返回 None 交回检索匹配 / 触发重扫补录。
            hits = [item for item in hits if not item.get("deleted")]
            if not hits:
                return None
            guids = {str(item.get("guid")) for item in hits if item.get("guid")}
            # 同一文件被索引多次（同一 guid）不算歧义；不同 guid 才算真歧义。
            if len(guids) == 1:
                return guids.pop()
            return None

    if not inside_library:
        return None
    if not alive:
        return None
    guids = {str(item.get("guid")) for item in alive if item.get("guid")}
    if len(guids) == 1:
        return guids.pop()
    return None
