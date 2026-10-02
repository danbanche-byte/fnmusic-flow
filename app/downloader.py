import asyncio
import hashlib
import os
import re
from pathlib import Path

import httpx


class DownloadError(RuntimeError):
    pass


class DownloadAborted(RuntimeError):
    """用户暂停/取消触发的协作式中断（区别于网络/内容类失败，不计入重试退避）。"""
    pass


def max_audio_bytes() -> int:
    """下载体积上限（字节），MAX_AUDIO_BYTES 可覆盖，默认 300MB。"""
    try:
        return max(1, int(os.getenv('MAX_AUDIO_BYTES', '314572800')))
    except ValueError:
        return 314572800


def _source_key(source: str | None) -> str:
    """把音源名转成文件名安全的 .part 后缀片段。"""
    text = re.sub(r'[^0-9A-Za-z._-]+', '_', str(source or '')).strip('._')
    return (text or 'nosource')[:48]


def _part_file(target: Path, source: str | None) -> Path:
    return target.with_suffix(target.suffix + '.part.' + _source_key(source))


def cleanup_part_files(target: Path) -> int:
    """删除某个目标音频的全部 .part 半截文件（各音源变体一起清）。

    取消任务时调用；完整音频永远不动。返回删除数量。
    """
    try:
        pattern = target.stem + target.suffix + '.part.*'
        siblings = list(target.parent.glob(pattern))
    except OSError:
        return 0
    count = 0
    for item in siblings:
        try:
            item.unlink(missing_ok=True)
            count += 1
        except OSError:
            pass
    return count


def _sweep_foreign_parts(target: Path, keep: Path) -> None:
    """清扫其它音源遗留的 .part，避免换源后误用半截数据。"""
    try:
        pattern = target.stem + target.suffix + '.part.*'
        siblings = list(target.parent.glob(pattern))
    except OSError:
        return
    for item in siblings:
        if item != keep:
            try:
                item.unlink(missing_ok=True)
            except OSError:
                pass


def _hash_target(target: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    with target.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1048576), b''):
            digest.update(chunk)
    return target.stat().st_size, digest.hexdigest()


async def download(url: str, target: Path, source: str | None = None, timeout: float = 60.0,
                   should_abort=None, on_progress=None):
    """下载音频到 target，返回 (字节数, sha256)。

    - .part 文件名携带音源标识：同音源可断点续传；换音源绝不续传，
      避免把两个源的半截字节流拼成"时长合法但中段损坏"的文件。
      开始新下载前清扫其它音源遗留的 .part。
    - MAX_AUDIO_BYTES（默认 300MB）：先看声明长度，再在流中累计兜底；
      超限先退出 with 关闭句柄、再删除临时文件（Windows 下句柄未关闭
      时 unlink 会 PermissionError）。
    - 写盘与最终哈希在线程池执行，不阻塞事件循环。
    - should_abort: 返回 True 时在每个分块边界协作式中断（DownloadAborted），
      支持任务暂停/取消；on_progress(done_bytes, total_bytes|None) 逐块回调，
      供上层节流写库展示进度。
    """
    if should_abort and should_abort():
        raise DownloadAborted('aborted_before_start')
    target.parent.mkdir(parents=True, exist_ok=True)
    part = _part_file(target, source)
    _sweep_foreign_parts(target, part)
    limit = max_audio_bytes()
    start = part.stat().st_size if part.exists() else 0
    if start > limit:
        part.unlink(missing_ok=True)
        start = 0
    headers = {'Range': f'bytes={start}-'} if start else {}
    overflow = False
    written = 0
    declared_total = None
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        async with client.stream('GET', url, headers=headers) as response:
            if response.status_code not in (200, 206):
                raise DownloadError(f'network:{response.status_code}')
            if response.status_code == 206:
                content_range = response.headers.get('content-range', '')
                if not content_range.startswith(f'bytes {start}-'):
                    raise DownloadError('network:invalid_content_range')
            declared = response.headers.get('content-length')
            if declared:
                try:
                    total = start + int(declared)
                except ValueError:
                    total = None
                if total is not None:
                    if total > limit:
                        raise DownloadError(f'content:too_large:{total}')
                    declared_total = total
            mode = 'ab' if start and response.status_code == 206 else 'wb'
            with part.open(mode) as handle:
                async for chunk in response.aiter_bytes(262144):
                    if should_abort and should_abort():
                        raise DownloadAborted(f'aborted_at_{start + written}_bytes')
                    await asyncio.to_thread(handle.write, chunk)
                    written += len(chunk)
                    if on_progress:
                        on_progress(start + written, declared_total)
                    if start + written > limit:
                        overflow = True
                        break
    if overflow:
        part.unlink(missing_ok=True)
        raise DownloadError(f'content:too_large:{start + written}')
    part.replace(target)
    size, digest = await asyncio.to_thread(_hash_target, target)
    if size < 65536:
        target.unlink(missing_ok=True)
        raise DownloadError('content:too_small')
    return size, digest
