from __future__ import annotations

import base64
import hashlib
from pathlib import Path


def validate_audio(path: str, expected_duration_ms: int | None = None) -> dict:
    file = Path(path)
    size = file.stat().st_size if file.exists() else 0
    result = {'path': str(file), 'size': size, 'valid': False, 'trial_snippet': False, 'duration_ms': None}
    if size < 65536:
        return result
    try:
        from mutagen import File
        audio = File(path)
        duration_ms = int(float(audio.info.length) * 1000) if audio is not None and getattr(audio, 'info', None) else 0
        result['duration_ms'] = duration_ms
        result['valid'] = duration_ms >= 10000
        if expected_duration_ms and duration_ms < max(10000, int(expected_duration_ms) * 0.6):
            result['trial_snippet'] = True
            result['valid'] = False
    except Exception:
        result['valid'] = False
    return result


def safe_filename(value: str) -> str:
    cleaned = ''.join('_' if char in '<>:"/\\|?*' else char for char in value).strip()
    return cleaned[:180] or 'unknown'


def image_mime(data: bytes) -> str | None:
    if data.startswith(b'\xff\xd8\xff'):
        return 'image/jpeg'
    if data.startswith(b'\x89PNG\r\n\x1a\n'):
        return 'image/png'
    if len(data) >= 12 and data[:4] == b'RIFF' and data[8:12] == b'WEBP':
        return 'image/webp'
    return None


def file_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def has_embedded_cover(path: str | Path) -> bool:
    try:
        from mutagen import File
        from mutagen.flac import FLAC
        from mutagen.mp4 import MP4
        audio = File(str(path))
        if audio is None:
            return False
        if isinstance(audio, FLAC):
            return bool(audio.pictures)
        if isinstance(audio, MP4):
            return bool(audio.tags and audio.tags.get('covr'))
        tags = getattr(audio, 'tags', None)
        if tags is None:
            return False
        if hasattr(tags, 'getall') and tags.getall('APIC'):
            return True
        values = tags.get('metadata_block_picture') if hasattr(tags, 'get') else None
        return bool(values)
    except Exception:
        return False


def write_tags(path: str, *, title: str, artist: str = '', album: str = '',
               cover: bytes | None = None, cover_mime: str | None = None) -> dict:
    """Write text tags and an embedded front cover, then verify both steps."""
    result = {'tags_written': False, 'cover_written': False, 'cover_mime': None, 'error': None}
    try:
        from mutagen import File
        from mutagen.flac import FLAC, Picture
        from mutagen.id3 import APIC, TALB, TIT2, TPE1, ID3
        from mutagen.mp4 import MP4, MP4Cover

        audio = File(path)
        if audio is None:
            raise RuntimeError('unsupported_audio_container')
        mime = cover_mime or (image_mime(cover) if cover else None)
        if cover and not mime:
            raise RuntimeError('unsupported_cover_image')

        if isinstance(audio, FLAC):
            audio['title'] = [title]
            if artist:
                audio['artist'] = [artist]
            if album:
                audio['album'] = [album]
            if cover:
                audio.clear_pictures()
                picture = Picture()
                picture.type = 3
                picture.mime = mime
                picture.desc = 'Cover'
                picture.data = cover
                audio.add_picture(picture)
            audio.save()
        elif isinstance(audio, MP4):
            if audio.tags is None:
                audio.add_tags()
            audio.tags['\xa9nam'] = [title]
            if artist:
                audio.tags['\xa9ART'] = [artist]
            if album:
                audio.tags['\xa9alb'] = [album]
            if cover:
                image_format = MP4Cover.FORMAT_PNG if mime == 'image/png' else MP4Cover.FORMAT_JPEG
                audio.tags['covr'] = [MP4Cover(cover, imageformat=image_format)]
            audio.save()
        elif path.lower().endswith(('.ogg', '.opus')):
            if audio.tags is None:
                audio.add_tags()
            audio['title'] = [title]
            if artist:
                audio['artist'] = [artist]
            if album:
                audio['album'] = [album]
            if cover:
                picture = Picture()
                picture.type = 3
                picture.mime = mime
                picture.desc = 'Cover'
                picture.data = cover
                audio['metadata_block_picture'] = [base64.b64encode(picture.write()).decode('ascii')]
            audio.save()
        else:
            tags = getattr(audio, 'tags', None)
            if tags is None or not hasattr(tags, 'add'):
                try:
                    audio.add_tags()
                    tags = audio.tags
                except Exception:
                    tags = ID3()
            for frame in ('TIT2', 'TPE1', 'TALB', 'APIC'):
                if hasattr(tags, 'delall'):
                    tags.delall(frame)
            tags.add(TIT2(encoding=3, text=title))
            if artist:
                tags.add(TPE1(encoding=3, text=artist))
            if album:
                tags.add(TALB(encoding=3, text=album))
            if cover:
                tags.add(APIC(encoding=3, mime=mime, type=3, desc='Cover', data=cover))
            if getattr(audio, 'tags', None) is tags:
                audio.save()
            else:
                tags.save(path)

        result['tags_written'] = True
        result['cover_written'] = bool(cover and has_embedded_cover(path))
        result['cover_mime'] = mime if result['cover_written'] else None
        if cover and not result['cover_written']:
            result['error'] = 'cover_verification_failed'
    except Exception as exc:
        result['error'] = str(exc)[:300]
    return result
