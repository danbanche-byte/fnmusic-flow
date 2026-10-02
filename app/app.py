import asyncio
import json
import time
from pathlib import Path
import os
import httpx
from fastapi import FastAPI, File, Header, HTTPException, UploadFile
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from .db import Database
from .library import index_music
from .models import TrackQuery
from .playlist import identify, parse_text
from .sources import SourceRegistry
from .web import page
from .worker import DEFAULT_QUALITY, get_worker

app=FastAPI(title='fnmusic-flow',version='1.3.3'); db=Database(os.getenv('DB_PATH','/config/fnmusic.db')); sources=SourceRegistry(); music_root=Path(os.getenv('MUSIC_ROOT','/music'))
_STARTED_AT = time.monotonic()
# 2026-09-29（1.0.9）：统一网关接入 —— 经 fnOS nginx 的 /app/fnmusic-flow-community/
# 路径反代（SSO）时，请求路径带网关前缀；这里剥掉前缀，让内部路由继续按根路径工作。
# 直连端口（LAN :8002）的请求没有该前缀，不受影响。
_GATEWAY_PREFIX = os.getenv('GATEWAY_PREFIX', '/app/fnmusic-flow-community').rstrip('/')

@app.middleware('http')
async def _strip_gateway_prefix(request, call_next):
    path = request.scope.get('path', '')
    if _GATEWAY_PREFIX and (path == _GATEWAY_PREFIX or path.startswith(_GATEWAY_PREFIX + '/')):
        request.scope['path'] = path[len(_GATEWAY_PREFIX):] or '/'
        raw = request.scope.get('raw_path')
        if raw and raw.decode('utf-8', 'ignore').startswith(_GATEWAY_PREFIX):
            request.scope['raw_path'] = raw[len(_GATEWAY_PREFIX.encode()):]
    return await call_next(request)
# 前端单页体积较大（约 170KB），开启 gzip 压缩可显著缩短首次打开时间
app.add_middleware(GZipMiddleware, minimum_size=1024)
library_scan_lock = asyncio.Lock()
def guard(token):
    if os.getenv('FN_TOKEN') and token!=os.getenv('FN_TOKEN'): raise HTTPException(401,'invalid token')
@app.on_event('startup')
async def startup():
    """按数据库恢复全部音源。kind 决定用哪套运行时；未知/损坏的行跳过，不影响启动。"""
    for row in db.fetchall('select name,kind,script_content,enabled,sort_order,status,last_detail,last_check from sources'):
        try:
            sort_order = int(row.get('sort_order') or 100)
            kind = str(row.get('kind') or '')
            if kind in ('lx_server', 'lx_api'):
                info = json.loads(row['script_content'])
                definition = sources.register_stored(kind, str(info['url']), info.get('token'),
                                                     row['name'], sort_order)
            elif kind == 'lx_custom':
                # 原生 lx-music 自定义源：脚本内容原样恢复，首次解析时再装进 Node 宿主
                definition = await sources.register_lx_custom(row['script_content'], row['name'],
                                                              sort_order=sort_order, load=False)
            else:
                definition = await sources.register_lx_script(row['script_content'], row['name'], probe=False,
                                                              sort_order=sort_order)
            definition.enabled = bool(row.get('enabled', 1))
            sources.seed_health(row['name'], row.get('status'), row.get('last_detail'), row.get('last_check'))
        except Exception as exc:
            # 恢复失败绝不能静默：否则出现「DB 里有音源、界面列表为空」的诡异状态
            print(f'[source-restore] {row.get("name")} ({row.get("kind")}): {type(exc).__name__}: {exc}', flush=True)


def _persist_source_health(name, result):
    db.execute('update sources set status=?,last_check=?,last_detail=? where name=?',
               (str(result.get('status') or 'unknown'), result.get('last_check'), str(result.get('detail') or '')[:300], name))


@app.get('/', response_class=HTMLResponse)
async def index():
    # Ship the reviewed v2 console in the image so NAS users see the product UI.
    frontend = Path(__file__).with_name('static') / 'index.html'
    if frontend.exists():
        return FileResponse(frontend, media_type='text/html', headers={
            'Cache-Control': 'no-store, no-cache, must-revalidate, max-age=0',
            'Pragma': 'no-cache',
        })
    return page()
@app.get('/health')
async def health():
    """纯本地健康检查，绝不为 /health 发起外部网络请求。

    旧实现直接 await sources.health() 逐源探测（单源超时 20s+搜索兜底 20s），
    而 healthcheck 只给 5s，导致容器被反复判 unhealthy 重启、并持续冲击上游。
    现在只检查：数据库可读 + worker 心跳。数据库读不出才 503（此时确实需要
    重启）；worker 心跳陈旧只降级上报，搜索/歌单/推送仍可用，不应重启容器。
    音源真实可用性请用手动 /api/sources/probe。
    """
    try:
        db.fetchall('select 1 as ok')
    except Exception:
        raise HTTPException(status_code=503, detail='database_unavailable')
    from .worker import WORKER
    heartbeat = WORKER.get('heartbeat') or 0.0
    age = time.monotonic() - heartbeat if heartbeat else None
    in_grace = time.monotonic() - _STARTED_AT < 180
    if age is None or age >= 120:
        worker_status = 'starting' if (in_grace and age is None) else 'stale'
    else:
        worker_status = 'ok'
    return {'status': 'ok', 'database': 'ok', 'worker': worker_status,
            'worker_heartbeat_age_seconds': None if age is None else round(age, 1),
            'sources': sources.health_snapshot(),
            'tasks': db.fetchall('select status,count(*) count from tasks group by status')}
@app.get('/api/sources')
async def list_sources(x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    rows = sorted(sources.list(), key=lambda s: (s.sort_order, s.name))
    health = {row['name']: row for row in sources.health_snapshot()}
    result = []
    for item in rows:
        snapshot = health.get(item.name, {})
        row = {**item.model_dump(exclude={'script_content'}),
               'status': snapshot.get('status', 'unknown'),
               'detail': snapshot.get('detail') or '尚未检测',
               'last_check': snapshot.get('last_check')}
        # 原生自定义源脚本会声明自己支持哪些平台，透出给前端展示
        if snapshot.get('platforms'):
            row['platforms'] = snapshot['platforms']
        result.append(row)
    return result

@app.get('/api/sources/health')
async def source_health(x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    return sources.health_snapshot()

@app.post('/api/sources/import')
async def import_source(file: list[UploadFile] = File(...), name: str | None = None,
                        x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    """批量导入音源 JS：一次可上传多个文件，逐个探测协议后入库，返回每条结果。"""
    guard(x_fn_token)
    results = []
    for item in file:
        try:
            content = (await item.read()).decode('utf-8', 'ignore')
            label = name if (name and len(file) == 1) else None
            definition = await sources.register_lx_script(content, label)
            db.execute('insert or replace into sources(name,kind,script_content,enabled,sort_order) values(?,?,?,1,?)',
                       (definition.name, definition.kind, definition.script_content, definition.sort_order))
            results.append({**definition.model_dump(exclude={'script_content'}), 'ok': True,
                            'file': item.filename})
        except Exception as exc:
            results.append({'ok': False, 'file': item.filename, 'error': str(exc)[:200]})
    ok = sum(1 for row in results if row.get('ok'))
    return {'imported': ok, 'failed': len(results) - ok, 'items': results}

@app.post('/api/sources/connect')
async def connect_source(payload: dict, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    """在线添加音源：urls 支持一次填多个地址（换行分隔），逐个探测协议后入库。"""
    guard(x_fn_token)
    raw = payload.get('urls') or payload.get('url') or []
    if isinstance(raw, str):
        raw = [line.strip() for line in raw.replace(',', '\n').splitlines()]
    token = payload.get('token') or None
    name = payload.get('name') or None
    results = []
    for url in [str(item).strip() for item in raw if str(item).strip()]:
        if not url.startswith(('http://', 'https://')):
            results.append({'ok': False, 'url': url, 'error': '地址需以 http(s):// 开头'})
            continue
        try:
            # 2026-10-01（1.3.3）：.js 直链 —— 论坛分享的自定义源脚本常以链接形式给出，
            # 直接拉取脚本内容按 JS 导入（与文件上传走同一注册路径）。
            if url.lower().split('?')[0].endswith('.js'):
                async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
                    resp = await client.get(url)
                    resp.raise_for_status()
                content = resp.text
                if not content.strip():
                    raise ValueError('脚本内容为空')
                definition = await sources.register_lx_script(content, name if len(raw) == 1 else None)
                db.execute('insert or replace into sources(name,kind,script_content,enabled,sort_order) values(?,?,?,1,?)',
                           (definition.name, definition.kind, definition.script_content, definition.sort_order))
                results.append({**definition.model_dump(exclude={'script_content'}), 'ok': True, 'url': url})
                continue
            definition = await sources.register_direct(url, token, name if len(raw) == 1 else None)
            db.execute('insert or replace into sources(name,kind,script_content,enabled,sort_order) values(?,?,?,1,?)',
                       (definition.name, definition.kind, definition.script_content, definition.sort_order))
            results.append({**definition.model_dump(exclude={'script_content'}), 'ok': True, 'url': url})
        except Exception as exc:
            results.append({'ok': False, 'url': url, 'error': str(exc)[:200]})
    ok = sum(1 for row in results if row.get('ok'))
    return {'connected': ok, 'failed': len(results) - ok, 'items': results}

@app.post('/api/sources/probe')
async def probe_sources(payload: dict | None = None, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    """一键测试全部（或指定）音源，结果持久化并刷新故障切换顺序。"""
    guard(x_fn_token)
    payload = payload or {}
    names = payload.get('names') or None
    return {'items': await sources.probe(persist=_persist_source_health, names=names)}

@app.post('/api/sources/{name}/probe')
async def probe_one(name: str, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    if not sources.get(name):
        raise HTTPException(404, '音源不存在')
    rows = await sources.probe(persist=_persist_source_health, names=[name])
    return rows[0] if rows else {}

@app.get('/api/sources/{name}/logs')
async def source_logs(name: str, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    """查看某个音源的运行时日志（原生自定义源可看到脚本内部的 console 输出与解析报错）。"""
    guard(x_fn_token)
    runtime = sources.runtime(name)
    if runtime is None:
        raise HTTPException(404, '音源不存在')
    if not hasattr(runtime, 'logs'):
        raise HTTPException(400, '该音源类型没有可查看的脚本日志')
    try:
        return await runtime.logs()
    except Exception as exc:
        raise HTTPException(502, str(exc)[:200]) from exc

@app.post('/api/sources/reorder')
async def reorder_sources(payload: dict, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    names = [str(item) for item in (payload.get('names') or [])]
    ordered = sources.reorder(names)
    for index, label in enumerate(ordered):
        db.execute('update sources set sort_order=? where name=?', ((index + 1) * 10, label))
    return {'names': ordered}

@app.post('/api/sources/{name}/enabled')
async def toggle_source(name: str, payload: dict, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    if not sources.set_enabled(name, bool(payload.get('enabled', True))):
        raise HTTPException(404, '音源不存在')
    db.execute('update sources set enabled=? where name=?', (1 if payload.get('enabled', True) else 0, name))
    return {'name': name, 'enabled': bool(payload.get('enabled', True))}

@app.delete('/api/sources/{name}')
async def delete_source(name: str, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    if not sources.remove(name):
        raise HTTPException(404, '音源不存在')
    db.execute('delete from sources where name=?', (name,))
    return {'deleted': name}

@app.post('/api/sources/{name}/resolve')
async def resolve(name:str,query:TrackQuery,x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    try: return {'url':await sources.resolve(name,query)}
    except KeyError: raise HTTPException(404,'音源不存在')
    except Exception as exc: raise HTTPException(502,str(exc)) from exc
@app.get('/api/sources/{name}/search')
async def source_search(name: str, keyword: str, platform: str = 'netease', x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    try: return {'items': await sources.search(name, keyword, platform)}
    except KeyError: raise HTTPException(404, '音源不存在')
    except Exception as exc: raise HTTPException(502, str(exc)) from exc
@app.post('/api/playlists/inspect')
async def inspect(payload:dict,x_fn_token: str | None = Header(default=None, alias='X-FN-Token')): guard(x_fn_token); return identify(payload.get('url',''))
@app.post('/api/playlists/text-preview')
async def text_preview(payload:dict,x_fn_token: str | None = Header(default=None, alias='X-FN-Token')): guard(x_fn_token); rows=parse_text(payload.get('text','')); return {'count':len(rows),'items':rows}
@app.post('/api/tracks')
async def create_track(track:dict,x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    db.execute('insert or ignore into tracks(platform,song_id,title,artist,album,duration_ms,quality,cover_url) values(?,?,?,?,?,?,?,?)',(track['platform'],track['song_id'],track['title'],track.get('artist',''),track.get('album',''),track.get('duration_ms'),track.get('quality') or DEFAULT_QUALITY,track.get('cover_url')))
    if track.get('cover_url'):
        db.execute('update tracks set cover_url=? where platform=? and song_id=?',(track.get('cover_url'),track['platform'],track['song_id']))
    row=db.fetchone('select id,status,file_path,cover_status from tracks where platform=? and song_id=?',(track['platform'],track['song_id']))
    if row.get('status') == 'archived' and row.get('file_path') and row.get('cover_status') == 'embedded' and not track.get('force'):
        return {'track_id':row['id'],'task_id':None,'reused':True,'file_path':row['file_path']}
    existing=db.fetchone("select id from tasks where track_id=? and status in ('pending','matching','downloading') order by id desc limit 1",(row['id'],))
    if existing and not track.get('force'):
        return {'track_id':row['id'],'task_id':existing['id'],'reused':True}
    db.execute('insert into tasks(track_id,task_type,priority) values(?,?,?)',(row['id'],track.get('task_type','manual_single'),track.get('priority',3)))
    task=db.fetchone('select id from tasks where track_id=? order by id desc limit 1',(row['id'],))
    return {'track_id':row['id'],'task_id':task['id'],'reused':False}
@app.post('/api/audio/url')
async def audio_url(track:dict,x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    query=TrackQuery(platform=track['platform'],song_id=str(track['song_id']),quality=track.get('quality') or DEFAULT_QUALITY)
    errors=[]
    try:
        url, source_name = await sources.resolve_first(query)
        return {'url': url, 'source': source_name}
    except Exception as exc: errors.append(str(exc)[:200])
    raise HTTPException(502,errors[-1] if errors else '没有音源能解析该歌曲')
@app.get('/api/tasks')
async def tasks(status: str | None = None, task_type: str | None = None, limit: int = 200,
                offset: int = 0, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    limit = min(max(1, int(limit)), 2000)
    offset = max(0, int(offset))
    where: list[str] = []
    params: list[str] = []
    if status:
        allowed = [value.strip() for value in status.split(',') if value.strip()]
        allowed = [value for value in allowed if value in {'pending', 'matching', 'downloading', 'archived', 'failed_final', 'paused', 'cancelled'}]
        if allowed:
            where.append('tasks.status in (' + ','.join('?' for _ in allowed) + ')')
            params.extend(allowed)
    if task_type:
        allowed_types = [value.strip() for value in task_type.split(',')
                         if value.strip() in {'daily','favorite','playlist','chart','manual_single','playback_fallback'}]
        if allowed_types:
            where.append('tasks.task_type in (' + ','.join('?' for _ in allowed_types) + ')')
            params.extend(allowed_types)
    where_sql = (' where ' + ' and '.join(where)) if where else ''
    total_row = db.fetchone('select count(*) as count from tasks join tracks on tracks.id=tasks.track_id' + where_sql,
                            tuple(params))
    total = int((total_row or {}).get('count') or 0)
    query = '''select tasks.*,tracks.title,tracks.artist,tracks.cover_status,tracks.cover_error,
               (select ta.detail from task_attempts ta where ta.task_id=tasks.id order by ta.id desc limit 1) as last_attempt_detail
               from tasks join tracks on tracks.id=tasks.track_id''' + where_sql + ' order by tasks.id desc limit ? offset ?'
    rows = db.fetchall(query, tuple(params) + (limit, offset))
    return {'items': rows, 'total': total, 'limit': limit, 'offset': offset,
            'has_more': offset + len(rows) < total}

@app.get('/api/tasks/summary')
async def task_summary(x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    by_status = {row['status']: int(row['count']) for row in db.fetchall(
        'select status,count(*) count from tasks group by status')}
    by_error = db.fetchall(
        """select case
                 when lower(coalesce(error,'')) like '%403%' or lower(coalesce(error,'')) like '%forbidden%' then '音源不可用（403）'
                 when lower(coalesce(error,'')) like '%尚未配置%' then '尚未配置可用音源'
                 when lower(coalesce(error,'')) like '%cover%' or lower(coalesce(error,'')) like '%artwork%' then '封面处理失败'
                 when error is null or error='' then '未知'
                 else '其他失败' end as reason, count(*) count
             from tasks where status='failed_final' group by reason order by count desc""")
    return {'total': sum(by_status.values()), 'by_status': by_status, 'failed_reasons': by_error,
            'generated_at': __import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat()}
@app.post('/api/tasks/{task_id}/retry')
async def retry(task_id:int,x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    worker = get_worker()
    if worker: worker.clear_user_state(task_id)
    db.execute("update tasks set status='pending',error=null,next_run_at=null,finished_at=NULL,progress_bytes=0,"
               "total_bytes=0,speed_bytes=0,lease_owner=NULL,lease_until=NULL,updated_at=CURRENT_TIMESTAMP where id=?",(task_id,))
    return {'status':'pending'}
@app.post('/api/tasks/retry-failed')
async def retry_failed(x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    row = db.fetchone("select count(*) as count from tasks where status='failed_final'") or {'count': 0}
    db.execute("update tasks set status='pending',error=null,next_run_at=null,finished_at=NULL,progress_bytes=0,"
               "total_bytes=0,speed_bytes=0,lease_owner=NULL,lease_until=NULL,updated_at=CURRENT_TIMESTAMP where status='failed_final'")
    return {'status': 'pending', 'count': int(row.get('count') or 0)}

# ------------------------------------------------ 任务管理（社区版 1.2.0）
# 状态机：pending → matching → downloading → archived
#         ↘ paused（用户，可恢复）/ cancelled（用户，终态）/ failed_final（重试退避耗尽）
# 用户终态不被 worker 过渡写回覆盖；取消清理 .part，暂停保留 .part（断点续传）。

def _get_task_or_404(task_id: int) -> dict:
    row = db.fetchone('select * from tasks where id=?', (task_id,))
    if not row:
        raise HTTPException(404, '任务不存在')
    return row


@app.post('/api/tasks/{task_id}/pause')
async def pause_task(task_id: int, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    row = _get_task_or_404(task_id)
    if row['status'] not in ('pending', 'matching', 'downloading'):
        raise HTTPException(409, f"当前状态 {row['status']} 不可暂停")
    worker = get_worker()
    if worker:
        worker.request_pause(task_id)
    # 未被认领的任务直接落终态；在途任务由 worker 在检查点收尾（保留 .part 支持断点续传）
    db.execute("update tasks set status='paused',lease_owner=NULL,lease_until=NULL,updated_at=CURRENT_TIMESTAMP "
               "where id=? and status='pending'", (task_id,))
    return {'id': task_id, 'status': 'paused'}


@app.post('/api/tasks/{task_id}/resume')
async def resume_task(task_id: int, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    row = _get_task_or_404(task_id)
    if row['status'] != 'paused':
        raise HTTPException(409, f"当前状态 {row['status']} 不可恢复")
    worker = get_worker()
    if worker:
        worker.clear_user_state(task_id)
    db.execute("update tasks set status='pending',next_run_at=null,lease_owner=NULL,lease_until=NULL,"
               "updated_at=CURRENT_TIMESTAMP where id=?", (task_id,))
    return {'id': task_id, 'status': 'pending'}


@app.post('/api/tasks/{task_id}/cancel')
async def cancel_task(task_id: int, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    """取消任务：在途下载由 worker 中断并清理 .part；不删除任何已入库音频。"""
    guard(x_fn_token)
    row = _get_task_or_404(task_id)
    if row['status'] not in ('pending', 'matching', 'downloading', 'paused'):
        raise HTTPException(409, f"当前状态 {row['status']} 无需取消")
    worker = get_worker()
    if worker:
        worker.request_cancel(task_id)
    db.execute("update tasks set status='cancelled',error='cancelled_by_user',finished_at=CURRENT_TIMESTAMP,"
               "lease_owner=NULL,lease_until=NULL,updated_at=CURRENT_TIMESTAMP where id=? and status in ('pending','paused')",
               (task_id,))
    return {'id': task_id, 'status': 'cancelled'}


@app.delete('/api/tasks/{task_id}')
async def delete_task(task_id: int, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    """删除任务：只删队列记录与尝试日志（在途下载先协作取消），不删 tracks 行、不动已入库音频文件；
    删除音乐文件属曲库管理操作，不在这里提供，避免误删曲库。"""
    guard(x_fn_token)
    row = _get_task_or_404(task_id)
    if row['status'] in ('matching', 'downloading'):
        worker = get_worker()
        if worker:
            worker.request_cancel(task_id)
    db.execute('delete from task_attempts where task_id=?', (task_id,))
    db.execute('delete from tasks where id=?', (task_id,))
    return {'deleted': task_id}


@app.post('/api/tasks/batch')
async def batch_tasks(payload: dict, x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    """批量操作：{action: pause|resume|cancel|retry|delete, ids: [...]}，单个失败不影响其余。"""
    guard(x_fn_token)
    action = str(payload.get('action') or '')
    try:
        ids = [int(i) for i in (payload.get('ids') or [])]
    except (TypeError, ValueError):
        raise HTTPException(422, 'ids 须为任务 ID 数组')
    if action not in ('pause', 'resume', 'cancel', 'retry', 'delete'):
        raise HTTPException(422, 'action 须为 pause/resume/cancel/retry/delete')
    if not ids:
        raise HTTPException(422, 'ids 为空')
    worker = get_worker()
    affected, errors = 0, []
    for task_id in ids:
        try:
            row = db.fetchone('select id,status from tasks where id=?', (task_id,))
            if not row:
                errors.append({'id': task_id, 'error': '任务不存在'})
                continue
            status = row['status']
            if action == 'pause':
                if status not in ('pending', 'matching', 'downloading'):
                    errors.append({'id': task_id, 'error': f'状态 {status} 不可暂停'})
                    continue
                if worker: worker.request_pause(task_id)
                db.execute("update tasks set status='paused',lease_owner=NULL,lease_until=NULL,updated_at=CURRENT_TIMESTAMP "
                           "where id=? and status='pending'", (task_id,))
            elif action == 'resume':
                if status != 'paused':
                    errors.append({'id': task_id, 'error': f'状态 {status} 不可恢复'})
                    continue
                if worker: worker.clear_user_state(task_id)
                db.execute("update tasks set status='pending',next_run_at=null,lease_owner=NULL,lease_until=NULL,"
                           "updated_at=CURRENT_TIMESTAMP where id=?", (task_id,))
            elif action == 'cancel':
                if status not in ('pending', 'matching', 'downloading', 'paused'):
                    errors.append({'id': task_id, 'error': f'状态 {status} 无需取消'})
                    continue
                if worker: worker.request_cancel(task_id)
                db.execute("update tasks set status='cancelled',error='cancelled_by_user',finished_at=CURRENT_TIMESTAMP,"
                           "lease_owner=NULL,lease_until=NULL,updated_at=CURRENT_TIMESTAMP where id=? and status in ('pending','paused')",
                           (task_id,))
            elif action == 'retry':
                if status not in ('failed_final', 'cancelled', 'paused'):
                    errors.append({'id': task_id, 'error': f'状态 {status} 无需重试'})
                    continue
                if worker: worker.clear_user_state(task_id)
                db.execute("update tasks set status='pending',error=null,next_run_at=null,finished_at=NULL,progress_bytes=0,"
                           "total_bytes=0,speed_bytes=0,lease_owner=NULL,lease_until=NULL,updated_at=CURRENT_TIMESTAMP where id=?",
                           (task_id,))
            elif action == 'delete':
                if status in ('matching', 'downloading') and worker:
                    worker.request_cancel(task_id)
                db.execute('delete from task_attempts where task_id=?', (task_id,))
                db.execute('delete from tasks where id=?', (task_id,))
            affected += 1
        except Exception as exc:
            errors.append({'id': task_id, 'error': str(exc)[:120]})
    return {'action': action, 'affected': affected, 'errors': errors}


@app.post('/api/tasks/cleanup-parts')
async def cleanup_parts_files(x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    """清扫曲库/缓存目录中所有 .part 半截临时文件；完整音频文件永远不动。"""
    guard(x_fn_token)
    deleted = 0
    try:
        candidates = list(music_root.rglob('*.part.*'))
    except OSError:
        candidates = []
    for part in candidates:
        try:
            part.unlink(missing_ok=True)
            deleted += 1
        except OSError:
            pass
    return {'deleted': deleted}


@app.post('/api/library/scan')
async def scan(x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    return await refresh_library_index()


async def refresh_library_index():
    async with library_scan_lock:
        app.state.library_index_status = {'status': 'running'}
        try:
            result = await asyncio.to_thread(index_music, str(music_root), db)
            app.state.library_index_status = result
            return result
        except Exception as exc:
            result = {'status': 'error', 'detail': str(exc)[:200]}
            app.state.library_index_status = result
            raise


@app.get('/api/library/status')
async def library_status(x_fn_token: str | None = Header(default=None, alias='X-FN-Token')):
    guard(x_fn_token)
    return getattr(app.state, 'library_index_status', {'status': 'not_started'})
