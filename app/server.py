from __future__ import annotations

import asyncio
import os
from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse

from .db import Database
from .library import index_music
from .models import TrackQuery
from .phase23 import init_phase23
from .sources import SourceRegistry
from .worker import TaskWorker
from .web import page

app = FastAPI(title='fnmusic-flow', version='0.3.0')
db, sources = Database(), SourceRegistry()
init_phase23(db)
worker: TaskWorker | None = None
worker_task: asyncio.Task | None = None

def public_source(row):
    return {'name': row.name, 'kind': row.kind, 'enabled': row.enabled}

@app.middleware('http')
async def token_guard(request: Request, call_next):
    token = os.getenv('FN_TOKEN')
    if token and request.url.path.startswith('/api/') and request.headers.get('X-FN-Token') != token:
        return JSONResponse({'detail':'invalid token'}, status_code=401)
    return await call_next(request)

@app.on_event('startup')
async def startup():
    global worker, worker_task
    for row in db.fetchall('SELECT name,script_content FROM sources WHERE enabled=1'):
        try: sources.register_lx_script(row['script_content'], row['name'])
        except ValueError: pass
    worker = TaskWorker(db, sources, os.getenv('MUSIC_ROOT','/music'))
    worker_task = asyncio.create_task(worker.run_forever())

@app.on_event('shutdown')
async def shutdown():
    if worker_task: worker_task.cancel()

@app.get('/')
async def index(): return page()

@app.get('/health')
async def health():
    return {'status':'ok','sources':await sources.health(),'tasks':db.fetchall('SELECT status,COUNT(*) count FROM tasks GROUP BY status')}

@app.get('/api/sources')
async def list_sources(): return [public_source(x) for x in sources.list()]

@app.post('/api/sources/import')
async def import_source(file: UploadFile = File(...), name: str|None = None):
    if not file.filename or not file.filename.lower().endswith('.js'): raise HTTPException(400,'请上传 .js 音源文件')
    try: content = (await file.read()).decode('utf-8'); definition = sources.register_lx_script(content, name)
    except (UnicodeDecodeError, ValueError) as exc: raise HTTPException(422,str(exc)) from exc
    db.execute('INSERT OR REPLACE INTO sources(name,kind,script_content,enabled) VALUES(?,?,?,1)', (definition.name,definition.kind,content))
    return public_source(definition)

@app.post('/api/sources/{name}/resolve')
async def resolve(name: str, query: TrackQuery):
    try: return {'url':await sources.resolve(name,query)}
    except KeyError: raise HTTPException(404,'音源不存在')
    except Exception as exc: raise HTTPException(502,str(exc)) from exc

@app.post('/api/tracks')
async def create_track(track: dict):
    db.execute('INSERT OR IGNORE INTO tracks(platform,song_id,title,artist,album,duration_ms,quality) VALUES(?,?,?,?,?,?,?)', (track['platform'],track['song_id'],track['title'],track.get('artist',''),track.get('album',''),track.get('duration_ms'),track.get('quality') or 'hires'))
    row=db.fetchone('SELECT id FROM tracks WHERE platform=? AND song_id=?',(track['platform'],track['song_id']))
    db.execute('INSERT INTO tasks(track_id,task_type,priority) VALUES(?,?,?)',(row['id'],track.get('task_type','manual_single'),track.get('priority',3)))
    task=db.fetchone('SELECT id FROM tasks WHERE track_id=? ORDER BY id DESC LIMIT 1',(row['id'],))
    return {'track_id':row['id'],'task_id':task['id']}

@app.get('/api/tasks')
async def tasks(): return db.fetchall('SELECT * FROM tasks ORDER BY id DESC')

@app.post('/api/library/scan')
async def scan_library():
    return await asyncio.to_thread(index_music, os.getenv('MUSIC_ROOT','/music'), db)
