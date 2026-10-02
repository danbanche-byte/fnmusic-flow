import asyncio
import os
from .app import app, db, sources, music_root, refresh_library_index
from . import full as _full_routes
from . import worker as worker_module
from .worker import TaskWorker
from .scheduler import SyncScheduler


async def _library_index_loop():
    await asyncio.sleep(30)
    interval = max(60, int(os.getenv('LIBRARY_SCAN_INTERVAL_MINUTES', '360'))) * 60
    while True:
        try:
            await refresh_library_index()
        except Exception:
            pass
        await asyncio.sleep(interval)

@app.on_event('startup')
async def start_worker():
    worker = TaskWorker(db, sources, str(music_root))
    worker.recover_inflight()
    # 单例注册：pause/cancel/delete 端点经 worker_module.get_worker() 设置协作式事件
    worker_module.TASK_WORKER = worker
    app.state.worker_task = asyncio.create_task(worker.run_forever())
    app.state.sync_task = asyncio.create_task(SyncScheduler(db).run_forever())
    app.state.push_pipeline_task = asyncio.create_task(SyncScheduler(db).run_push_pipeline_forever())
    app.state.library_index_task = asyncio.create_task(_library_index_loop())
    # 1.0.10：周期性飞牛 token 探活自愈（防止 token 行被飞牛侧删除后长期不自知）
    app.state.fnos_token_guard_task = asyncio.create_task(SyncScheduler(db).run_fnos_token_guard_forever())

@app.on_event('shutdown')
async def stop_worker():
    task = getattr(app.state, 'worker_task', None)
    if task: task.cancel()
    task = getattr(app.state, 'sync_task', None)
    if task: task.cancel()
    task = getattr(app.state, 'push_pipeline_task', None)
    if task: task.cancel()
    task = getattr(app.state, 'library_index_task', None)
    if task: task.cancel()
    task = getattr(app.state, 'fnos_token_guard_task', None)
    if task: task.cancel()
