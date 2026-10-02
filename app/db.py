from pathlib import Path
import sqlite3
from contextlib import contextmanager
from threading import Lock

SCHEMA = """
CREATE TABLE IF NOT EXISTS sources(name TEXT PRIMARY KEY, kind TEXT NOT NULL, script_content TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS tracks(id INTEGER PRIMARY KEY, platform TEXT NOT NULL, song_id TEXT NOT NULL, title TEXT NOT NULL, artist TEXT NOT NULL DEFAULT '', album TEXT NOT NULL DEFAULT '', duration_ms INTEGER, quality TEXT NOT NULL DEFAULT 'hires', cover_url TEXT, cover_status TEXT NOT NULL DEFAULT 'pending', cover_error TEXT, file_path TEXT, file_hash TEXT, status TEXT NOT NULL DEFAULT 'pending', created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, UNIQUE(platform,song_id));
CREATE TABLE IF NOT EXISTS tasks(id INTEGER PRIMARY KEY, track_id INTEGER NOT NULL REFERENCES tracks(id), task_type TEXT NOT NULL, priority INTEGER NOT NULL DEFAULT 5, status TEXT NOT NULL DEFAULT 'pending', source_name TEXT, attempts INTEGER NOT NULL DEFAULT 0, error TEXT, next_run_at TEXT, progress_bytes INTEGER NOT NULL DEFAULT 0, total_bytes INTEGER NOT NULL DEFAULT 0, speed_bytes INTEGER NOT NULL DEFAULT 0, lease_owner TEXT, lease_until TEXT, started_at TEXT, finished_at TEXT, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS task_attempts(id INTEGER PRIMARY KEY, task_id INTEGER NOT NULL REFERENCES tasks(id), source_name TEXT NOT NULL, status TEXT NOT NULL, detail TEXT, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
CREATE INDEX IF NOT EXISTS idx_tasks_queue ON tasks(status,priority,id);
"""

class Database:
    def __init__(self, path: str = "/config/fnmusic.db"):
        self.path = Path(path); self.path.parent.mkdir(parents=True, exist_ok=True); self.lock = Lock()
        conn = self.connect()
        try:
            conn.executescript(SCHEMA)
            columns = {row[1] for row in conn.execute('PRAGMA table_info(tasks)').fetchall()}
            if 'next_run_at' not in columns:
                conn.execute('ALTER TABLE tasks ADD COLUMN next_run_at TEXT')
            # 2026-10-01（社区版 1.2.0）下载管理状态机：进度 / 速度 / 租约 / 时间戳。
            # 租约（lease_owner+lease_until）解决并发槽认领与 worker 崩溃后的僵尸任务回收。
            for name, definition in {
                'progress_bytes': 'INTEGER NOT NULL DEFAULT 0',
                'total_bytes': 'INTEGER NOT NULL DEFAULT 0',
                'speed_bytes': 'INTEGER NOT NULL DEFAULT 0',
                'lease_owner': 'TEXT',
                'lease_until': 'TEXT',
                'started_at': 'TEXT',
                'finished_at': 'TEXT',
            }.items():
                if name not in columns:
                    conn.execute(f'ALTER TABLE tasks ADD COLUMN {name} {definition}')
            columns = {row[1] for row in conn.execute('PRAGMA table_info(tasks)').fetchall()}
            track_columns = {row[1] for row in conn.execute('PRAGMA table_info(tracks)').fetchall()}
            for name, definition in {
                'cover_url': 'TEXT',
                'cover_status': "TEXT NOT NULL DEFAULT 'pending'",
                'cover_error': 'TEXT',
            }.items():
                if name not in track_columns:
                    conn.execute(f'ALTER TABLE tracks ADD COLUMN {name} {definition}')
            # 音源管理：最近一次健康探测结果与故障切换排序权重
            source_columns = {row[1] for row in conn.execute('PRAGMA table_info(sources)').fetchall()}
            for name, definition in {
                'status': "TEXT NOT NULL DEFAULT 'unknown'",
                'last_check': 'TEXT',
                'last_detail': 'TEXT',
                'sort_order': 'INTEGER NOT NULL DEFAULT 100',
            }.items():
                if name not in source_columns:
                    conn.execute(f'ALTER TABLE sources ADD COLUMN {name} {definition}')
            # 2026-09-29（1.0.8）音质迁移：CREATE TABLE IF NOT EXISTS **不会修改已存在
            # 表的列默认值** —— 1.0.6 把 SCHEMA 默认值改成 'hires'，但线上老库的
            # tracks.quality 默认值仍是 '320k'（pragma 实测），且存量行全部被写成
            # 320k；worker 用 track.quality 请求音源 → 无损音源也只能下 mp3。
            # 迁移判定：仅当表默认值还是旧的有损档位时执行（幂等，新库不触发），
            # 把存量 320k/128k 统一升到 hires；拿不到无损时由音源层自动降级。
            qcol = next((r for r in conn.execute('PRAGMA table_info(tracks)').fetchall()
                         if r[1] == 'quality'), None)
            if qcol and str(qcol[4] or '').strip("'") in ('320k', '128k'):
                conn.execute("update tracks set quality='hires' where quality in ('320k','128k')")
            duplicate_ids = [
                row[0] for row in conn.execute(
                    """select id from tasks
                       where task_type='favorite' and status<>'archived'
                         and id not in (
                           select max(id) from tasks
                           where task_type='favorite' and status<>'archived'
                           group by track_id
                         )"""
                ).fetchall()
            ]
            if duplicate_ids:
                placeholders = ','.join('?' for _ in duplicate_ids)
                conn.execute(f'delete from task_attempts where task_id in ({placeholders})', duplicate_ids)
                conn.execute(f'delete from tasks where id in ({placeholders})', duplicate_ids)
            conn.commit()
        finally:
            conn.close()
    def connect(self):
        conn = sqlite3.connect(self.path, check_same_thread=False); conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL"); conn.execute("PRAGMA busy_timeout=5000"); return conn
    def execute(self, sql, params=()):
        with self.lock:
            conn = self.connect()
            try:
                cur = conn.execute(sql, params)
                conn.commit()
                return cur.lastrowid
            finally:
                conn.close()
    def execute_rc(self, sql, params=()):
        """返回受影响行数（条件认领/状态守卫写回需要区分 0 行与 1 行）。"""
        with self.lock:
            conn = self.connect()
            try:
                cur = conn.execute(sql, params)
                conn.commit()
                return cur.rowcount
            finally:
                conn.close()
    def executemany(self, sql, seq):
        """批量写入：单连接单事务，避免逐条 execute 造成的多次连接+提交开销。"""
        with self.lock:
            conn = self.connect()
            try:
                cur = conn.executemany(sql, seq)
                conn.commit()
                return cur.rowcount
            finally:
                conn.close()
    @contextmanager
    def transaction(self):
        """多语句事务：同一连接内完成一批读写，只在结束时提交一次。"""
        with self.lock:
            conn = self.connect()
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()
    def fetchall(self, sql, params=()):
        conn = self.connect()
        try:
            return [dict(x) for x in conn.execute(sql, params).fetchall()]
        finally:
            conn.close()
    def fetchone(self, sql, params=()):
        rows = self.fetchall(sql, params); return rows[0] if rows else None
