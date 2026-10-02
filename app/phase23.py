from datetime import datetime, timedelta, timezone


PHASE23_SCHEMA = """
CREATE TABLE IF NOT EXISTS provider_configs(provider TEXT PRIMARY KEY, endpoint TEXT NOT NULL, cookie TEXT, enabled INTEGER NOT NULL DEFAULT 1, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS recommendation_runs(id INTEGER PRIMARY KEY AUTOINCREMENT, provider TEXT NOT NULL, kind TEXT NOT NULL, status TEXT NOT NULL, added_count INTEGER NOT NULL DEFAULT 0, error TEXT, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS playlists(id INTEGER PRIMARY KEY AUTOINCREMENT, provider TEXT NOT NULL, playlist_id TEXT NOT NULL, title TEXT NOT NULL DEFAULT '', source_url TEXT, kind TEXT NOT NULL DEFAULT 'playlist', last_sync_at TEXT, UNIQUE(provider,playlist_id));
CREATE TABLE IF NOT EXISTS playlist_items(playlist_id INTEGER NOT NULL, position INTEGER NOT NULL, platform TEXT NOT NULL, song_id TEXT NOT NULL, title TEXT NOT NULL, artist TEXT NOT NULL DEFAULT '', album TEXT NOT NULL DEFAULT '', duration_ms INTEGER, cover_url TEXT);
CREATE TABLE IF NOT EXISTS subscriptions(id INTEGER PRIMARY KEY AUTOINCREMENT, provider TEXT NOT NULL, playlist_id TEXT NOT NULL, kind TEXT NOT NULL DEFAULT 'playlist', enabled INTEGER NOT NULL DEFAULT 1, interval_minutes INTEGER NOT NULL DEFAULT 1440, schedule_time TEXT, next_run_at TEXT, last_run_at TEXT, last_status TEXT NOT NULL DEFAULT 'pending', last_error TEXT, target_title TEXT, UNIQUE(provider,playlist_id,kind));
CREATE TABLE IF NOT EXISTS fnos_capabilities(name TEXT PRIMARY KEY, status TEXT NOT NULL DEFAULT 'not_verified', detail TEXT, checked_at TEXT);
CREATE TABLE IF NOT EXISTS accounts(provider TEXT PRIMARY KEY, display_name TEXT, endpoint TEXT, cookie TEXT, status TEXT NOT NULL DEFAULT 'disconnected', last_checked_at TEXT, error TEXT, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS playlist_covers(playlist_row_id INTEGER PRIMARY KEY REFERENCES playlists(id), cover_url TEXT, description TEXT, owner TEXT, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS sync_settings(name TEXT PRIMARY KEY, enabled INTEGER NOT NULL DEFAULT 1, interval_minutes INTEGER NOT NULL DEFAULT 30, last_run_at TEXT, last_error TEXT, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS daily_recommendations(provider TEXT NOT NULL, song_id TEXT NOT NULL, title TEXT NOT NULL, artist TEXT NOT NULL DEFAULT '', album TEXT NOT NULL DEFAULT '', duration_ms INTEGER, cover_url TEXT, fetched_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(provider,song_id));
CREATE TABLE IF NOT EXISTS library_files(path TEXT PRIMARY KEY, size INTEGER NOT NULL, mtime_ns INTEGER NOT NULL, title TEXT NOT NULL DEFAULT '', artist TEXT NOT NULL DEFAULT '', album TEXT NOT NULL DEFAULT '', norm_title TEXT NOT NULL DEFAULT '', norm_artist TEXT NOT NULL DEFAULT '', duration_ms INTEGER, file_hash TEXT, artwork_status TEXT NOT NULL DEFAULT 'unknown', artwork_checked_at TEXT, artwork_error TEXT, indexed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS artwork_jobs(id INTEGER PRIMARY KEY AUTOINCREMENT, status TEXT NOT NULL DEFAULT 'pending', total INTEGER NOT NULL DEFAULT 0, checked INTEGER NOT NULL DEFAULT 0, repaired INTEGER NOT NULL DEFAULT 0, already_ok INTEGER NOT NULL DEFAULT 0, failed INTEGER NOT NULL DEFAULT 0, current_path TEXT, error TEXT, started_at TEXT, completed_at TEXT, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS push_targets(id INTEGER PRIMARY KEY AUTOINCREMENT, provider TEXT NOT NULL, playlist_id TEXT NOT NULL, playlist_row_id INTEGER, kind TEXT NOT NULL DEFAULT 'playlist', target_title TEXT NOT NULL, fnos_guid TEXT, status TEXT NOT NULL DEFAULT 'active', retention_mode TEXT NOT NULL DEFAULT 'permanent', expires_at TEXT, scheduled_push_at TEXT, scheduled_auto_sync INTEGER, scheduled_interval_minutes INTEGER, last_pushed_at TEXT, last_error TEXT, run_started_at TEXT, run_completed_at TEXT, run_status TEXT NOT NULL DEFAULT 'idle', run_stage TEXT, run_total INTEGER NOT NULL DEFAULT 0, run_matched INTEGER NOT NULL DEFAULT 0, run_missing INTEGER NOT NULL DEFAULT 0, run_duration_seconds REAL, run_last_checked_at TEXT, run_scan_requested_at TEXT, run_attempts INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, UNIQUE(provider,playlist_id,kind));
CREATE TABLE IF NOT EXISTS push_history(id INTEGER PRIMARY KEY AUTOINCREMENT, target_id INTEGER NOT NULL REFERENCES push_targets(id), action TEXT NOT NULL, status TEXT NOT NULL, detail TEXT, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS push_item_status(target_id INTEGER NOT NULL REFERENCES push_targets(id), platform TEXT NOT NULL, song_id TEXT NOT NULL, position INTEGER NOT NULL DEFAULT 0, title TEXT NOT NULL, artist TEXT NOT NULL DEFAULT '', status TEXT NOT NULL, reason TEXT, fnos_track_guid TEXT, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(target_id,platform,song_id));
CREATE TABLE IF NOT EXISTS daily_cache(track_id INTEGER PRIMARY KEY REFERENCES tracks(id), path TEXT, cached_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, expires_at TEXT NOT NULL DEFAULT (datetime('now','+3 days')), play_count INTEGER NOT NULL DEFAULT 0, last_played_at TEXT, status TEXT NOT NULL DEFAULT 'active', promoted_at TEXT, removed_at TEXT, reason TEXT);
CREATE TABLE IF NOT EXISTS daily_cache_plays(event_key TEXT PRIMARY KEY,track_id INTEGER NOT NULL REFERENCES tracks(id),played_at TEXT,created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
CREATE INDEX IF NOT EXISTS idx_library_files_title ON library_files(norm_title);
CREATE INDEX IF NOT EXISTS idx_library_files_hash ON library_files(file_hash);
CREATE INDEX IF NOT EXISTS idx_push_targets_status ON push_targets(status,expires_at,scheduled_push_at);
CREATE INDEX IF NOT EXISTS idx_push_history_target ON push_history(target_id,id DESC);
CREATE INDEX IF NOT EXISTS idx_push_item_status_target ON push_item_status(target_id,status,position);
CREATE INDEX IF NOT EXISTS idx_daily_cache_expiry ON daily_cache(status,expires_at);
"""

# 「为我推荐」推荐层（for_you.py）：池 / 运行记录 / 冷却历史 / 黑名单。
# 全部 CREATE TABLE IF NOT EXISTS + UNIQUE 约束，幂等可重复执行；
# 旧库升级只新增表，不动任何现有表。
FOR_YOU_SCHEMA = """
CREATE TABLE IF NOT EXISTS for_you_pool(id INTEGER PRIMARY KEY AUTOINCREMENT, provider TEXT NOT NULL, playlist_id TEXT NOT NULL, title TEXT NOT NULL DEFAULT '', description TEXT NOT NULL DEFAULT '', cover_url TEXT, owner TEXT NOT NULL DEFAULT '', item_count INTEGER NOT NULL DEFAULT 0, play_count INTEGER NOT NULL DEFAULT 0, update_time TEXT, coarse_score REAL NOT NULL DEFAULT 0, deep_score REAL NOT NULL DEFAULT 0, total_score REAL NOT NULL DEFAULT 0, taste_hits INTEGER NOT NULL DEFAULT 0, hard_hits INTEGER NOT NULL DEFAULT 0, dislike_hits INTEGER NOT NULL DEFAULT 0, scored_tracks INTEGER NOT NULL DEFAULT 0, taste_rate REAL NOT NULL DEFAULT 0, hard_rate REAL NOT NULL DEFAULT 0, dislike_rate REAL NOT NULL DEFAULT 0, coverage REAL NOT NULL DEFAULT 0, reasons TEXT, risks TEXT, seed_keyword TEXT NOT NULL DEFAULT '', batch_id INTEGER, playlist_type TEXT NOT NULL DEFAULT 'general', dominant_artist TEXT NOT NULL DEFAULT '', added_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, UNIQUE(provider,playlist_id));
CREATE TABLE IF NOT EXISTS for_you_runs(id INTEGER PRIMARY KEY AUTOINCREMENT, trigger TEXT NOT NULL DEFAULT 'scheduled', status TEXT NOT NULL DEFAULT 'running', run_date TEXT NOT NULL, started_at TEXT, finished_at TEXT, candidates INTEGER NOT NULL DEFAULT 0, deep_checked INTEGER NOT NULL DEFAULT 0, accepted INTEGER NOT NULL DEFAULT 0, swapped_in INTEGER NOT NULL DEFAULT 0, swapped_out INTEGER NOT NULL DEFAULT 0, hard_filtered INTEGER NOT NULL DEFAULT 0, low_match INTEGER NOT NULL DEFAULT 0, errors TEXT, profile_version TEXT, next_rotation_at TEXT);
CREATE TABLE IF NOT EXISTS for_you_history(id INTEGER PRIMARY KEY AUTOINCREMENT, provider TEXT NOT NULL, playlist_id TEXT NOT NULL, title TEXT NOT NULL DEFAULT '', added_at TEXT, removed_at TEXT NOT NULL, batch_id INTEGER, note TEXT NOT NULL DEFAULT '', rotation_count INTEGER NOT NULL DEFAULT 1, UNIQUE(provider,playlist_id));
CREATE TABLE IF NOT EXISTS for_you_blacklist(id INTEGER PRIMARY KEY AUTOINCREMENT, provider TEXT NOT NULL, playlist_id TEXT NOT NULL, title TEXT NOT NULL DEFAULT '', reason TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, UNIQUE(provider,playlist_id));
CREATE INDEX IF NOT EXISTS idx_for_you_pool_batch ON for_you_pool(batch_id);
CREATE INDEX IF NOT EXISTS idx_for_you_history_removed ON for_you_history(removed_at);
CREATE INDEX IF NOT EXISTS idx_for_you_runs_date ON for_you_runs(run_date,status);
"""

# 跨进程轮换互斥（审核 P2）：同一「运行日」最多一个 running 行。
# 部分唯一索引使 _open_run 的 INSERT 成为原子认领——两个进程同时开轮时，
# 后者会因唯一冲突失败而不是开出第二轮突破每日 15 个预算。
# 注意：索引必须单独建——老库若残留多条 running 行，先去重再建才不报错。
FOR_YOU_SCHEMA_INDEX = """
CREATE UNIQUE INDEX IF NOT EXISTS idx_for_you_runs_running_day ON for_you_runs(run_date) WHERE status='running';
"""

def init_phase23(db):
    with db.lock:
        conn = db.connect()
        try:
            conn.executescript(PHASE23_SCHEMA)
            # 「为我推荐」表幂等新增（旧库升级只加表，不清库）。
            conn.executescript(FOR_YOU_SCHEMA)
            # round3 修订：for_you_pool 增加类型/主导歌手列（旧表 ALTER 兼容）
            pool_cols = {row[1] for row in conn.execute('PRAGMA table_info(for_you_pool)').fetchall()}
            for column, ddl in (('playlist_type', "TEXT NOT NULL DEFAULT 'general'"),
                                ('dominant_artist', "TEXT NOT NULL DEFAULT ''")):
                if column not in pool_cols:
                    conn.execute(f'alter table for_you_pool add column {column} {ddl}')
            # 老库若因中断残留多条 running 行，先去重（保留最新一条，其余判失败），
            # 再建「每日至多一个 running」的部分唯一索引。
            conn.execute(
                """update for_you_runs set status='failed',finished_at=CURRENT_TIMESTAMP,
                   errors=coalesce(errors,'[{"stage":"run","error":"superseded_running_row"}]')
                   where status='running' and id not in (
                     select max(id) from for_you_runs where status='running' group by run_date)""")
            conn.executescript(FOR_YOU_SCHEMA_INDEX)
            # Upgrade databases created by the earlier prototype in place.
            cols = {row[1] for row in conn.execute('PRAGMA table_info(accounts)').fetchall()}
            if 'endpoint' not in cols:
                conn.execute('ALTER TABLE accounts ADD COLUMN endpoint TEXT')
            table_upgrades = {
                'playlists': {'kind': "TEXT NOT NULL DEFAULT 'playlist'"},
                'playlist_items': {'cover_url': 'TEXT'},
                'daily_recommendations': {'cover_url': 'TEXT'},
                'library_files': {
                    'artwork_status': "TEXT NOT NULL DEFAULT 'unknown'",
                    'artwork_checked_at': 'TEXT',
                    'artwork_error': 'TEXT',
                },
            }
            for table, upgrades in table_upgrades.items():
                table_cols = {row[1] for row in conn.execute(f'PRAGMA table_info({table})').fetchall()}
                for name, definition in upgrades.items():
                    if name not in table_cols:
                        conn.execute(f'ALTER TABLE {table} ADD COLUMN {name} {definition}')
            subscription_cols = {row[1] for row in conn.execute('PRAGMA table_info(subscriptions)').fetchall()}
            subscription_upgrades = {
                'interval_minutes': 'INTEGER NOT NULL DEFAULT 1440',
                'schedule_time': 'TEXT',
                'next_run_at': 'TEXT',
                'last_status': "TEXT NOT NULL DEFAULT 'pending'",
                'last_error': 'TEXT',
                'target_title': 'TEXT',
            }
            for name, definition in subscription_upgrades.items():
                if name not in subscription_cols:
                    conn.execute(f'ALTER TABLE subscriptions ADD COLUMN {name} {definition}')
            push_target_cols = {row[1] for row in conn.execute('PRAGMA table_info(push_targets)').fetchall()}
            push_target_upgrades = {
                'scheduled_auto_sync': 'INTEGER',
                'scheduled_interval_minutes': 'INTEGER',
                'run_started_at': 'TEXT',
                'run_completed_at': 'TEXT',
                'run_status': "TEXT NOT NULL DEFAULT 'idle'",
                'run_stage': 'TEXT',
                'run_total': 'INTEGER NOT NULL DEFAULT 0',
                'run_matched': 'INTEGER NOT NULL DEFAULT 0',
                'run_missing': 'INTEGER NOT NULL DEFAULT 0',
                'run_duration_seconds': 'REAL',
                'run_last_checked_at': 'TEXT',
                'run_scan_requested_at': 'TEXT',
                'run_attempts': 'INTEGER NOT NULL DEFAULT 0',
            }
            for name, definition in push_target_upgrades.items():
                if name not in push_target_cols:
                    conn.execute(f'ALTER TABLE push_targets ADD COLUMN {name} {definition}')
            # Daily recommendation polling starts at 06:00 Beijing time.  The
            # scheduler retries unchanged upstream snapshots every 30 minutes.
            conn.execute("update subscriptions set schedule_time='06:00' where kind='daily' and (schedule_time is null or trim(schedule_time)='' or schedule_time='10:00')")
            beijing = timezone(timedelta(hours=8))
            now_local = datetime.now(beijing)
            next_daily = now_local.replace(hour=6, minute=0, second=0, microsecond=0)
            if next_daily <= now_local:
                next_daily += timedelta(days=1)
            next_daily_utc = next_daily.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            conn.execute(
                '''update subscriptions set next_run_at=?
                   where kind='daily' and schedule_time='06:00'
                     and coalesce(last_status,'')<>'waiting_update'
                     and (next_run_at is null or strftime('%H:%M',datetime(next_run_at,'+8 hours'))<>'06:00')''',
                (next_daily_utc,),
            )
            conn.execute("update subscriptions set kind='chart',interval_minutes=10080 where kind='playlist' and playlist_id like 'chart:%'")
            conn.execute(
                '''insert or ignore into push_targets(provider,playlist_id,playlist_row_id,kind,target_title,status,retention_mode,last_pushed_at)
                   select s.provider,s.playlist_id,p.id,s.kind,coalesce(s.target_title,p.title,s.playlist_id),
                          case when s.enabled=1 then 'active' else 'cancelled' end,'permanent',coalesce(s.last_run_at,CURRENT_TIMESTAMP)
                   from subscriptions s left join playlists p on p.provider=s.provider and p.playlist_id=s.playlist_id'''
            )
            conn.commit()
        finally:
            conn.close()

def normalize_rows(payload, provider):
    rows = payload.get('songs') or payload.get('data') or payload.get('playlist') or [] if isinstance(payload, dict) else payload
    rows = rows if isinstance(rows, list) else rows.get('songs', [])
    result = []
    for row in rows:
        song_id = row.get('id') or row.get('songmid') or row.get('mid') or row.get('hash')
        title = row.get('name') or row.get('title') or ''
        if song_id and title:
            result.append({'platform': provider, 'song_id': str(song_id), 'title': str(title), 'artist': str(row.get('artist') or row.get('singer') or ''), 'album': str(row.get('album') or ''), 'duration_ms': row.get('duration') or row.get('interval')})
    return result
