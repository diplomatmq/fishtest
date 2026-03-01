import os
import sqlite3
import logging
import random
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from config import DB_PATH

# Optional Postgres support
try:
    import psycopg2
except Exception:
    psycopg2 = None

class PostgresConnWrapper:
    """A thin wrapper exposing a sqlite-like connection API for psycopg2.
    It provides execute(), cursor(), commit(), and context-manager support.
    """
    def __init__(self, dsn: str):
        if not psycopg2:
            raise RuntimeError('psycopg2 is required for Postgres support')
        # accept full DATABASE_URL or components
        self._conn = psycopg2.connect(dsn)

    def _translate_sql(self, sql: str) -> str:
        s = sql
        # normalize whitespace for pattern matching
        import re
        # Replace SQLite AUTOINCREMENT with Postgres serial primary key
        s = re.sub(r"INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT", 'SERIAL PRIMARY KEY', s, flags=re.IGNORECASE)
        # Also handle bare AUTOINCREMENT token
        s = re.sub(r"AUTOINCREMENT", '', s, flags=re.IGNORECASE)
        # Convert sqlite '?' placeholders to psycopg2 '%s'
        s = s.replace('?', '%s')
        # Replace sqlite datetime(...) with inner expression (Postgres uses native timestamp types)
        s = re.sub(r"datetime\s*\(([^)]+)\)", r"\1", s, flags=re.IGNORECASE)
        # remove sqlite-specific PRAGMA statements
        if s.strip().upper().startswith('PRAGMA'):
            return ''
        # translate INSERT OR IGNORE -> INSERT ... ON CONFLICT DO NOTHING
        if 'INSERT OR IGNORE' in s.upper():
            # simple replacement: remove OR IGNORE and append ON CONFLICT DO NOTHING
            # append only if not already present
            s = s.replace('INSERT OR IGNORE', 'INSERT')
            if 'ON CONFLICT' not in s.upper():
                s = s.rstrip().rstrip(';') + ' ON CONFLICT DO NOTHING;'

        # translate INSERT OR REPLACE for common tables to Postgres upsert
        # Use a robust parser for matching parentheses instead of a fragile regex,
        # because VALUES(...) can contain nested parentheses (e.g. COALESCE, SELECT).
        try:
            import re
            m = re.search(r"INSERT\s+OR\s+REPLACE\s+INTO\s+(\w+)", s, re.IGNORECASE)
            if m:
                table = m.group(1)
                # find the first '(' after the table name for columns
                start_cols = s.find('(', m.end())
                if start_cols != -1:
                    # find matching ')' for cols
                    depth = 0
                    end_cols = None
                    for idx in range(start_cols, len(s)):
                        ch = s[idx]
                        if ch == '(':
                            depth += 1
                        elif ch == ')':
                            depth -= 1
                            if depth == 0:
                                end_cols = idx
                                break
                    if end_cols:
                        cols_text = s[start_cols+1:end_cols]
                        cols = [c.strip() for c in cols_text.split(',')]
                        # Find VALUES keyword after end_cols
                        vals_kw = re.search(r"VALUES\s*\(", s[end_cols:], re.IGNORECASE)
                        if vals_kw:
                            start_vals = end_cols + vals_kw.start() + s[end_cols+vals_kw.start():].find('(')
                            # find matching ')' for vals, accounting for nesting
                            depth = 0
                            end_vals = None
                            for idx in range(start_vals, len(s)):
                                ch = s[idx]
                                if ch == '(':
                                    depth += 1
                                elif ch == ')':
                                    depth -= 1
                                    if depth == 0:
                                        end_vals = idx
                                        break
                            if end_vals:
                                vals = s[start_vals+1:end_vals]
                                # mapping of tables -> conflict target
                                conflict_map = {
                                    'baits': 'name',
                                    'fish': 'name',
                                    'player_baits': 'user_id, bait_name',
                                    'player_nets': 'user_id, net_name',
                                    'player_rods': 'user_id, rod_name',
                                    'chat_configs': 'chat_id',
                                    'user_ref_links': 'user_id',
                                }
                                conflict_cols = conflict_map.get(table.lower())
                                if conflict_cols:
                                    updates = ', '.join([f"{col} = EXCLUDED.{col}" for col in cols if col])
                                    s = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({vals}) ON CONFLICT ({conflict_cols}) DO UPDATE SET {updates};"
        except Exception:
            # fallback to original behavior on any parse error
            pass
        # psycopg2 uses Python %-format-style param interpolation; stray '%' in SQL
        # (e.g. LIKE '%Все%') will be treated as format specifiers and cause errors.
        # Preserve '%s' placeholders, escape other '%' by doubling them.
        if '%s' in s:
            s = s.replace('%s', '__PG_PLACEHOLDER__')
            s = s.replace('%', '%%')
            s = s.replace('__PG_PLACEHOLDER__', '%s')
        else:
            s = s.replace('%', '%%')

        return s

    def execute(self, sql: str, params=None):
        sql = sql or ''
        # Short-circuit sqlite-specific sqlite_master queries which don't exist in Postgres
        try:
            if 'sqlite_master' in sql.lower():
                return FakeCursor([])
        except Exception:
            pass
        # Handle PRAGMA table_info(...) emulation
        if sql.strip().upper().startswith('PRAGMA TABLE_INFO'):
            # extract table name
            import re
            m = re.search(r"PRAGMA\s+table_info\(([^)]+)\)", sql, re.IGNORECASE)
            table = m.group(1).strip(' \"') if m else None
            cur = self._conn.cursor()
            if table:
                cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name=%s ORDER BY ordinal_position", (table,))
                cols = cur.fetchall()
                # emulate sqlite pragma rows: (cid, name, type, notnull, dflt_value, pk)
                rows = []
                for i, (colname,) in enumerate(cols):
                    rows.append((i, colname, None, None, None, 0))
                return FakeCursor(rows)
            return FakeCursor([])

        out_sql = self._translate_sql(sql)
        if not out_sql:
            return FakeCursor([])

        cur = self._conn.cursor()
        # psycopg2 expects a sequence/tuple for parameters
        try:
            if params is not None:
                # convert list->tuple for psycopg2
                if isinstance(params, list):
                    params = tuple(params)
                try:
                    cur.execute(out_sql, params)
                except Exception:
                    logger.exception("DB execute failed. SQL: %s PARAMS: %s", out_sql, params)
                    raise
            else:
                try:
                    cur.execute(out_sql)
                except Exception:
                    logger.exception("DB execute failed. SQL: %s", out_sql)
                    raise
        except Exception:
            # re-raise so caller sees DB errors
            raise
        return cur

    def cursor(self):
        parent = self

        class _CursorWrapper:
            def __init__(self):
                self._last = None

            @property
            def rowcount(self):
                try:
                    return getattr(self._last, 'rowcount', -1)
                except Exception:
                    return -1

            @property
            def lastrowid(self):
                try:
                    return getattr(self._last, 'lastrowid', None)
                except Exception:
                    return None

            def execute(self, sql, params=None):
                # Delegate to the parent.execute so translations and PRAGMA emulation apply
                self._last = parent.execute(sql, params)
                return self._last

            def executemany(self, sql, seq_of_params):
                # executemany isn't used heavily; emulate by executing in a loop so translations apply
                last = None
                for params in seq_of_params:
                    last = parent.execute(sql, params)
                self._last = last
                return last

            def fetchall(self):
                try:
                    return self._last.fetchall() if self._last is not None else []
                except Exception:
                    return []

            def fetchone(self):
                try:
                    return self._last.fetchone() if self._last is not None else None
                except Exception:
                    return None

            @property
            def description(self):
                try:
                    return getattr(self._last, 'description', None)
                except Exception:
                    return None

            def __iter__(self):
                return iter(self._last) if self._last is not None else iter(())

            def close(self):
                try:
                    if hasattr(self._last, 'close'):
                        self._last.close()
                except Exception:
                    pass

        return _CursorWrapper()

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        try:
            self._conn.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type:
            try:
                self._conn.rollback()
            except Exception:
                pass
        else:
            try:
                self._conn.commit()
            except Exception:
                pass
        # do not close connection here to allow reuse by app lifecycle
        return False


class FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def __iter__(self):
        return iter(self._rows)
    
    @property
    def rowcount(self):
        try:
            return len(self._rows)
        except Exception:
            return -1

    @property
    def description(self):
        return None


logger = logging.getLogger(__name__)


def ensure_serial_pk(conn, table: str, id_col: str = 'id'):
    """Ensure the integer primary key column has a Postgres sequence DEFAULT.
    Safe to call multiple times; will create sequence if missing and set it to max(id).
    """
    # Для SQLite ничего не делаем
    if hasattr(conn, 'execute'):
        return
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT column_default FROM information_schema.columns WHERE table_name = %s AND column_name = %s",
            (table, id_col),
        )
        row = cur.fetchone()
        if not row:
            return
        col_default = row[0]
        if col_default:
            return
        seq_name = f"{table}_{id_col}_seq"
        cur.execute(f"CREATE SEQUENCE IF NOT EXISTS {seq_name}")
        cur.execute(f"ALTER SEQUENCE {seq_name} OWNED BY {table}.{id_col}")
        cur.execute(f"ALTER TABLE {table} ALTER COLUMN {id_col} SET DEFAULT nextval('{seq_name}')")
        cur.execute(f"SELECT COALESCE(MAX({id_col}), 0) FROM {table}")
        max_id = cur.fetchone()[0] or 0
        if max_id <= 0:
            cur.execute("SELECT setval(%s, %s, false)", (seq_name, 1))
        else:
            cur.execute("SELECT setval(%s, %s, true)", (seq_name, max_id))
        try:
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
    except Exception:
        logger.exception('ensure_serial_pk failed for %s.%s', table, id_col)
        try:
            conn.rollback()
        except Exception:
            pass


def ensure_all_serial_pks(conn):
    """Ensure all integer primary-key columns have a Postgres sequence DEFAULT.
    Finds PK columns of integer types without a nextval() default and installs
    a sequence + DEFAULT for them. Safe to call multiple times.
    """
    # Для SQLite ничего не делаем, для Postgres оставляем как есть
    try:
        if hasattr(conn, 'execute'):
            # SQLite: пропускаем
            return
        cur = conn.cursor()
        cur.execute(
            """
            SELECT c.table_name, c.column_name
            FROM information_schema.columns c
            JOIN information_schema.table_constraints tc
              ON c.table_schema = tc.table_schema AND c.table_name = tc.table_name
            JOIN information_schema.key_column_usage k
              ON k.table_schema = c.table_schema AND k.table_name = c.table_name AND k.column_name = c.column_name AND k.constraint_name = tc.constraint_name
                        WHERE tc.constraint_type = 'PRIMARY KEY'
                            AND c.table_schema = 'public'
                            AND c.data_type IN ('integer','bigint','smallint')
                            AND (c.column_default IS NULL OR c.column_default NOT LIKE 'nextval(%')
            """
        )
        rows = cur.fetchall()
        for table, col in rows:
            try:
                ensure_serial_pk(conn, table, col)
            except Exception:
                logger.exception('failed to ensure serial for %s.%s', table, col)
        try:
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
    except Exception:
        logger.exception('ensure_all_serial_pks failed')
        try:
            conn.rollback()
        except Exception:
            pass

BAMBOO_ROD = "Бамбуковая удочка"
TEMP_ROD_RANGES = {
    "Углепластиковая удочка": (30, 70),
    "Карбоновая удочка": (50, 100),
    "Золотая удочка": (90, 150),
}

LEVEL_XP_REQUIREMENTS = [
    100, 250, 700, 1450, 2500, 3850, 5500, 7450, 9700, 12250,
    15100, 18250, 21700, 25450, 29500, 33850, 38500, 43450, 48700, 54250,
    60100, 66250, 72700, 79450, 86500, 93850, 101500, 109450, 117700, 126250,
    135100, 144250, 153700, 163450, 173500, 183850, 194500, 205450, 216700, 228250,
    240100, 252250, 264700, 277450, 290500, 303850, 317500, 331450, 345700, 360250,
    375100, 390250, 405700, 421450, 437500, 453850, 470500, 487450, 504700, 522250,
    540100, 558250, 576700, 595450, 614500, 633850, 653500, 673450, 693700, 714250,
    735100, 756250, 777700, 799450, 821500, 843850, 866500, 889450, 912700, 936250,
    960100, 984250, 1008700, 1033450, 1058500, 1083850, 1109500, 1135450, 1161700, 1188250,
    1215100, 1242250, 1269700, 1297450, 1325500, 1353850, 1382500, 1411450, 1440700, 1470250,
]

LEVEL_XP_THRESHOLDS = [0]
for requirement in LEVEL_XP_REQUIREMENTS:
    LEVEL_XP_THRESHOLDS.append(LEVEL_XP_THRESHOLDS[-1] + requirement)

MAX_LEVEL = len(LEVEL_XP_REQUIREMENTS)

BASE_XP_BY_RARITY = {
    "Обычная": 5,
    "Редкая": 20,
    "Легендарная": 100,
}

RARITY_XP_MULTIPLIERS = {
    "Обычная": 1.0,
    "Редкая": 1.1,
    "Легендарная": 1.2,
}

class Database:
    def __init__(self):
        self.init_db()

    def _connect(self):
        db_url = os.getenv('DATABASE_URL')
        if db_url:
            return PostgresConnWrapper(db_url)
        # Fallback to SQLite for testing/dev
        return sqlite3.connect(str(DB_PATH))

    def _get_temp_rod_uses(self, rod_name: str) -> Optional[int]:
        rod_range = TEMP_ROD_RANGES.get(rod_name)
        if not rod_range:
            return None
        return random.randint(rod_range[0], rod_range[1])
    
    def init_db(self):
        """Инициализация базы данных"""
        with self._connect() as conn:
            cursor = conn.cursor()
            
            # Таблица игроков
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS players (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT NOT NULL,
                    coins INTEGER DEFAULT 100,
                    stars INTEGER DEFAULT 0,
                    xp INTEGER DEFAULT 0,
                    level INTEGER DEFAULT 0,
                    current_rod TEXT DEFAULT 'Бамбуковая удочка',
                    current_bait TEXT DEFAULT 'Черви',
                    current_location TEXT DEFAULT 'Городской пруд',
                    last_fish_time TEXT,
                    is_banned INTEGER DEFAULT 0,
                    ban_until TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Таблица удочек
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS rods (
                    id INTEGER PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL,
                    price INTEGER NOT NULL,
                    durability INTEGER NOT NULL,
                    max_durability INTEGER NOT NULL,
                    fish_bonus INTEGER DEFAULT 0,
                    max_weight INTEGER DEFAULT 999
                )
            ''')
            
            # Таблица состояния удочек игроков
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS player_rods (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    rod_name TEXT NOT NULL,
                    current_durability INTEGER NOT NULL,
                    max_durability INTEGER NOT NULL,
                    last_repair_time TEXT,
                    recovery_start_time TEXT,
                    FOREIGN KEY (user_id) REFERENCES players (user_id),
                    FOREIGN KEY (rod_name) REFERENCES rods (name),
                    UNIQUE(user_id, rod_name)
                )
            ''')
            
            # Таблица рыбы
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS fish (
                    id INTEGER PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL,
                    rarity TEXT NOT NULL,
                    min_weight REAL NOT NULL,
                    max_weight REAL NOT NULL,
                    min_length REAL NOT NULL,
                    max_length REAL NOT NULL,
                    price INTEGER NOT NULL,
                    locations TEXT NOT NULL,
                    seasons TEXT NOT NULL,
                    suitable_baits TEXT DEFAULT 'Все',
                    max_rod_weight INTEGER DEFAULT 999,
                    required_level INTEGER DEFAULT 0,
                    sticker_id TEXT
                )
            ''')
            
            # Таблица локаций
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS locations (
                    id INTEGER PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL,
                    fish_population INTEGER NOT NULL,
                    current_players INTEGER DEFAULT 0,
                    max_players INTEGER NOT NULL
                )
            ''')
            
            # Таблица наживок
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS baits (
                    id INTEGER PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL,
                    price INTEGER NOT NULL,
                    fish_bonus INTEGER DEFAULT 0,
                    suitable_for TEXT DEFAULT 'Все'
                )
            ''')
            
            # Таблица мусора
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS trash (
                    id INTEGER PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL,
                    weight REAL NOT NULL,
                    price INTEGER NOT NULL,
                    locations TEXT NOT NULL,
                    sticker_id TEXT
                )
            ''')
            
            # Таблица инвентаря наживок
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS player_baits (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    bait_name TEXT NOT NULL,
                    quantity INTEGER DEFAULT 0,
                    FOREIGN KEY (user_id) REFERENCES players (user_id),
                    FOREIGN KEY (bait_name) REFERENCES baits (name),
                    UNIQUE(user_id, bait_name)
                )
            ''')
            
            # Таблица пойманной рыбы
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS caught_fish (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    fish_name TEXT NOT NULL,
                    weight REAL NOT NULL,
                    length REAL DEFAULT 0,
                    location TEXT NOT NULL,
                    sold INTEGER DEFAULT 0,
                    caught_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    sold_at TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES players (user_id)
                )
            ''')

            # Таблица транзакций Telegram Stars
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS star_transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    telegram_payment_charge_id TEXT NOT NULL UNIQUE,
                    total_amount INTEGER NOT NULL,
                    chat_id INTEGER,
                    chat_title TEXT,
                    refund_status TEXT DEFAULT 'none',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES players (user_id)
                )
            ''')

            # Таблица погоды по локациям
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS weather (
                    id INTEGER PRIMARY KEY,
                    location TEXT UNIQUE NOT NULL,
                    condition TEXT DEFAULT 'Ясно',
                    temperature INTEGER DEFAULT 20,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (location) REFERENCES locations (name)
                )
            ''')
            
            # Таблица сетей
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS nets (
                    id INTEGER PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL,
                    price INTEGER NOT NULL,
                    fish_count INTEGER NOT NULL,
                    cooldown_hours INTEGER NOT NULL,
                    max_uses INTEGER DEFAULT -1,
                    description TEXT
                )
            ''')
            
            # Таблица сетей игроков
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS player_nets (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    net_name TEXT NOT NULL,
                    uses_left INTEGER DEFAULT -1,
                    last_use_time TEXT,
                    FOREIGN KEY (user_id) REFERENCES players (user_id),
                    FOREIGN KEY (net_name) REFERENCES nets (name),
                    UNIQUE(user_id, net_name)
                )
            ''')
            
            # Таблица настроек чатов для реферальной системы
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS chat_configs (
                    chat_id INTEGER PRIMARY KEY,
                    admin_user_id INTEGER NOT NULL,
                    is_configured INTEGER DEFAULT 1,
                    admin_ref_link TEXT,
                    chat_invite_link TEXT,
                    chat_title TEXT,
                    stars_total INTEGER DEFAULT 0,
                    configured_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Таблица реф-ссылок пользователей (из Telegram Affiliate)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_ref_links (
                    user_id INTEGER PRIMARY KEY,
                    ref_link TEXT NOT NULL,
                    chat_invite_link TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Таблица реферальной статистики звёзд (агрегаты по user_id + chat_id)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ref_stars_stats (
                    user_id BIGINT NOT NULL,
                    chat_id BIGINT NOT NULL,
                    stars_received INTEGER DEFAULT 0,
                    stars_spent INTEGER DEFAULT 0,
                    stars_refunded INTEGER DEFAULT 0,
                    stars_withdrawn INTEGER DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, chat_id)
                )
            ''')

            # Таблица системных флагов/миграций
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS system_flags (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            ''')

            # Таблица турниров (конкурсов)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS tournaments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id BIGINT NOT NULL,
                    created_by BIGINT NOT NULL,
                    title TEXT NOT NULL,
                    tournament_type TEXT NOT NULL,
                    target_fish TEXT,
                    starts_at TIMESTAMP NOT NULL,
                    ends_at TIMESTAMP NOT NULL,
                    status TEXT DEFAULT 'scheduled',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.commit()
        
        # Ensure integer PK columns have sequences/defaults (Postgres)
        try:
            ensure_all_serial_pks(conn)
        except Exception:
            logger.exception('ensure_all_serial_pks call failed during init_db')
            try:
                conn.rollback()
            except Exception:
                pass

        # Миграции - добавляем колонки если их нет
        self._run_migrations()

        # Заполняем начальными данными (можно отключить для smoke-check БД)
        if os.getenv('FISHBOT_SKIP_DEFAULT_FILL') == '1':
            logger.info('Skipping _fill_default_data due to FISHBOT_SKIP_DEFAULT_FILL=1')
        else:
            self._fill_default_data()
    
    def _run_migrations(self):
        """Выполнение миграций для обновления схемы БД"""
        with self._connect() as conn:
            cursor = conn.cursor()

            def get_columns(table_name: str):
                # SQLite: используем PRAGMA table_info
                if type(cursor).__module__.startswith('sqlite3'):
                    cursor.execute(f"PRAGMA table_info({table_name})")
                    return [r[1] for r in cursor.fetchall()]
                else:
                    # Postgres: используем information_schema
                    cursor.execute(
                        "SELECT column_name FROM information_schema.columns WHERE table_name = %s AND table_schema = 'public'",
                        (table_name,)
                    )
                    return [r[0] for r in cursor.fetchall()]

            # Проверяем наличие колонок в таблице players (Postgres-friendly)
            columns = get_columns('players')

            # Добавление колонок для SQLite и Postgres
            def add_column_if_missing(table, col, coltype):
                cols = get_columns(table)
                if col not in cols:
                    try:
                        cursor.execute(f'ALTER TABLE {table} ADD COLUMN {col} {coltype}')
                        conn.commit()
                    except Exception:
                        pass

            add_column_if_missing('players', 'ref', 'INTEGER')
            add_column_if_missing('players', 'ref_link', 'TEXT')
            add_column_if_missing('players', 'chat_id', 'BIGINT')
            add_column_if_missing('players', 'xp', 'INTEGER DEFAULT 0')
            add_column_if_missing('players', 'level', 'INTEGER DEFAULT 0')
            add_column_if_missing('players', 'last_net_use_time', 'TEXT')
            add_column_if_missing('chat_configs', 'stars_total', 'INTEGER DEFAULT 0')
            add_column_if_missing('chat_configs', 'chat_title', 'TEXT')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS tournaments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id BIGINT NOT NULL,
                    created_by BIGINT NOT NULL,
                    title TEXT NOT NULL,
                    tournament_type TEXT NOT NULL,
                    target_fish TEXT,
                    starts_at TIMESTAMP NOT NULL,
                    ends_at TIMESTAMP NOT NULL,
                    status TEXT DEFAULT 'scheduled',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            add_column_if_missing('tournaments', 'target_fish', 'TEXT')
            add_column_if_missing('tournaments', 'status', "TEXT DEFAULT 'scheduled'")
            add_column_if_missing('tournaments', 'created_at', 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP')

            # Таблица агрегированной реф-статистики звёзд
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ref_stars_stats (
                    user_id BIGINT NOT NULL,
                    chat_id BIGINT NOT NULL,
                    stars_received INTEGER DEFAULT 0,
                    stars_spent INTEGER DEFAULT 0,
                    stars_refunded INTEGER DEFAULT 0,
                    stars_withdrawn INTEGER DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, chat_id)
                )
            ''')
            add_column_if_missing('ref_stars_stats', 'stars_received', 'INTEGER DEFAULT 0')
            add_column_if_missing('ref_stars_stats', 'stars_spent', 'INTEGER DEFAULT 0')
            add_column_if_missing('ref_stars_stats', 'stars_refunded', 'INTEGER DEFAULT 0')
            add_column_if_missing('ref_stars_stats', 'stars_withdrawn', 'INTEGER DEFAULT 0')

            # CRITICAL: Migrate players table to use composite primary key (user_id, chat_id)
            pk_cols = []
            if type(cursor).__module__.startswith('sqlite3'):
                cursor.execute("PRAGMA table_info(players)")
                pk_cols = [r[1] for r in cursor.fetchall() if r[5] == 1]
            else:
                cursor.execute(
                    """
                    SELECT kcu.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                      ON tc.constraint_name = kcu.constraint_name
                      AND tc.table_schema = kcu.table_schema
                    WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_name = %s
                    """,
                    ('players',)
                )
                pk_cols = [r[0] for r in cursor.fetchall()]

            if pk_cols == ['user_id']:
                # Need to recreate table with composite key
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS players_new (
                        user_id BIGINT NOT NULL,
                        chat_id BIGINT NOT NULL,
                        username TEXT NOT NULL,
                        coins INTEGER DEFAULT 100,
                        stars INTEGER DEFAULT 0,
                        xp INTEGER DEFAULT 0,
                        level INTEGER DEFAULT 0,
                        current_rod TEXT DEFAULT 'Бамбуковая удочка',
                        current_bait TEXT DEFAULT 'Черви',
                        current_location TEXT DEFAULT 'Городской пруд',
                        last_fish_time TEXT,
                        is_banned INTEGER DEFAULT 0,
                        ban_until TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        ref INTEGER,
                        ref_link TEXT,
                        last_net_use_time TEXT,
                        PRIMARY KEY (user_id, chat_id)
                    )
                ''')

                # Copy data from old table, normalizing NULL chat_id to -1
                if type(cursor).__module__.startswith('sqlite3'):
                    cursor.execute('''
                        INSERT OR IGNORE INTO players_new (user_id, chat_id, username, coins, stars, xp, level, current_rod, current_bait, current_location, last_fish_time, is_banned, ban_until, created_at, ref, ref_link, last_net_use_time)
                        SELECT user_id, COALESCE(chat_id, -1), username, coins, stars, COALESCE(xp, 0), COALESCE(level, 0), current_rod, current_bait, current_location, last_fish_time, is_banned, ban_until, created_at, ref, ref_link, last_net_use_time
                        FROM players
                    ''')
                else:
                    cursor.execute('''
                        INSERT INTO players_new (user_id, chat_id, username, coins, stars, xp, level, current_rod, current_bait, current_location, last_fish_time, is_banned, ban_until, created_at, ref, ref_link, last_net_use_time)
                        SELECT user_id, COALESCE(chat_id, -1), username, coins, stars, COALESCE(xp, 0), COALESCE(level, 0), current_rod, current_bait, current_location, last_fish_time, is_banned, ban_until, created_at, ref, ref_link, last_net_use_time
                        FROM players
                        ON CONFLICT (user_id, chat_id) DO NOTHING
                    ''')

                # Attempt to replace the old players table only if nothing
                # references it via foreign key constraints. If other objects
                # depend on `players` skip the destructive replacement to
                # avoid dropping dependent objects and noisy stack traces.
                try:
                    # Check for any foreign-key constraints referencing players
                    cursor.execute(
                        "SELECT COUNT(*) FROM pg_constraint WHERE confrelid = (SELECT oid FROM pg_class WHERE relname = %s AND relnamespace = 'public'::regnamespace)",
                        ('players',)
                    )
                    # fetchone() returns a tuple like (count,); use the first value
                    ref_count_row = cursor.fetchone()
                    ref_count = ref_count_row[0] if ref_count_row else 0
                except Exception:
                    # Fallback: if we cannot reliably detect references, avoid DROP
                    ref_count = 1

                if ref_count:
                    logger.warning("Could not replace players table (dependent objects exist). Skipping composite-PK migration.")
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    try:
                        cursor.execute('DROP TABLE IF EXISTS players_new')
                        conn.commit()
                    except Exception:
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                else:
                    try:
                        cursor.execute('DROP TABLE players')
                        cursor.execute('ALTER TABLE players_new RENAME TO players')
                        conn.commit()
                    except Exception as e:
                        logger.warning("Could not replace players table (%s). Skipping composite-PK migration.", e)
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                        try:
                            cursor.execute('DROP TABLE IF EXISTS players_new')
                            conn.commit()
                        except Exception:
                            try:
                                conn.rollback()
                            except Exception:
                                pass

            # refresh columns list after potential schema change
            columns = get_columns('players')

            if 'last_net_use_time' not in columns:
                cursor.execute('ALTER TABLE players ADD COLUMN last_net_use_time TEXT')
                conn.commit()

            # Helper to add a column if missing
            def ensure_column(table: str, col: str, col_def: str):
                cols = get_columns(table)
                if col not in cols:
                    cursor.execute(f'ALTER TABLE {table} ADD COLUMN {col} {col_def}')
                    conn.commit()

            ensure_column('trash', 'sticker_id', 'TEXT')
            ensure_column('caught_fish', 'length', 'REAL DEFAULT 0')
            ensure_column('caught_fish', 'sold', 'INTEGER DEFAULT 0')
            ensure_column('caught_fish', 'sold_at', 'TIMESTAMP')
            ensure_column('fish', 'required_level', 'INTEGER DEFAULT 0')
            ensure_column('star_transactions', 'chat_id', 'INTEGER')
            ensure_column('star_transactions', 'chat_title', 'TEXT')
            ensure_column('player_rods', 'chat_id', 'INTEGER')
            ensure_column('player_nets', 'chat_id', 'INTEGER')
            ensure_column('chat_configs', 'admin_ref_link', 'TEXT')
            ensure_column('chat_configs', 'chat_invite_link', 'TEXT')
            ensure_column('user_ref_links', 'chat_invite_link', 'TEXT')
            ensure_column('caught_fish', 'chat_id', 'INTEGER')

            # Ensure unique index for ON CONFLICT targets that expect (user_id, chat_id)
            try:
                cols = get_columns('players')
                if 'chat_id' in cols:
                    cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS players_user_chat_unique ON players (user_id, chat_id)")
                    conn.commit()
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass

            # Ensure integer PK columns have a sequence/default on Postgres (e.g., rods.id)
            # Also ensure user/chat identifier columns are 64-bit on Postgres to avoid integer out of range
            def ensure_bigint_column(table_name: str, column_name: str):
                try:
                    cursor.execute(
                        "SELECT data_type FROM information_schema.columns WHERE table_name = %s AND column_name = %s AND table_schema = 'public'",
                        (table_name, column_name)
                    )
                    row = cursor.fetchone()
                    if row and row[0] != 'bigint':
                        try:
                            # Use a safe USING expression that converts only numeric text to bigint,
                            # setting non-numeric values to NULL to avoid cast errors.
                            safe_using = (
                                "USING (CASE WHEN COALESCE(" + column_name + "::text, '') ~ '^[0-9]+$' "
                                "THEN (" + column_name + "::text)::bigint ELSE NULL END)"
                            )
                            cursor.execute(f'ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE BIGINT {safe_using}')
                            conn.commit()
                        except Exception:
                            try:
                                conn.rollback()
                            except Exception:
                                pass
                except Exception:
                    try:
                        conn.rollback()
                    except Exception:
                        pass

            # Convert known user/chat id columns to BIGINT to support large Telegram IDs
            bigint_targets = [
                ('players', 'user_id'),
                ('players', 'chat_id'),
                ('player_rods', 'user_id'),
                ('player_rods', 'chat_id'),
                ('player_baits', 'user_id'),
                ('caught_fish', 'user_id'),
                ('caught_fish', 'chat_id'),
                ('player_nets', 'user_id'),
                ('player_nets', 'chat_id'),
                ('star_transactions', 'user_id'),
                ('star_transactions', 'chat_id'),
                ('chat_configs', 'chat_id'),
                ('chat_configs', 'admin_user_id'),
                ('user_ref_links', 'user_id')
            ]
            for tbl, col in bigint_targets:
                ensure_bigint_column(tbl, col)

            # Use module-level helper `ensure_serial_pk(conn, table, id_col)`
            try:
                ensure_serial_pk(conn, 'rods', 'id')
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass


                # ===== Методы для /ref и вывода/вывода звёзд =====
                def get_ref_access_chats(self, user_id: int) -> List[Dict[str, Any]]:
                    """Получить чаты, где пользователь имеет доступ к реферальному выводу"""
                    with self._connect() as conn:
                        cursor = conn.cursor()
                        cursor.execute('''
                            SELECT chat_id, chat_title, stars_total
                            FROM chat_configs
                            WHERE admin_user_id = ? AND is_configured = 1
                        ''', (user_id,))
                        rows = cursor.fetchall()
                        cols = [d[0] for d in cursor.description] if cursor.description else []
                        return [dict(zip(cols, row)) for row in rows]

                def get_chat_title(self, chat_id: int) -> str:
                    """Получить название чата по chat_id"""
                    with self._connect() as conn:
                        cursor = conn.cursor()
                        cursor.execute('SELECT chat_title FROM chat_configs WHERE chat_id = ?', (chat_id,))
                        row = cursor.fetchone()
                        return row[0] if row and row[0] else str(chat_id)

                def get_chat_stars_total(self, chat_id: int) -> int:
                    """Получить общее количество звёзд в чате"""
                    with self._connect() as conn:
                        cursor = conn.cursor()
                        cursor.execute('SELECT COALESCE(stars_total, 0) FROM chat_configs WHERE chat_id = ?', (chat_id,))
                        row = cursor.fetchone()
                        return int(row[0]) if row else 0

                def get_chat_refunds_total(self, chat_id: int) -> int:
                    """Получить сумму всех выведенных/возвращённых звёзд по чату"""
                    with self._connect() as conn:
                        cursor = conn.cursor()
                        cursor.execute('''
                            SELECT COALESCE(SUM(total_amount), 0)
                            FROM star_transactions
                            WHERE chat_id = ? AND refund_status IN ("approved", "refunded")
                        ''', (chat_id,))
                        row = cursor.fetchone()
                        return int(row[0]) if row else 0

                def get_available_stars_for_withdraw(self, chat_id: int) -> int:
                    """Получить количество звёзд, доступных для вывода в чате"""
                    total = self.get_chat_stars_total(chat_id)
                    withdrawn = self.get_chat_refunds_total(chat_id)
                    return max(0, total - withdrawn)

                def get_withdrawn_stars(self, chat_id: int) -> int:
                    """Получить количество уже выведенных звёзд в чате"""
                    return self.get_chat_refunds_total(chat_id)

                def mark_stars_withdrawn(self, chat_id: int, amount: int, user_id: int, admin_id: int, status: str = "approved") -> bool:
                    """Отметить вывод звёзд (создать транзакцию)"""
                    with self._connect() as conn:
                        cursor = conn.cursor()
                        cursor.execute('''
                            INSERT INTO star_transactions (user_id, total_amount, chat_id, chat_title, refund_status)
                            VALUES (?, ?, ?, ?, ?)
                        ''', (user_id, amount, chat_id, self.get_chat_title(chat_id), status))
                        conn.commit()
                        return cursor.rowcount > 0
            # Populate chat_id in player_rods and player_nets and caught_fish
            if type(cursor).__module__.startswith('sqlite3'):
                # SQLite: просто берем максимальный chat_id для каждого user_id, если он числовой
                cursor.execute('''
                    UPDATE player_rods
                    SET chat_id = (
                        SELECT MAX(chat_id) FROM players p WHERE p.user_id = player_rods.user_id AND CAST(chat_id AS TEXT) GLOB '[0-9]*'
                    )
                    WHERE chat_id IS NULL OR chat_id < 1
                ''')
            else:
                cursor.execute('''
                    UPDATE player_rods
                    SET chat_id = (
                        SELECT MAX(
                            CASE WHEN COALESCE(p.chat_id::text, '') ~ '^[0-9]+$' THEN (p.chat_id::text)::bigint ELSE NULL END
                        ) FROM players p WHERE p.user_id = player_rods.user_id
                    )
                    WHERE chat_id IS NULL OR chat_id < 1
                ''')
            conn.commit()

            if type(cursor).__module__.startswith('sqlite3'):
                cursor.execute('''
                    UPDATE player_nets
                    SET chat_id = (
                        SELECT MAX(chat_id) FROM players p WHERE p.user_id = player_nets.user_id AND CAST(chat_id AS TEXT) GLOB '[0-9]*'
                    )
                    WHERE chat_id IS NULL OR chat_id < 1
                ''')
                conn.commit()
                cursor.execute('''
                    UPDATE caught_fish
                    SET chat_id = (
                        SELECT MAX(chat_id) FROM players p WHERE p.user_id = caught_fish.user_id AND CAST(chat_id AS TEXT) GLOB '[0-9]*'
                    )
                    WHERE chat_id IS NULL OR chat_id < 1
                ''')
                conn.commit()
            else:
                cursor.execute('''
                    UPDATE player_nets
                    SET chat_id = (
                        SELECT MAX(
                            CASE WHEN COALESCE(p.chat_id::text, '') ~ '^[0-9]+$' THEN (p.chat_id::text)::bigint ELSE NULL END
                        ) FROM players p WHERE p.user_id = player_nets.user_id
                    )
                    WHERE chat_id IS NULL OR chat_id < 1
                ''')
                conn.commit()
                cursor.execute('''
                    UPDATE caught_fish
                    SET chat_id = (
                        SELECT MAX(
                            CASE WHEN COALESCE(p.chat_id::text, '') ~ '^[0-9]+$' THEN (p.chat_id::text)::bigint ELSE NULL END
                        ) FROM players p WHERE p.user_id = caught_fish.user_id
                    )
                    WHERE chat_id IS NULL OR chat_id < 1
                ''')
                conn.commit()

            # Инициализация погоды для локаций
            cursor.execute('SELECT name FROM locations')
            locations = cursor.fetchall()

            from weather import weather_system
            for location in locations:
                loc_name = location[0]
                if type(cursor).__module__.startswith('sqlite3'):
                    cursor.execute('SELECT 1 FROM weather WHERE location = ?', (loc_name,))
                else:
                    cursor.execute('SELECT 1 FROM weather WHERE location = %s', (loc_name,))
                if not cursor.fetchone():
                    condition, temp = weather_system.generate_weather(loc_name)
                    if type(cursor).__module__.startswith('sqlite3'):
                        cursor.execute(
                            'INSERT INTO weather (location, condition, temperature) VALUES (?, ?, ?)',
                            (loc_name, condition, temp),
                        )
                    else:
                        cursor.execute(
                            'INSERT INTO weather (location, condition, temperature) VALUES (%s, %s, %s)',
                            (loc_name, condition, temp),
                        )
                # Ensure a global players row exists (user_id = -1, chat_id = -1)
                try:
                    cursor.execute(
                        "INSERT INTO players (user_id, chat_id, username, coins, stars, xp, level) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (user_id, chat_id) DO NOTHING",
                        (-1, -1, 'GLOBAL', 0, 0, 0, 0),
                    )
                except Exception:
                    try:
                        conn.rollback()
                    except Exception:
                        pass

                # Ensure a global base net exists (user_id = -1, chat_id = -1)
                try:
                    cursor.execute(
                        "INSERT INTO player_nets (user_id, net_name, uses_left, chat_id) VALUES (%s, %s, %s, %s) ON CONFLICT (user_id, net_name) DO NOTHING",
                        (-1, 'Базовая сеть', -1, -1),
                    )
                except Exception:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
            
            conn.commit()
    
    def _fill_default_data(self):
        """Заполнение базы данных начальными данными"""
        with self._connect() as conn:
            cursor = conn.cursor()
            
            # Добавление удочек с информацией о максимальном весе
            # Формат: (name, price, durability, max_durability, fish_bonus, max_weight)
            rods_data = [
                ("Бамбуковая удочка", 0, 100, 100, 0, 20),  # стартовая удочка, макс вес 20 кг
                ("Углепластиковая удочка", 1500, 150, 150, 5, 35),  # макс вес 35 кг
                ("Карбоновая удочка", 4500, 200, 200, 10, 120),  # макс вес 120 кг
                ("Золотая удочка", 15000, 300, 300, 20, 350),  # макс вес 350 кг
                ("Гарпун", 75000, 100, 100, 0, 10000),  # гарпун: макс вес 10 тонн, мин. 150 кг (логика в game)
            ]
            
            cursor.executemany('''
                INSERT OR IGNORE INTO rods (name, price, durability, max_durability, fish_bonus, max_weight)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', rods_data)
            
            # Добавление локаций
            locations_data = [
                ("Городской пруд", 100, 0, 5),
                ("Река", 150, 0, 10),
                ("Озеро", 200, 0, 15),
                ("Море", 300, 0, 20),
            ]

            # Ensure `locations.id` has a sequence/default on Postgres so inserts without id work
            try:
                cursor.execute(
                    "SELECT column_default FROM information_schema.columns WHERE table_name = %s AND column_name = %s",
                    ('locations', 'id'),
                )
                row = cursor.fetchone()
                if row and not row[0]:
                    seq_name = 'locations_id_seq'
                    cursor.execute(f"CREATE SEQUENCE IF NOT EXISTS {seq_name}")
                    cursor.execute(f"ALTER TABLE locations ALTER COLUMN id SET DEFAULT nextval('{seq_name}')")
                    cursor.execute("SELECT COALESCE(MAX(id), 1) FROM locations")
                    max_id = cursor.fetchone()[0] or 1
                    cursor.execute("SELECT setval(%s, %s)", (seq_name, max_id))
                    conn.commit()
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass

            cursor.executemany('''
                INSERT OR IGNORE INTO locations (name, fish_population, current_players, max_players)
                VALUES (?, ?, ?, ?)
            ''', locations_data)
            
            # Добавление наживок
            bait_price_factor = 0.85
            base_baits_data = [
                ("Черви", 20, 0, "Все"),
                ("Опарыш", 30, 2, "Все"),
                ("Мотыль", 30, 2, "Все"),
                ("Хлеб", 15, 0, "Все"),
                ("Мякиш хлеба", 20, 0, "Все"),
                ("Тесто", 20, 0, "Все"),
                ("Манка", 25, 0, "Все"),
                ("Каша", 25, 0, "Все"),
                ("Кукуруза", 30, 1, "Все"),
                ("Горох", 30, 1, "Все"),
                ("Бойлы", 80, 5, "Все"),
                ("Картофель", 25, 0, "Все"),
                ("Технопланктон", 120, 6, "Все"),
                ("Зелень", 20, 0, "Все"),
                ("Камыш", 20, 0, "Все"),
                ("Огурец", 25, 0, "Все"),
                ("Паста", 35, 1, "Все"),
                ("Творожное тесто", 35, 1, "Все"),
                ("Креветка", 60, 3, "Все"),
                ("Морской червь", 70, 3, "Все"),
                ("Кусочки рыбы", 60, 3, "Все"),
                ("Сало", 30, 0, "Все"),
                ("Живец", 80, 6, "Все"),
                ("Крупный живец", 120, 8, "Все"),
                ("Кальмар", 90, 5, "Все"),
                ("Сардина", 70, 4, "Все"),
                ("Сельдь", 70, 4, "Все"),
                ("Моллюск", 80, 4, "Все"),
                ("Пилькер", 110, 7, "Все"),
                ("Блесна", 60, 5, "Все"),
                ("Узкая блесна", 70, 6, "Все"),
                ("Маленькая блесна", 50, 4, "Все"),
                ("Воблер", 80, 6, "Все"),
                ("Мушка", 40, 2, "Все"),
                ("Муха", 25, 1, "Все"),
                ("Кузнечик", 30, 1, "Все"),
                ("Майский жук", 40, 2, "Все"),
                ("Лягушонок", 90, 6, "Все"),
                ("Выползок", 60, 4, "Все"),
                ("Пучок червей", 70, 4, "Все"),
                ("Личинка", 30, 1, "Все"),
                ("Личинка короеда", 60, 4, "Все"),
                ("Мышь", 120, 8, "Все"),
                ("Икра", 90, 6, "Все"),
                ("Мормыш", 70, 5, "Все"),
                ("Спрут", 140, 9, "Все"),
                ("Туша рыбы", 160, 10, "Все"),
                ("Крупный кусок мяса", 140, 9, "Все"),
                ("Печень", 90, 6, "Все"),
                ("Кусок мяса", 110, 7, "Все"),
                # --- Кормушки ---
                ("Кормушка простая", 500, 10, "feeder"),
                ("Кормушка премиум", 2500, 25, "feeder"),
                ("Кормушка звёздная", 0, 50, "feeder_star"),  # Покупка за 50 звёзд, цена в монетах = 0
            ]

            baits_data = [
                (name, max(1, int(round(price * bait_price_factor))), bonus, suitable)
                for name, price, bonus, suitable in base_baits_data
            ]
            # Ensure `baits.id` has a sequence/default on Postgres so inserts without id work
            try:
                ensure_serial_pk(conn, 'baits', 'id')
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass

            cursor.executemany('''
                INSERT OR REPLACE INTO baits (name, price, fish_bonus, suitable_for)
                VALUES (?, ?, ?, ?)
            ''', baits_data)
            
            # Добавление рыб с расширенной информацией
            # Формат: (имя, редкость, min_вес_кг, max_вес_кг, min_длина_см, max_длина_см, цена, локации, сезоны, наживка, макс_вес_удочки, стикер)
            fish_data = [
                # ===== ПРУД =====
                ("Карась", "Обычная", 0.2, 1.2, 15, 35, 15, "Городской пруд", "Все", "Хлеб,Манка,Черви,Опарыш,Тесто,Кукуруза,Мотыль,Горох,Каша", 6, None),
                ("Ротан", "Обычная", 0.1, 0.6, 12, 30, 12, "Городской пруд", "Все", "Сало,Черви,Кусочки рыбы,Опарыш,Мотыль,Личинка", 5, None),
                ("Верховка", "Обычная", 0.02, 0.1, 6, 12, 5, "Городской пруд", "Лето", "Манка,Тесто,Хлеб,Опарыш,Мотыль", 3, None),
                ("Вьюн", "Обычная", 0.05, 0.3, 15, 25, 8, "Городской пруд", "Лето", "Черви,Мотыль,Личинка,Опарыш", 4, None),
                ("Горчак", "Обычная", 0.05, 0.25, 10, 18, 7, "Городской пруд", "Лето", "Хлеб,Тесто,Манка,Опарыш", 4, None),
                ("Золотой карась", "Редкая", 0.3, 1.5, 20, 40, 40, "Городской пруд", "Весна,Лето", "Черви,Тесто,Хлеб,Опарыш,Кукуруза,Горох,Манка,Каша", 8, None),
                ("Карп", "Редкая", 2.0, 12.0, 40, 90, 80, "Городской пруд", "Лето,Осень", "Кукуруза,Бойлы,Картофель,Горох,Черви,Каша,Тесто,Хлеб,Пучок червей", 25, None),
                ("Толстолобик", "Редкая", 3.0, 15.0, 50, 100, 90, "Городской пруд", "Лето", "Технопланктон,Камыш,Зелень,Хлеб,Тесто,Каша", 30, None),
                ("Прудовая форель", "Редкая", 1.0, 4.0, 30, 60, 70, "Городской пруд", "Осень,Зима", "Паста,Кукуруза,Живец,Блесна,Икра,Черви,Опарыш,Мушка", 12, None),
                ("Буффало", "Редкая", 2.0, 8.0, 40, 80, 85, "Городской пруд", "Лето", "Тесто,Кукуруза,Каша,Бойлы,Черви,Горох,Картофель", 20, None),
                ("Черный амур", "Легендарная", 7.0, 25.0, 60, 120, 250, "Городской пруд", "Лето", "Моллюск,Кукуруза,Горох,Камыш,Зелень,Тесто,Каша", 35, None),
                ("Карп Кои", "Легендарная", 3.0, 12.0, 40, 80, 220, "Городской пруд", "Лето", "Бойлы,Кукуруза,Тесто,Хлеб,Горох,Картофель,Каша,Пучок червей", 25, None),
                ("Змееголов", "Легендарная", 4.0, 15.0, 50, 100, 260, "Городской пруд", "Лето", "Лягушонок,Живец,Кусок мяса,Блесна,Воблер,Крупный живец,Кусочки рыбы", 30, None),
                
                # ===== РЕКА =====
                ("Плотва", "Обычная", 0.05, 0.4, 12, 28, 10, "Река", "Все", "Опарыш,Тесто,Мотыль,Черви,Хлеб,Манка,Кукуруза,Горох", 5, None),
                ("Окунь", "Обычная", 0.1, 0.8, 15, 30, 20, "Река,Озеро", "Все", "Мотыль,Черви,Живец,Блесна,Опарыш,Маленькая блесна,Воблер,Кусочки рыбы", 6, None),
                ("Голавль", "Обычная", 0.2, 1.0, 20, 40, 18, "Река", "Весна,Лето", "Кузнечик,Майский жук,Хлеб,Блесна,Черви,Воблер,Опарыш,Муха", 7, None),
                ("Уклейка", "Обычная", 0.02, 0.15, 8, 18, 8, "Река", "Лето", "Муха,Опарыш,Тесто,Хлеб,Мотыль,Манка", 3, None),
                ("Лещ", "Обычная", 0.5, 2.5, 25, 50, 25, "Река,Озеро", "Лето,Осень", "Горох,Пучок червей,Кукуруза,Опарыш,Каша,Черви,Мотыль,Тесто", 10, None),
                ("Ёрш", "Обычная", 0.05, 0.2, 10, 18, 8, "Река", "Зима,Весна", "Черви,Мотыль,Опарыш,Личинка", 4, None),
                ("Жерех", "Редкая", 1.0, 4.0, 35, 60, 50, "Река", "Весна,Лето", "Блесна,Живец,Кузнечик,Воблер,Узкая блесна,Кусочки рыбы", 14, None),
                ("Судак", "Редкая", 1.0, 5.0, 35, 70, 55, "Река", "Осень,Зима", "Живец,Узкая блесна,Воблер,Пучок червей,Блесна,Кусочки рыбы", 12, None),
                ("Язь", "Редкая", 0.6, 2.5, 25, 45, 40, "Река", "Весна,Осень", "Кукуруза,Горох,Черви,Хлеб,Кузнечик,Опарыш,Тесто,Каша", 10, None),
                ("Налим", "Редкая", 0.5, 3.0, 30, 60, 45, "Река", "Зима", "Лягушонок,Пучок червей,Кусочки рыбы,Печень,Черви,Живец", 12, None),
                ("Хариус", "Редкая", 0.4, 1.5, 25, 40, 40, "Река", "Лето", "Мушка,Опарыш,Черви,Маленькая блесна,Муха,Мотыль", 10, None),
                ("Сом", "Легендарная", 5.0, 40.0, 80, 200, 200, "Река", "Лето", "Печень,Крупный живец,Выползок,Лягушонок,Кусочки рыбы,Живец,Кусок мяса", 40, None),
                ("Стерлядь", "Легендарная", 1.0, 6.0, 40, 80, 220, "Река", "Весна,Лето", "Личинка короеда,Черви,Опарыш,Мотыль,Икра", 20, None),
                ("Таймень", "Легендарная", 5.0, 20.0, 60, 120, 250, "Река", "Осень", "Блесна,Воблер,Мышь,Крупный живец,Живец,Кусочки рыбы", 30, None),
                ("Белуга", "Легендарная", 30.0, 120.0, 120, 250, 400, "Река", "Весна", "Кусочки рыбы,Крупный живец,Моллюск,Живец,Сельдь,Выползок,Кусок мяса", 60, None),
                
                # ===== ОЗЕРО =====
                ("Красноперка", "Обычная", 0.1, 0.5, 15, 25, 10, "Озеро", "Лето", "Тесто,Хлеб,Муха,Опарыш,Черви,Манка,Кукуруза,Мотыль", 5, None),
                ("Густера", "Обычная", 0.15, 0.8, 18, 30, 12, "Озеро", "Лето", "Опарыш,Мотыль,Каша,Черви,Тесто,Горох", 6, None),
                ("Щука", "Обычная", 1.0, 6.0, 40, 80, 30, "Река,Озеро", "Весна,Осень", "Живец,Блесна,Воблер,Лягушонок,Кусочки рыбы,Узкая блесна", 18, None),
                ("Синец", "Обычная", 0.2, 0.8, 20, 35, 12, "Озеро", "Лето", "Черви,Опарыш,Мотыль,Тесто,Каша", 6, None),
                ("Подлещик", "Обычная", 0.2, 1.0, 20, 40, 15, "Озеро", "Весна,Лето", "Мотыль,Опарыш,Каша,Тесто,Черви,Горох,Кукуруза", 8, None),
                ("Пескарь", "Обычная", 0.05, 0.3, 12, 22, 7, "Озеро", "Все", "Черви,Мотыль,Хлеб,Опарыш,Манка", 5, None),
                ("Чехонь", "Редкая", 0.3, 1.2, 25, 40, 30, "Озеро", "Весна,Лето", "Опарыш,Муха,Тесто,Кузнечик,Мотыль,Черви", 8, None),
                ("Линь", "Редкая", 0.5, 2.0, 30, 50, 35, "Озеро", "Лето", "Черви,Творожное тесто,Опарыш,Мотыль,Кукуруза,Горох", 10, None),
                ("Сиг", "Редкая", 0.8, 3.0, 35, 60, 45, "Озеро", "Осень,Зима", "Икра,Мормыш,Мотыль,Маленькая блесна,Опарыш,Черви", 12, None),
                ("Белый амур", "Редкая", 2.0, 10.0, 50, 90, 60, "Озеро", "Лето", "Камыш,Кукуруза,Горох,Огурец,Зелень,Тесто", 20, None),
                ("Пелядь", "Редкая", 0.8, 3.0, 35, 60, 45, "Озеро", "Зима", "Мормыш,Мотыль,Икра,Опарыш", 12, None),
                ("Форель озерная", "Легендарная", 1.5, 6.0, 40, 70, 200, "Озеро", "Весна,Осень", "Воблер,Блесна,Живец,Икра,Кузнечик,Мушка,Опарыш,Червия,Маленькая блесна", 16, None),
                ("Угорь", "Легендарная", 1.0, 5.0, 50, 80, 180, "Озеро", "Лето", "Выползок,Живец,Кусочки рыбы,Пучок червей,Лягушонок,Кусок мяса", 18, None),
                ("Осетр", "Легендарная", 3.0, 25.0, 70, 140, 260, "Озеро", "Лето,Осень", "Сельдь,Кусочки рыбы,Моллюск,Выползок,Крупный живец,Живец,Икра", 35, None),
                
                # ===== МОРЕ =====
                ("Сельдь", "Обычная", 0.2, 0.8, 20, 35, 15, "Море", "Все", "Креветка,Опарыш,Морской червь,Кусочки рыбы,Блесна", 6, None),
                ("Ставрида", "Обычная", 0.3, 1.0, 25, 40, 18, "Море", "Лето,Осень", "Блесна,Креветка,Кусочки рыбы,Пилькер,Воблер", 8, None),
                ("Бычок", "Обычная", 0.05, 0.3, 10, 20, 10, "Море", "Весна,Лето", "Черви,Кусочки рыбы,Креветка,Сало,Морской червь", 6, None),
                ("Камбала", "Обычная", 0.5, 3.0, 30, 50, 20, "Море", "Осень,Зима", "Морской червь,Кусочки рыбы,Моллюск,Креветка,Сельдь", 10, None),
                ("Морской окунь", "Обычная", 0.4, 1.5, 25, 40, 22, "Море", "Весна,Лето", "Живец,Креветка,Блесна,Воблер,Кусочки рыбы", 10, None),
                ("Кефаль", "Обычная", 0.4, 1.2, 30, 45, 20, "Море,Река", "Лето", "Мякиш хлеба,Морской червь,Тесто,Хлеб,Креветка", 8, None),
                ("Барабулька", "Редкая", 0.2, 0.8, 20, 30, 35, "Море", "Лето", "Морской червь,Креветка,Опарыш,Кусочки рыбы", 8, None),
                ("Скумбрия", "Редкая", 0.6, 2.5, 30, 50, 50, "Море", "Лето,Осень", "Блесна,Пилькер,Живец,Кальмар,Кусочки рыбы,Креветка", 14, None),
                ("Тунец", "Редкая", 5.0, 30.0, 80, 150, 180, "Море", "Лето", "Воблер,Сардина,Живец,Кальмар,Кусочки рыбы,Блесна,Пилькер", 35, None),
                ("Дорадо", "Редкая", 1.0, 6.0, 40, 70, 90, "Море", "Лето", "Кальмар,Креветка,Кусочки рыбы,Живец,Сардина", 18, None),
                ("Мурена", "Редкая", 2.0, 10.0, 60, 120, 120, "Море", "Лето", "Крупный кусок мяса,Кусочки рыбы,Кальмар,Живец,Кусок мяса", 25, None),
                ("Сарган", "Редкая", 0.5, 2.0, 30, 60, 60, "Море", "Осень", "Опарыш,Кусочки рыбы,Креветка,Блесна,Морской червь", 12, None),
                ("Рыба-меч", "Легендарная", 20.0, 110.0, 120, 250, 500, "Море", "Лето,Осень", "Крупный живец,Кальмар,Туша рыбы,Воблер,Сардина,Живец", 60, None),
                ("Марлин", "Легендарная", 20.0, 120.0, 140, 300, 600, "Море", "Осень", "Воблер,Спрут,Крупный живец,Кальмар,Туша рыбы,Живец", 60, None),
                ("Белая акула", "Легендарная", 50.0, 300.0, 200, 500, 900, "Море", "Лето", "Туша рыбы,Крупный живец,Кусок мяса,Крупный кусок мяса,Кальмар,Спрут", 80, None),
            ]

            from fish_stickers import FISH_INFO

            bait_name_map = {name.lower(): name for name, _, _, _ in base_baits_data}

            def normalize_seasons(seasons_value: str) -> str:
                if not seasons_value:
                    return "Все"
                if "Круглый год" in seasons_value:
                    return "Все"
                parts = [s.strip() for s in seasons_value.split(',') if s.strip()]
                return ','.join(parts) if parts else "Все"

            def normalize_baits(nutrition_value: str) -> str:
                if not nutrition_value:
                    return "Все"
                raw_parts: List[str] = [p.strip() for p in nutrition_value.split(',') if p.strip()]
                normalized: List[str] = []
                for part in raw_parts:
                    lower = part.lower()
                    if lower in bait_name_map:
                        normalized.append(bait_name_map[lower])
                    else:
                        normalized.append(part[:1].upper() + part[1:])
                return ','.join(normalized) if normalized else "Все"

            normalized_fish_data = []
            for entry in fish_data:
                (name, rarity, min_weight, max_weight, min_length, max_length, price,
                 locations, seasons, suitable_baits, max_rod_weight, sticker_id) = entry
                required_level = 0
                info = FISH_INFO.get(name)
                if info:
                    seasons = normalize_seasons(info.get("seasons", ""))
                    suitable_baits = normalize_baits(info.get("nutrition", ""))
                normalized_fish_data.append((
                    name, rarity, min_weight, max_weight, min_length, max_length, price,
                    locations, seasons, suitable_baits, max_rod_weight, required_level, sticker_id
                ))
            fish_data = normalized_fish_data
            
            cursor.execute('DELETE FROM fish')
            cursor.executemany('''
                INSERT OR REPLACE INTO fish (name, rarity, min_weight, max_weight, min_length, max_length, price, locations, seasons, suitable_baits, max_rod_weight, required_level, sticker_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', fish_data)
            
            # Добавление мусора для реки
            trash_data = [
                ("Коряга", 0.5, 2, "Река", None),
                ("Старая шина", 2.0, 1, "Река", None),
                ("Консервная банка", 0.1, 1, "Река", None),
                ("Ботинок", 0.3, 2, "Река", None),
                ("Пластиковая бутылка", 0.05, 0, "Река", None),
                ("Ржавый крючок", 0.02, 5, "Река", None),
                ("Кусок трубы", 1.5, 3, "Река", None),
                ("Поломанная удочка", 1.0, 10, "Река", None),
                ("Рыболовная сетка", 0.8, 5, "Река", None),
                ("Деревянная доска", 2.5, 4, "Река", None),
                ("Старый якорь", 3.0, 15, "Река", None),
                ("Веревка", 0.3, 1, "Река", None),
            ]
            
            cursor.executemany('''
                INSERT OR IGNORE INTO trash (name, weight, price, locations, sticker_id)
                VALUES (?, ?, ?, ?, ?)
            ''', trash_data)
            
            # Добавление сетей
            # Формат: (name, price, fish_count, cooldown_hours, max_uses, description)
            nets_data = [
                ("Базовая сеть", 0, 5, 24, -1, "Бесплатная сеть, можно использовать раз в 24 часа. Вытаскивает 5 рыб."),
                ("Прочная сеть", 300, 8, 24, 7, "Сеть на 7 использований. Можно использовать раз в 24 часа. Вытаскивает 8 рыб."),
                ("Быстрая сеть", 500, 5, 12, 14, "Сеть на 14 использований. Можно использовать раз в 12 часов. Вытаскивает 5 рыб."),
            ]
            
            cursor.executemany('''
                INSERT OR IGNORE INTO nets (name, price, fish_count, cooldown_hours, max_uses, description)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', nets_data)

            # Исправление перепутанных полей в caught_fish (локация/длина)
            cursor.execute('''
                UPDATE caught_fish
                SET location = CAST(length AS TEXT), length = 0
                WHERE location NOT IN (SELECT name FROM locations)
                  AND CAST(length AS TEXT) IN (SELECT name FROM locations)
            ''')

            # Одноразовый сброс рефералов (2026-02-06)
            cursor.execute("SELECT value FROM system_flags WHERE key = 'ref_reset_20260206'")
            if not cursor.fetchone():
                cursor.execute('UPDATE players SET ref = NULL')
                cursor.execute(
                    "INSERT INTO system_flags (key, value) VALUES (?, ?)",
                    ('ref_reset_20260206', '1')
                )

            # Одноразовая очистка реф-ссылок (2026-02-06)
            cursor.execute("SELECT value FROM system_flags WHERE key = 'ref_links_cleanup_20260206'")
            if not cursor.fetchone():
                cursor.execute('UPDATE players SET ref = NULL, ref_link = NULL')
                cursor.execute('DELETE FROM chat_configs')
                cursor.execute('DELETE FROM user_ref_links')
                cursor.execute(
                    "INSERT INTO system_flags (key, value) VALUES (?, ?)",
                    ('ref_links_cleanup_20260206', '1')
                )

            # Одноразовая миграция временных удочек (2026-02-10)
            cursor.execute("SELECT value FROM system_flags WHERE key = 'temp_rods_migrated_20260210'")
            if not cursor.fetchone():
                rod_names = list(TEMP_ROD_RANGES.keys())
                cursor.execute(
                    f"SELECT id, rod_name FROM player_rods WHERE rod_name IN ({','.join(['?'] * len(rod_names))})",
                    rod_names
                )
                rows = cursor.fetchall()
                for rod_id, rod_name in rows:
                    uses = self._get_temp_rod_uses(rod_name)
                    if uses is None:
                        continue
                    cursor.execute(
                        '''
                        UPDATE player_rods
                        SET current_durability = ?, max_durability = ?, recovery_start_time = NULL, last_repair_time = NULL
                        WHERE id = ?
                        ''',
                        (uses, uses, rod_id)
                    )
                cursor.execute(
                    "INSERT INTO system_flags (key, value) VALUES (?, ?)",
                    ('temp_rods_migrated_20260210', '1')
                )

            # Одноразовая миграция опыта и уровней (2026-02-08)
            cursor.execute("SELECT value FROM system_flags WHERE key = 'xp_levels_migrated_20260208'")
            if not cursor.fetchone():
                cursor.execute('''
                    SELECT cf.user_id,
                           cf.weight,
                           COALESCE(f.rarity, 'Мусор') AS rarity,
                           COALESCE(f.min_weight, 0) AS min_weight,
                           COALESCE(f.max_weight, 0) AS max_weight,
                           CASE WHEN f.name IS NULL THEN 1 ELSE 0 END AS is_trash
                    FROM caught_fish cf
                    LEFT JOIN fish f ON TRIM(cf.fish_name) = f.name
                    WHERE cf.sold = 1
                ''')
                rows = cursor.fetchall()

                xp_by_user: Dict[int, int] = {}
                for user_id, weight, rarity, min_weight, max_weight, is_trash in rows:
                    item = {
                        'weight': weight,
                        'rarity': rarity,
                        'min_weight': min_weight,
                        'max_weight': max_weight,
                        'is_trash': bool(is_trash),
                    }
                    xp_value = self.calculate_item_xp(item)
                    xp_by_user[user_id] = xp_by_user.get(user_id, 0) + xp_value

                for user_id, xp_value in xp_by_user.items():
                    cursor.execute(
                        'SELECT COALESCE(xp, 0) FROM players WHERE user_id = ? ORDER BY created_at DESC LIMIT 1',
                        (user_id,)
                    )
                    row = cursor.fetchone()
                    current_xp = row[0] if row else 0
                    new_xp = max(current_xp, xp_value)
                    new_level = self.get_level_from_xp(new_xp)
                    cursor.execute(
                        'UPDATE players SET xp = ?, level = ? WHERE user_id = ?',
                        (new_xp, new_level, user_id)
                    )

                cursor.execute(
                    "INSERT INTO system_flags (key, value) VALUES (?, ?)",
                    ('xp_levels_migrated_20260208', '1')
                )
            
            conn.commit()
            
            # Миграция существующих игроков - добавляем недостающие поля
            cursor.execute('''
                UPDATE players SET 
                    current_location = COALESCE(current_location, 'Городской пруд'),
                    current_bait = COALESCE(current_bait, 'Черви'),
                    current_rod = COALESCE(NULLIF(current_rod, ''), 'Бамбуковая удочка')
                WHERE current_location IS NULL
                   OR current_bait IS NULL
                   OR current_rod IS NULL
                   OR current_rod = ''
            ''')
            conn.commit()
    
    def get_player(self, user_id: int, chat_id: int) -> Optional[Dict[str, Any]]:
        """Получить данные игрока (единый профиль на все чаты)"""
        with self._connect() as conn:
            cursor = conn.cursor()
            # If players table contains chat-specific rows, prefer the row for this chat_id.
            cursor.execute("PRAGMA table_info(players)")
            cols = [c[1] for c in cursor.fetchall()]
            if 'chat_id' in cols:
                # Prefer a global profile row (chat_id IS NULL or < 1) which stores shared data
                cursor.execute('SELECT * FROM players WHERE user_id = ? AND (chat_id IS NULL OR chat_id < 1) LIMIT 1', (user_id,))
                row = cursor.fetchone()
                if not row:
                    # No global profile yet — fallback to a per-chat row for compatibility
                    cursor.execute('SELECT * FROM players WHERE user_id = ? AND chat_id = ? LIMIT 1', (user_id, chat_id))
                    row = cursor.fetchone()
            else:
                cursor.execute('SELECT * FROM players WHERE user_id = ? ORDER BY created_at DESC LIMIT 1', (user_id,))
                row = cursor.fetchone()
            if row:
                columns = [description[0] for description in cursor.description]
                player: Dict[str, Any] = dict(zip(columns, row))

                # Debug: log which chat_id row was returned and its last_fish_time
                try:
                    returned_chat = player.get('chat_id')
                    logger.debug("get_player: returned row for user=%s requested_chat=%s returned_chat=%s last_fish=%s",
                                 user_id, chat_id, returned_chat, player.get('last_fish_time'))
                except Exception:
                    logger.debug("get_player: returned row for user=%s requested_chat=%s (no chat column)", user_id, chat_id)

                # Обеспечиваем наличие полей по умолчанию
                if not player.get('current_location'):
                    player['current_location'] = 'Городской пруд'
                if not player.get('current_bait'):
                    player['current_bait'] = 'Черви'
                if not player.get('current_rod'):
                    player['current_rod'] = 'Бамбуковая удочка'
                    cursor.execute('''
                        UPDATE players SET current_rod = ?
                        WHERE user_id = ?
                    ''', (player['current_rod'], user_id))
                    conn.commit()

                if player.get('xp') is None:
                    player['xp'] = 0
                if player.get('level') is None:
                    player['level'] = 0

                return player
            return None

    def has_any_player_profile(self, user_id: int) -> bool:
        """Проверить, есть ли профиль пользователя в любом чате"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT 1 FROM players WHERE user_id = ? LIMIT 1', (user_id,))
            return cursor.fetchone() is not None

    def has_any_referral(self, user_id: int) -> bool:
        """Проверить, является ли пользователь рефералом в любом чате"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT 1 FROM players WHERE user_id = ? AND ref IS NOT NULL LIMIT 1', (user_id,))
            return cursor.fetchone() is not None
    
    def create_player(self, user_id: int, username: str, chat_id: int) -> Optional[Dict[str, Union[str, int]]]:
        """Создать нового игрока (один профиль на все чаты)"""
        # If a profile for this exact (user_id, chat_id) exists, return it
        existing = self.get_player(user_id, chat_id)
        if existing:
            return existing

        # Try to copy values from any existing user profile to initialize a per-chat profile
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM players WHERE user_id = ? ORDER BY created_at DESC LIMIT 1', (user_id,))
            row = cursor.fetchone()
            if row:
                cols = [description[0] for description in cursor.description]
                template = dict(zip(cols, row))
                coins = template.get('coins', 100)
                stars = template.get('stars', 0)
                xp = template.get('xp', 0)
                level = template.get('level', 0)
                current_rod = template.get('current_rod', BAMBOO_ROD)
                current_bait = template.get('current_bait', 'Черви')
                current_location = template.get('current_location', 'Городской пруд')
            else:
                coins = 100
                stars = 0
                xp = 0
                level = 0
                current_rod = BAMBOO_ROD
                current_bait = 'Черви'
                current_location = 'Городской пруд'

            # Create a GLOBAL profile row (chat_id = -1) to store shared player data
            cursor.execute('''
                INSERT INTO players (user_id, username, coins, stars, xp, level, current_rod, current_bait, current_location, chat_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, -1)
            ''', (user_id, username, coins, stars, xp, level, current_rod, current_bait, current_location))
            conn.commit()

            # Инициализируем удочку и сеть для игрока в этом чате
            self.init_player_rod(user_id, current_rod, chat_id)
            self.init_player_net(user_id, 'Базовая сеть', chat_id)

            return self.get_player(user_id, chat_id)
    
    def update_player(self, user_id: int, chat_id: int, **kwargs: Dict[str, Union[str, int, float]]):
        """Обновить данные игрока (единый профиль на все чаты)"""
        if not kwargs:
            return

        # Allow only specific fields to be updated to avoid SQL injection
        allowed_fields = {
            'username', 'coins', 'stars', 'xp', 'level', 'current_rod', 'current_bait',
            'current_location', 'last_fish_time', 'is_banned', 'ban_until', 'ref', 'ref_link', 'last_net_use_time'
        }

        # Prevent passing chat_id as a kwarg (it is a positional arg here)
        if 'chat_id' in kwargs:
            kwargs.pop('chat_id', None)

        update_keys = [k for k in kwargs.keys() if k in allowed_fields]
        if not update_keys:
            return

        set_clause = ', '.join([f"{k} = ?" for k in update_keys])
        values: List[Union[str, int, float]] = [kwargs[k] for k in update_keys]

        # Decide whether to include chat_id in WHERE depending on DB schema
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(players)")
            columns = [col[1] for col in cursor.fetchall()]
            uses_chat = 'chat_id' in columns

            if uses_chat:
                # Prefer updating the GLOBAL profile row (chat_id IS NULL or <1)
                sql = f'UPDATE players SET {set_clause} WHERE user_id = ? AND (chat_id IS NULL OR chat_id < 1)'
                params = values + [user_id]
                cursor.execute(sql, params)
                if cursor.rowcount == 0:
                    # No global row — fall back to updating per-chat row
                    sql = f'UPDATE players SET {set_clause} WHERE user_id = ? AND chat_id = ?'
                    params = values + [user_id, chat_id]
            else:
                sql = f'UPDATE players SET {set_clause} WHERE user_id = ?'
                params = values + [user_id]

            # Defensive check -- ensure parameter count matches placeholders
            if sql.count('?') != len(params):
                # Log and adapt: try to trim trailing None values if any
                logger.error("Binding mismatch preparing UPDATE players: %s params=%s", sql, params)
                # Attempt best-effort: trim params to match
                if len(params) > sql.count('?'):
                    params = params[:sql.count('?')]
            cursor.execute(sql, params)
            conn.commit()
            # Log update result for debugging cooldown issues
            try:
                logger.debug("update_player: user=%s chat=%s sql=%s params=%s rows=%s",
                             user_id, chat_id, sql, params, cursor.rowcount)
            except Exception:
                logger.debug("update_player executed")

    def get_fish_by_location(self, location: str, season: str = "Лето", min_level: Optional[int] = None) -> List[Dict[str, Union[str, int, float]]]:
        """Получить список рыб для локации"""
        with self._connect() as conn:
            cursor = conn.cursor()
            query = '''
                SELECT * FROM fish 
                WHERE locations LIKE ? AND (seasons LIKE ? OR seasons LIKE '%Все%')
            '''
            params: List[Union[str, int]] = [f"%{location}%", f"%{season}%"]
            if min_level is not None:
                query += " AND required_level <= ?"
                params.append(min_level)
            query += " ORDER BY rarity"
            cursor.execute(query, params)
            rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            return [dict(zip(columns, row)) for row in rows]

    def get_fish_by_location_any_season(self, location: str, min_level: Optional[int] = None) -> List[Dict[str, Any]]:
        """Получить список рыб для локации без учета сезона"""
        with self._connect() as conn:
            cursor = conn.cursor()
            query = '''
                SELECT * FROM fish 
                WHERE locations LIKE ?
            '''
            params: List[Union[str, int]] = [f"%{location}%"]
            if min_level is not None:
                query += " AND required_level <= ?"
                params.append(min_level)
            query += " ORDER BY rarity"
            cursor.execute(query, params)
            rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
    
    def get_random_fish(self, location: str, season: str = "Лето", bait_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Получить случайную рыбу для локации с учетом наживки"""
        fish_list = self.get_fish_by_location(location, season)
        if not fish_list:
            return None

        if bait_name:
            fish_list = [fish for fish in fish_list if self.check_bait_suitable_for_fish(bait_name, fish['name'])]
            if not fish_list:
                return None
        
        # Взвешенный случайный выбор с учетом редкости
        weights = self.calculate_weights(fish_list)

        import random
        return random.choices(fish_list, weights=weights)[0]
    
    def add_caught_fish(self, user_id: int, chat_id: int, fish_name: str, weight: float, location: str, length: float = 0):
        """Добавить пойманную рыбу"""
        normalized_name = fish_name.strip() if isinstance(fish_name, str) else fish_name
        try:
            with self._connect() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO caught_fish (user_id, chat_id, fish_name, weight, length, location)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (user_id, chat_id, normalized_name, weight, length, location))
                conn.commit()
            logger.info(
                "Caught fish saved: user=%s chat_id=%s fish=%s weight=%.2fkg length=%.1fcm location=%s",
                user_id,
                chat_id,
                normalized_name,
                float(weight or 0),
                float(length or 0),
                location
            )
        except Exception as exc:
            logger.error(
                "Caught fish save failed: user=%s chat_id=%s fish=%s weight=%s length=%s location=%s error=%s",
                user_id,
                chat_id,
                normalized_name,
                weight,
                length,
                location,
                exc
            )
            raise
    
    def remove_caught_fish(self, fish_id: int):
        """Удалить пойманную рыбу по ID"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM caught_fish WHERE id = ?', (fish_id,))
            conn.commit()
    
    def mark_fish_as_sold(self, fish_ids: List[int]):
        """Пометить рыбу как проданную"""
        if not fish_ids:
            return

        # Some DB drivers (SQLite) have a limit on the number of bound parameters
        # allowed in a single statement. To be robust when selling many items at
        # once, perform the update in chunks.
        chunk_size = 500
        with self._connect() as conn:
            cursor = conn.cursor()
            total_updated = 0
            for i in range(0, len(fish_ids), chunk_size):
                chunk = fish_ids[i:i + chunk_size]
                placeholders = ','.join('?' * len(chunk))
                cursor.execute(f'''
                    UPDATE caught_fish 
                    SET sold = 1, sold_at = CURRENT_TIMESTAMP
                    WHERE id IN ({placeholders})
                ''', chunk)
                try:
                    updated = cursor.rowcount if hasattr(cursor, 'rowcount') else -1
                except Exception:
                    updated = -1
                if isinstance(updated, int) and updated > 0:
                    total_updated += updated
                logger.info("mark_fish_as_sold: chunk %s-%s updated %s rows", i, i+len(chunk)-1, updated)
            conn.commit()
            logger.info("mark_fish_as_sold: total ids=%s total_updated=%s", len(fish_ids), total_updated)
    
    def get_player_stats(self, user_id: int, chat_id: int) -> Dict[str, Any]:
        """Получить статистику игрока"""
        with self._connect() as conn:
            cursor = conn.cursor()
            
            # Общая статистика
            cursor.execute('''
                SELECT COUNT(*) as total_fish, 
                       SUM(weight) as total_weight,
                       COUNT(DISTINCT fish_name) as unique_fish
                FROM caught_fish cf
                JOIN fish f ON TRIM(cf.fish_name) = f.name
                WHERE cf.user_id = ?
            ''', (user_id,))
            
            stats = cursor.fetchone()

            cursor.execute('''
                SELECT COALESCE(SUM(cf.weight), 0) as trash_weight
                FROM caught_fish cf
                LEFT JOIN fish f ON TRIM(cf.fish_name) = f.name
                WHERE cf.user_id = ? AND f.name IS NULL
            ''', (user_id,))
            trash_weight_row = cursor.fetchone()
            trash_weight = trash_weight_row[0] if trash_weight_row else 0

            cursor.execute('''
                SELECT COUNT(*), COALESCE(SUM(cf.weight), 0)
                FROM caught_fish cf
                JOIN fish f ON TRIM(cf.fish_name) = f.name
                WHERE cf.user_id = ? AND cf.sold = 1
            ''', (user_id,))
            sold_row = cursor.fetchone()
            sold_count = sold_row[0] if sold_row else 0
            sold_weight = sold_row[1] if sold_row else 0
            
            # Самая большая рыба
            cursor.execute('''
                SELECT fish_name, weight FROM caught_fish 
                                WHERE user_id = ?
                                    AND TRIM(fish_name) IN (SELECT name FROM fish)
                                ORDER BY weight DESC LIMIT 1
                        ''', (user_id,))
            
            biggest = cursor.fetchone()
            
            return {
                'total_fish': stats[0] or 0,
                'total_weight': stats[1] or 0,
                'unique_fish': stats[2] or 0,
                'biggest_fish': biggest[0] if biggest else None,
                'biggest_weight': biggest[1] if biggest else 0,
                'trash_weight': trash_weight or 0,
                'sold_fish_count': sold_count or 0,
                'sold_fish_weight': sold_weight or 0
            }
    
    def get_rod(self, rod_name: str) -> Optional[Dict[str, Any]]:
        """Получить информацию об удочке"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM rods WHERE name = ?', (rod_name,))
            row = cursor.fetchone()
            if not row:
                return None
            columns = [description[0] for description in cursor.description]
            return dict(zip(columns, row))
    
    def get_rod_by_id(self, rod_id: int) -> Optional[Dict[str, Any]]:
        """Получить информацию об удочке по ID"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM rods WHERE id = ?', (rod_id,))
            row = cursor.fetchone()
            if not row:
                return None
            columns = [description[0] for description in cursor.description]
            return dict(zip(columns, row))
    
    def get_location(self, location_name: str) -> Optional[Dict[str, Any]]:
        """Получить информацию о локации"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM locations WHERE name = ?', (location_name,))
            row = cursor.fetchone()
            if row:
                columns = [description[0] for description in cursor.description]
                return dict(zip(columns, row))
            return None
    
    def update_location_players(self, location_name: str, delta: int):
        """Обновить количество игроков на локации"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE locations SET current_players = current_players + ? 
                WHERE name = ?
            ''', (delta, location_name))
            conn.commit()
    
    def update_player_location(self, user_id: int, chat_id: int, location_name: str):
        """Обновить локацию игрока"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE players SET current_location = ? WHERE user_id = ?
            ''', (location_name, user_id))
            conn.commit()
    
    def update_player_bait(self, user_id: int, chat_id: int, bait_name: str):
        """Обновить наживку игрока"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE players SET current_bait = ? WHERE user_id = ?
            ''', (bait_name, user_id))
            conn.commit()
    
    def buy_rod(self, user_id: int, chat_id: int, rod_name: str) -> bool:
        """Купить удочку"""
        with self._connect() as conn:
            cursor = conn.cursor()
            
            # Проверяем баланс и текущую удочку
            cursor.execute('''
                SELECT p.coins, r.price FROM players p
                JOIN rods r ON r.name = ?
                WHERE p.user_id = ?
            ''', (rod_name, user_id))
            
            result = cursor.fetchone()
            if not result:
                return False
            
            player_coins, rod_price = result
            
            if player_coins < rod_price:
                return False
            
            # Списываем монеты и обновляем удочку
            cursor.execute('''
                UPDATE players 
                SET coins = coins - ?, current_rod = ?
                WHERE user_id = ?
            ''', (rod_price, rod_name, user_id))
            
            conn.commit()
            self.init_player_rod(user_id, rod_name, chat_id)
            return True
    
    def clear_cooldown(self, user_id: int, chat_id: int) -> bool:
        """Очистить кулдаун рыбалки (старый метод для совместимости)"""
        with self._connect() as conn:
            cursor = conn.cursor()
            
            # Очистка кулдауна
            cursor.execute('''
                UPDATE players 
                SET last_fish_time = NULL
                WHERE user_id = ?
            ''', (user_id,))
            
            conn.commit()
            return True
    
    def get_caught_fish(self, user_id: int, chat_id: int) -> List[Dict[str, Any]]:
        """Получить всю пойманную рыбу пользователя"""
        with self._connect() as conn:
            cursor = conn.cursor()
            # Do NOT mutate DB when reading caught_fish (was assigning missing chat_id to current chat)
            # Previously this code updated rows with NULL/invalid chat_id to the current chat_id here,
            # which caused old catches to be retroactively reassigned when a user viewed `/stats`.
            # Keep reads side-effect free; use tools/fix_caught_fish_chatid.py or admin commands
            # to perform any explicit normalization instead.
            cursor.execute('''
                SELECT cf.*, 
                       COALESCE(f.name, t.name) AS name,
                       COALESCE(f.rarity, 'Мусор') AS rarity,
                       COALESCE(f.price, t.price, 0) AS price,
                       f.min_weight AS min_weight,
                       f.max_weight AS max_weight,
                       f.min_length AS min_length,
                       f.max_length AS max_length,
                       CASE WHEN f.name IS NULL THEN 1 ELSE 0 END AS is_trash
                FROM caught_fish cf
                LEFT JOIN fish f ON TRIM(cf.fish_name) = f.name
                LEFT JOIN trash t ON TRIM(cf.fish_name) = t.name
                WHERE cf.user_id = ? AND (cf.chat_id = ? OR cf.chat_id IS NULL OR cf.chat_id < 1)
                ORDER BY cf.weight DESC
            ''', (user_id, chat_id))
            
            rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            results = [dict(zip(columns, row)) for row in rows]

            for item in results:
                if item.get('is_trash'):
                    continue
                item['price'] = self.calculate_fish_price(item, item.get('weight', 0), item.get('length', 0))

            return results

    def calculate_fish_price(self, fish: Dict[str, Any], weight: float, length: float) -> int:
        """Рассчитать динамическую цену рыбы по редкости и размеру"""
        base_price = fish.get('price', 0) or 0
        rarity = fish.get('rarity', 'Обычная')

        rarity_multipliers = {
            'Обычная': 1.15,
            'Редкая': 1.5,
            'Легендарная': 2.2,
        }
        rarity_multiplier = rarity_multipliers.get(rarity, 1.0)

        min_weight = fish.get('min_weight') or 0
        max_weight = fish.get('max_weight') or 0
        min_length = fish.get('min_length') or 0
        max_length = fish.get('max_length') or 0

        def normalize(value: float, minimum: float, maximum: float) -> float:
            if maximum <= minimum:
                return 0.5
            return max(0.0, min(1.0, (value - minimum) / (maximum - minimum)))

        weight_ratio = normalize(weight, min_weight, max_weight)
        length_ratio = normalize(length, min_length, max_length)
        size_ratio = (0.7 * weight_ratio) + (0.3 * length_ratio)
        size_multiplier = 0.7 + (0.8 * size_ratio)

        price = int(round(base_price * rarity_multiplier * size_multiplier))
        return max(1, price)

    def get_level_from_xp(self, xp: int) -> int:
        """Получить уровень по суммарному опыту"""
        xp_value = max(0, int(xp or 0))
        level = 0
        for idx in range(1, len(LEVEL_XP_THRESHOLDS)):
            if xp_value >= LEVEL_XP_THRESHOLDS[idx]:
                level = idx
            else:
                break
        return min(level, MAX_LEVEL)

    def get_level_progress(self, xp: int) -> Dict[str, Any]:
        """Получить прогресс уровня по суммарному опыту"""
        xp_value = max(0, int(xp or 0))
        level = self.get_level_from_xp(xp_value)
        if level >= MAX_LEVEL:
            return {
                "level": MAX_LEVEL,
                "xp_total": xp_value,
                "level_start_xp": LEVEL_XP_THRESHOLDS[MAX_LEVEL],
                "next_level_xp": None,
                "xp_into_level": 0,
                "xp_needed": 0,
                "progress": 1.0,
            }

        level_start = LEVEL_XP_THRESHOLDS[level]
        next_level_xp = LEVEL_XP_THRESHOLDS[level + 1]
        xp_into_level = xp_value - level_start
        xp_needed = max(1, next_level_xp - level_start)
        progress = max(0.0, min(1.0, xp_into_level / xp_needed))

        return {
            "level": level,
            "xp_total": xp_value,
            "level_start_xp": level_start,
            "next_level_xp": next_level_xp,
            "xp_into_level": xp_into_level,
            "xp_needed": xp_needed,
            "progress": progress,
        }

    def calculate_item_xp_details(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Рассчитать опыт за предмет с деталями бонуса"""
        if item.get('is_trash') or item.get('rarity') == 'Мусор':
            return {
                'xp_total': 1,
                'xp_base': 1,
                'rarity_bonus': 0,
                'rarity_multiplier': 1.0,
                'weight_multiplier': 1.0,
                'weight_bonus': 0,
            }

        rarity = item.get('rarity', 'Обычная')
        base_xp = BASE_XP_BY_RARITY.get(rarity, BASE_XP_BY_RARITY['Обычная'])
        rarity_multiplier = RARITY_XP_MULTIPLIERS.get(rarity, 1.0)

        weight = float(item.get('weight') or 0)
        min_weight = float(item.get('min_weight') or 0)
        max_weight = float(item.get('max_weight') or 0)

        weight_multiplier = 1.0
        if max_weight > min_weight and weight > 0:
            ratio = (weight - min_weight) / (max_weight - min_weight)
            ratio = max(0.0, min(1.0, ratio))
            weight_multiplier = 1.0 + (0.6 * ratio)

        xp_before_weight = base_xp * rarity_multiplier
        xp_rarity = int(round(xp_before_weight))
        xp_total = int(round(xp_before_weight * weight_multiplier))
        xp_base = int(round(base_xp))
        rarity_bonus = max(0, xp_rarity - xp_base)
        weight_bonus = max(0, xp_total - xp_rarity)

        return {
            'xp_total': max(1, xp_total),
            'xp_base': max(1, xp_base),
            'rarity_bonus': rarity_bonus,
            'rarity_multiplier': rarity_multiplier,
            'weight_multiplier': weight_multiplier,
            'weight_bonus': weight_bonus,
        }

    def calculate_item_xp(self, item: Dict[str, Any]) -> int:
        """Рассчитать опыт за предмет (рыба или мусор)"""
        return self.calculate_item_xp_details(item)['xp_total']

    def calculate_weights(self, fish_list: List[Dict[str, Any]]) -> List[float]:
        """Вычислить веса для взвешенного случайного выбора рыб.

        Простая функция: базовые веса по редкости + небольшой вклад от среднего веса рыбы.
        """
        rarity_base = {
            'Обычная': 60.0,
            'Редкая': 30.0,
            'Легендарная': 10.0,
            'Мусор': 5.0,
        }
        weights: List[float] = []
        for fish in fish_list:
            rarity = fish.get('rarity') or 'Обычная'
            w = float(rarity_base.get(rarity, 20.0))
            try:
                min_w = float(fish.get('min_weight') or 0)
                max_w = float(fish.get('max_weight') or 0)
                if max_w > 0 and max_w >= min_w:
                    avg = (min_w + max_w) / 2.0
                    # add a small contribution from average weight
                    w += (avg * 1.0)
            except Exception:
                pass
            weights.append(max(1.0, w))
        return weights

    def add_player_xp(self, user_id: int, chat_id: int, xp_amount: int) -> Dict[str, Any]:
        """Добавить опыт игроку и обновить уровень"""
        xp_delta = int(xp_amount or 0)
        with self._connect() as conn:
            cursor = conn.cursor()
            # Use the same player-row selection logic as `update_player`:
            # prefer a global profile row (chat_id IS NULL or <1) when chat-aware schema is used.
            cursor.execute('PRAGMA table_info(players)')
            cols = [c[1] for c in cursor.fetchall()]
            if 'chat_id' in cols:
                cursor.execute('SELECT COALESCE(xp, 0), COALESCE(level, 0) FROM players WHERE user_id = ? AND (chat_id IS NULL OR chat_id < 1) LIMIT 1', (user_id,))
                row = cursor.fetchone()
                if not row:
                    cursor.execute('SELECT COALESCE(xp, 0), COALESCE(level, 0) FROM players WHERE user_id = ? AND chat_id = ? LIMIT 1', (user_id, chat_id))
                    row = cursor.fetchone()
                current_xp = row[0] if row else 0
                current_level = row[1] if row else 0
                new_xp = max(0, current_xp + xp_delta)
                new_level = self.get_level_from_xp(new_xp)
                # update global row if exists, else update per-chat row
                cursor.execute('UPDATE players SET xp = ?, level = ? WHERE user_id = ? AND (chat_id IS NULL OR chat_id < 1)', (new_xp, new_level, user_id))
                if cursor.rowcount == 0:
                    cursor.execute('UPDATE players SET xp = ?, level = ? WHERE user_id = ? AND chat_id = ?', (new_xp, new_level, user_id, chat_id))
            else:
                cursor.execute('SELECT COALESCE(xp, 0), COALESCE(level, 0) FROM players WHERE user_id = ? ORDER BY created_at DESC LIMIT 1', (user_id,))
                row = cursor.fetchone()
                current_xp = row[0] if row else 0
                current_level = row[1] if row else 0
                new_xp = max(0, current_xp + xp_delta)
                new_level = self.get_level_from_xp(new_xp)
                cursor.execute('UPDATE players SET xp = ?, level = ? WHERE user_id = ?', (new_xp, new_level, user_id))
            conn.commit()

        progress = self.get_level_progress(new_xp)
        progress['leveled_up'] = new_level > (current_level or 0)
        return progress
    
    def get_leaderboard(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Получить таблицу лидеров (по умолчанию - глобально за все время)"""
        return self.get_leaderboard_period(limit=limit)

    def get_level_leaderboard(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Получить топ по уровню (глобально)"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                SELECT
                    COALESCE(MAX(username), 'Неизвестно') as username,
                    user_id,
                    MAX(COALESCE(level, 0)) as level,
                    MAX(COALESCE(xp, 0)) as xp
                FROM players
                GROUP BY user_id
                ORDER BY level DESC, xp DESC
                LIMIT ?
                ''',
                (limit,)
            )
            rows = cursor.fetchall()
            return [
                {
                    'username': row[0],
                    'user_id': row[1],
                    'level': row[2],
                    'xp': row[3],
                }
                for row in rows
            ]

    def get_leaderboard_period(self, limit: int = 10, since: Optional[datetime] = None, chat_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """Получить таблицу лидеров за период и/или по чату"""
        with self._connect() as conn:
            cursor = conn.cursor()

            where_clauses: List[str] = []
            params: List = []

            # Always join players to get username
            join_clause = "LEFT JOIN players p ON p.user_id = cf.user_id"

            # If chat_id provided, filter strictly by integer chat_id stored in caught_fish
            if chat_id is not None:
                where_clauses.append("CAST(cf.chat_id AS BIGINT) = ?")
                params.append(int(chat_id))

            if since is not None:
                where_clauses.append("datetime(cf.caught_at) >= datetime(?)")
                params.append(since.strftime("%Y-%m-%d %H:%M:%S"))

            where_clauses.append("cf.sold = 0")

            where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

            query = f'''
                SELECT 
                    COALESCE(MAX(p.username), 'Неизвестно') as username,
                    cf.user_id as user_id,
                    COUNT(cf.id) as total_fish,
                    COALESCE(SUM(cf.weight), 0) as total_weight
                FROM caught_fish cf
                JOIN fish f ON TRIM(cf.fish_name) = f.name
                {join_clause}
                {where_sql}
                GROUP BY cf.user_id
                ORDER BY total_weight DESC, total_fish DESC
                LIMIT ?
            '''

            params.append(limit)
            cursor.execute(query, params)

            rows = cursor.fetchall()
            return [
                {
                    'username': row[0],
                    'user_id': row[1],
                    'total_fish': row[2],
                    'total_weight': row[3]
                }
                for row in rows
            ]

    # ===== ТУРНИРЫ =====

    def create_tournament(
        self,
        chat_id: int,
        created_by: int,
        title: str,
        tournament_type: str,
        starts_at: datetime,
        ends_at: datetime,
        target_fish: Optional[str] = None,
    ) -> int:
        """Создать турнир и вернуть его ID."""
        starts_str = starts_at.strftime('%Y-%m-%d %H:%M:%S')
        ends_str = ends_at.strftime('%Y-%m-%d %H:%M:%S')
        with self._connect() as conn:
            cursor = conn.cursor()
            if type(cursor).__module__.startswith('sqlite3'):
                cursor.execute(
                    '''
                    INSERT INTO tournaments (chat_id, created_by, title, tournament_type, target_fish, starts_at, ends_at, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'scheduled')
                    ''',
                    (int(chat_id), int(created_by), title, tournament_type, target_fish, starts_str, ends_str)
                )
            else:
                cursor.execute(
                    '''
                    INSERT INTO tournaments (chat_id, created_by, title, tournament_type, target_fish, starts_at, ends_at, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, 'scheduled')
                    RETURNING id
                    ''',
                    (int(chat_id), int(created_by), title, tournament_type, target_fish, starts_str, ends_str)
                )
            conn.commit()

            if type(cursor).__module__.startswith('sqlite3'):
                return int(cursor.lastrowid)
            row = cursor.fetchone()
            return int(row[0]) if row else 0

    def get_tournament(self, tournament_id: int) -> Optional[Dict[str, Any]]:
        """Получить турнир по ID."""
        with self._connect() as conn:
            cursor = conn.cursor()
            if type(cursor).__module__.startswith('sqlite3'):
                cursor.execute('SELECT * FROM tournaments WHERE id = ? LIMIT 1', (int(tournament_id),))
            else:
                cursor.execute('SELECT * FROM tournaments WHERE id = %s LIMIT 1', (int(tournament_id),))
            row = cursor.fetchone()
            if not row:
                return None
            cols = [d[0] for d in cursor.description]
            return dict(zip(cols, row))

    def get_active_tournament(self, chat_id: int, now_dt: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
        """Получить активный (или ближайший scheduled) турнир для чата."""
        now_dt = now_dt or datetime.now()
        now_str = now_dt.strftime('%Y-%m-%d %H:%M:%S')
        with self._connect() as conn:
            cursor = conn.cursor()
            if type(cursor).__module__.startswith('sqlite3'):
                cursor.execute(
                    '''
                    SELECT * FROM tournaments
                    WHERE chat_id = ?
                      AND (
                        (datetime(starts_at) <= datetime(?) AND datetime(ends_at) >= datetime(?))
                        OR status = 'scheduled'
                      )
                    ORDER BY
                      CASE WHEN datetime(starts_at) <= datetime(?) AND datetime(ends_at) >= datetime(?) THEN 0 ELSE 1 END,
                      datetime(starts_at) ASC
                    LIMIT 1
                    ''',
                    (int(chat_id), now_str, now_str, now_str, now_str)
                )
            else:
                cursor.execute(
                    '''
                    SELECT * FROM tournaments
                    WHERE chat_id = %s
                      AND (
                        (starts_at <= %s AND ends_at >= %s)
                        OR status = 'scheduled'
                      )
                    ORDER BY
                      CASE WHEN starts_at <= %s AND ends_at >= %s THEN 0 ELSE 1 END,
                      starts_at ASC
                    LIMIT 1
                    ''',
                    (int(chat_id), now_str, now_str, now_str, now_str)
                )
            row = cursor.fetchone()
            if not row:
                return None
            cols = [d[0] for d in cursor.description]
            return dict(zip(cols, row))

    def get_tournament_leaderboard(self, tournament_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        """Получить лидерборд турнира по его типу."""
        tour = self.get_tournament(tournament_id)
        if not tour:
            return []

        tour_type = (tour.get('tournament_type') or '').strip()
        target_fish = (tour.get('target_fish') or '').strip() or None
        chat_id = int(tour.get('chat_id'))
        starts_at = str(tour.get('starts_at'))
        ends_at = str(tour.get('ends_at'))

        metric_expr = 'COALESCE(SUM(cf.weight), 0)'
        extra_filter = ''
        extra_params: List[Any] = []

        if tour_type == 'longest_fish':
            metric_expr = 'COALESCE(MAX(cf.length), 0)'
        elif tour_type == 'biggest_weight':
            metric_expr = 'COALESCE(MAX(cf.weight), 0)'
        elif tour_type == 'total_weight':
            metric_expr = 'COALESCE(SUM(cf.weight), 0)'
        elif tour_type == 'specific_fish':
            metric_expr = 'COALESCE(SUM(cf.weight), 0)'
            extra_filter = ' AND TRIM(cf.fish_name) = ? '
            if target_fish:
                extra_params.append(target_fish)
            else:
                return []
        else:
            return []

        with self._connect() as conn:
            cursor = conn.cursor()
            if type(cursor).__module__.startswith('sqlite3'):
                sql = f'''
                    SELECT
                        COALESCE(MAX(p.username), 'Неизвестно') AS username,
                        cf.user_id,
                        {metric_expr} AS metric,
                        COUNT(cf.id) AS catches_count
                    FROM caught_fish cf
                    LEFT JOIN players p ON p.user_id = cf.user_id
                    WHERE CAST(cf.chat_id AS BIGINT) = ?
                      AND datetime(cf.caught_at) >= datetime(?)
                      AND datetime(cf.caught_at) <= datetime(?)
                      {extra_filter}
                    GROUP BY cf.user_id
                    ORDER BY metric DESC, catches_count DESC
                    LIMIT ?
                '''
                params = [chat_id, starts_at, ends_at] + extra_params + [int(limit)]
                cursor.execute(sql, params)
            else:
                sql = f'''
                    SELECT
                        COALESCE(MAX(p.username), 'Неизвестно') AS username,
                        cf.user_id,
                        {metric_expr} AS metric,
                        COUNT(cf.id) AS catches_count
                    FROM caught_fish cf
                    LEFT JOIN players p ON p.user_id = cf.user_id
                    WHERE CAST(cf.chat_id AS BIGINT) = %s
                      AND cf.caught_at >= %s
                      AND cf.caught_at <= %s
                      {extra_filter.replace('?', '%s')}
                    GROUP BY cf.user_id
                    ORDER BY metric DESC, catches_count DESC
                    LIMIT %s
                '''
                params = [chat_id, starts_at, ends_at] + extra_params + [int(limit)]
                cursor.execute(sql, params)

            rows = cursor.fetchall()
            return [
                {
                    'username': row[0],
                    'user_id': int(row[1]),
                    'metric': float(row[2] or 0),
                    'catches_count': int(row[3] or 0),
                    'tournament_type': tour_type,
                    'target_fish': target_fish,
                }
                for row in rows
            ]
    
    def get_rods(self) -> List[Dict[str, Any]]:
        """Получить список всех удочек"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM rods ORDER BY price')
            rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
    
    def get_locations(self) -> List[Dict[str, Any]]:
        """Получить список всех локаций"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM locations ORDER BY id')
            rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            return [dict(zip(columns, row)) for row in rows]

    def get_location_players_count(self, location_name: str, chat_id: int) -> int:
        """Получить количество игроков на локации в конкретном чате"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT COUNT(*)
                FROM players
                WHERE current_location = ?
            ''', (location_name,))
            result = cursor.fetchone()
            return result[0] if result else 0
    
    def get_baits(self) -> List[Dict[str, Any]]:
        """Получить список всех наживок"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM baits ORDER BY name')
            rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            return [dict(zip(columns, row)) for row in rows]

    def get_bait_by_id(self, bait_id: int) -> Optional[Dict[str, Any]]:
        """Получить наживку по ID"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM baits WHERE id = ?', (bait_id,))
            row = cursor.fetchone()
            if row:
                columns = [description[0] for description in cursor.description]
                return dict(zip(columns, row))
            return None
    
    def get_player_baits(self, user_id: int) -> List[Dict[str, Any]]:
        """Получить наживки игрока"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT b.*, COALESCE(pb.quantity, 0) as player_quantity 
                FROM baits b 
                LEFT JOIN player_baits pb ON b.name = pb.bait_name AND pb.user_id = ?
                ORDER BY b.fish_bonus DESC
            ''', (user_id,))
            rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
    
    def get_bait_count(self, user_id: int, bait_name: str) -> int:
        """Получить количество наживки у игрока"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT quantity FROM player_baits 
                WHERE user_id = ? AND bait_name = ?
            ''', (user_id, bait_name))
            result = cursor.fetchone()
            return result[0] if result else 0
    
    def add_bait_to_inventory(self, user_id: int, bait_name: str, quantity: int = 1):
        """Добавить наживку в инвентарь"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO player_baits (user_id, bait_name, quantity)
                VALUES (?, ?, COALESCE((SELECT quantity FROM player_baits WHERE user_id = ? AND bait_name = ?), 0) + ?)
            ''', (user_id, bait_name, user_id, bait_name, quantity))
            conn.commit()
    
    def use_bait(self, user_id: int, bait_name: str) -> bool:
        """Использовать наживку"""
        with self._connect() as conn:
            cursor = conn.cursor()
            
            # Проверяем количество наживки
            cursor.execute('''
                SELECT quantity FROM player_baits 
                WHERE user_id = ? AND bait_name = ?
            ''', (user_id, bait_name))
            result = cursor.fetchone()
            
            if not result or result[0] <= 0:
                return False
            
            # Уменьшаем количество
            cursor.execute('''
                UPDATE player_baits SET quantity = quantity - 1
                WHERE user_id = ? AND bait_name = ?
            ''', (user_id, bait_name))
            
            # Удаляем если количество 0
            cursor.execute('''
                DELETE FROM player_baits WHERE user_id = ? AND bait_name = ? AND quantity <= 0
            ''', (user_id, bait_name))
            
            conn.commit()
            return True
    
    def get_trash_by_location(self, location: str) -> List[Dict[str, Any]]:
        """Получить список мусора для локации"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM trash 
                WHERE locations LIKE ?
                ORDER BY name
            ''', (f"%{location}%",))
            rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
    
    def get_random_trash(self, location: str) -> Optional[Dict[str, Any]]:
        """Получить случайный мусор для локации"""
        trash_list = self.get_trash_by_location(location)
        if not trash_list:
            with self._connect() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT * FROM trash ORDER BY name')
                rows = cursor.fetchall()
                columns = [description[0] for description in cursor.description]
                trash_list = [dict(zip(columns, row)) for row in rows]
            if not trash_list:
                return None
        
        import random
        return random.choice(trash_list)
    
    def check_bait_suitable_for_fish(self, bait_name: str, fish_name: str) -> bool:
        """Проверить подходит ли наживка для рыбы"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT suitable_baits FROM fish WHERE name = ?
            ''', (fish_name,))
            result = cursor.fetchone()
            
            if not result:
                return False
            
            suitable_baits = result[0]
            if suitable_baits == "Все":
                return True
            
            # Сравниваем без учёта регистра и пробелов
            suitable_list = [b.strip().lower() for b in suitable_baits.split(',') if b.strip()]
            if not bait_name:
                return False
            return bait_name.strip().lower() in suitable_list

    def _upsert_ref_stars_stats(self, user_id: int, chat_id: int, received: int = 0, spent: int = 0, refunded: int = 0, withdrawn: int = 0) -> bool:
        """Обновить агрегаты реф-звёзд для пары (user_id, chat_id)."""
        if user_id is None or chat_id is None:
            return False

        try:
            with self._connect() as conn:
                cursor = conn.cursor()
                if type(cursor).__module__.startswith('sqlite3'):
                    cursor.execute(
                        '''
                        INSERT INTO ref_stars_stats (
                            user_id, chat_id, stars_received, stars_spent, stars_refunded, stars_withdrawn, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                        ON CONFLICT(user_id, chat_id) DO UPDATE SET
                            stars_received = COALESCE(ref_stars_stats.stars_received, 0) + excluded.stars_received,
                            stars_spent = COALESCE(ref_stars_stats.stars_spent, 0) + excluded.stars_spent,
                            stars_refunded = COALESCE(ref_stars_stats.stars_refunded, 0) + excluded.stars_refunded,
                            stars_withdrawn = COALESCE(ref_stars_stats.stars_withdrawn, 0) + excluded.stars_withdrawn,
                            updated_at = CURRENT_TIMESTAMP
                        ''',
                        (int(user_id), int(chat_id), int(received), int(spent), int(refunded), int(withdrawn))
                    )
                else:
                    cursor.execute(
                        '''
                        INSERT INTO ref_stars_stats (
                            user_id, chat_id, stars_received, stars_spent, stars_refunded, stars_withdrawn, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (user_id, chat_id) DO UPDATE SET
                            stars_received = COALESCE(ref_stars_stats.stars_received, 0) + EXCLUDED.stars_received,
                            stars_spent = COALESCE(ref_stars_stats.stars_spent, 0) + EXCLUDED.stars_spent,
                            stars_refunded = COALESCE(ref_stars_stats.stars_refunded, 0) + EXCLUDED.stars_refunded,
                            stars_withdrawn = COALESCE(ref_stars_stats.stars_withdrawn, 0) + EXCLUDED.stars_withdrawn,
                            updated_at = CURRENT_TIMESTAMP
                        ''',
                        (int(user_id), int(chat_id), int(received), int(spent), int(refunded), int(withdrawn))
                    )
                conn.commit()
                return True
        except Exception as e:
            logger.error("_upsert_ref_stars_stats error: %s", e)
            return False

    def _resolve_ref_user_for_chat(self, cursor, chat_id: int) -> Optional[int]:
        """Определить владельца/рефера для чата через chat_configs.admin_user_id."""
        try:
            if type(cursor).__module__.startswith('sqlite3'):
                cursor.execute('SELECT admin_user_id FROM chat_configs WHERE chat_id = ? LIMIT 1', (chat_id,))
            else:
                cursor.execute('SELECT admin_user_id FROM chat_configs WHERE chat_id = %s LIMIT 1', (chat_id,))
            row = cursor.fetchone()
            if not row:
                return None
            admin_user_id = int(row[0]) if row[0] is not None else None
            return admin_user_id if admin_user_id and admin_user_id > 0 else None
        except Exception:
            return None

    def add_star_transaction(self, user_id: int, telegram_payment_charge_id: str, total_amount: int, refund_status: str = "none", chat_id: Optional[int] = None, chat_title: Optional[str] = None) -> bool:
        """Добавить запись о транзакции Telegram Stars"""
        if not telegram_payment_charge_id:
            return False
        with self._connect() as conn:
            cursor = conn.cursor()
            # If DB has chat_id/chat_title columns, insert them as well when provided via kwargs
            try:
                cursor.execute("PRAGMA table_info(star_transactions)")
                cols = [c[1] for c in cursor.fetchall()]
            except Exception:
                cols = []

            if 'chat_id' in cols and 'chat_title' in cols:
                cursor.execute('''
                    INSERT OR IGNORE INTO star_transactions (user_id, telegram_payment_charge_id, total_amount, chat_id, chat_title, refund_status)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (user_id, telegram_payment_charge_id, total_amount, chat_id, chat_title, refund_status))
            else:
                cursor.execute('''
                    INSERT OR IGNORE INTO star_transactions (user_id, telegram_payment_charge_id, total_amount, refund_status)
                    VALUES (?, ?, ?, ?)
                ''', (user_id, telegram_payment_charge_id, total_amount, refund_status))
            conn.commit()
            return cursor.rowcount > 0

    def increment_chat_stars(self, chat_id: int, amount: int, chat_title: Optional[str] = None) -> bool:
        """Увеличить счётчик звёзд для чата. Создаст запись если нужно."""
        if chat_id is None:
            return False
        try:
            with self._connect() as conn:
                cursor = conn.cursor()
                is_sqlite = type(cursor).__module__.startswith('sqlite3')
                if is_sqlite:
                    cursor.execute('INSERT OR IGNORE INTO chat_configs (chat_id, admin_user_id, is_configured, chat_title, stars_total) VALUES (?, ?, 1, ?, 0)', (chat_id, 0, chat_title))
                    if chat_title is not None:
                        cursor.execute('UPDATE chat_configs SET chat_title = ? WHERE chat_id = ?', (chat_title, chat_id))
                    cursor.execute('UPDATE chat_configs SET stars_total = COALESCE(stars_total, 0) + ? WHERE chat_id = ?', (amount, chat_id))
                else:
                    cursor.execute(
                        'INSERT INTO chat_configs (chat_id, admin_user_id, is_configured, chat_title, stars_total) VALUES (%s, %s, 1, %s, 0) ON CONFLICT (chat_id) DO NOTHING',
                        (chat_id, 0, chat_title)
                    )
                    if chat_title is not None:
                        cursor.execute('UPDATE chat_configs SET chat_title = %s WHERE chat_id = %s', (chat_title, chat_id))
                    cursor.execute('UPDATE chat_configs SET stars_total = COALESCE(stars_total, 0) + %s WHERE chat_id = %s', (amount, chat_id))

                ref_user_id = self._resolve_ref_user_for_chat(cursor, chat_id)
                if ref_user_id:
                    self._upsert_ref_stars_stats(ref_user_id, chat_id, received=int(amount))

                conn.commit()
                return True
        except Exception as e:
            logger.error("increment_chat_stars error: %s", e)
            return False

    def get_all_chat_stars(self) -> List[Dict[str, Any]]:
        """Return list of group/channel chats with title and total stars."""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT chat_id, COALESCE(chat_title, "") as chat_title, COALESCE(stars_total,0) as stars_total '
                'FROM chat_configs '
                'WHERE chat_id < 0 '
                'ORDER BY stars_total DESC'
            )
            rows = cursor.fetchall()
            cols = [d[0] for d in cursor.description]
            return [dict(zip(cols, r)) for r in rows]

    def update_chat_title(self, chat_id: int, chat_title: str) -> bool:
        """Update chat title in chat_configs."""
        if chat_id is None or not chat_title:
            return False
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('INSERT OR IGNORE INTO chat_configs (chat_id, admin_user_id, is_configured, chat_title, stars_total) VALUES (?, ?, 1, ?, 0)', (chat_id, 0, chat_title))
            cursor.execute('UPDATE chat_configs SET chat_title = ? WHERE chat_id = ?', (chat_title, chat_id))
            conn.commit()
            return True

    def get_star_transaction(self, telegram_payment_charge_id: str) -> Optional[Dict[str, Any]]:
        """Получить транзакцию по telegram_payment_charge_id"""
        if not telegram_payment_charge_id:
            return None
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM star_transactions WHERE telegram_payment_charge_id = ?
            ''', (telegram_payment_charge_id,))
            row = cursor.fetchone()
            if row:
                columns = [description[0] for description in cursor.description]
                return dict(zip(columns, row))
            return None

    def update_star_refund_status(self, telegram_payment_charge_id: str, refund_status: str) -> bool:
        """Обновить статус возврата по транзакции"""
        if not telegram_payment_charge_id:
            return False
        with self._connect() as conn:
            cursor = conn.cursor()
            is_sqlite = type(cursor).__module__.startswith('sqlite3')
            if is_sqlite:
                cursor.execute(
                    'SELECT user_id, chat_id, total_amount, refund_status FROM star_transactions WHERE telegram_payment_charge_id = ? LIMIT 1',
                    (telegram_payment_charge_id,)
                )
            else:
                cursor.execute(
                    'SELECT user_id, chat_id, total_amount, refund_status FROM star_transactions WHERE telegram_payment_charge_id = %s LIMIT 1',
                    (telegram_payment_charge_id,)
                )
            prev = cursor.fetchone()

            if is_sqlite:
                cursor.execute('''
                    UPDATE star_transactions
                    SET refund_status = ?
                    WHERE telegram_payment_charge_id = ?
                ''', (refund_status, telegram_payment_charge_id))
            else:
                cursor.execute('''
                    UPDATE star_transactions
                    SET refund_status = %s
                    WHERE telegram_payment_charge_id = %s
                ''', (refund_status, telegram_payment_charge_id))

            if cursor.rowcount > 0 and prev:
                tx_user_id, tx_chat_id, tx_amount, prev_status = prev
                new_status = (refund_status or '').strip().lower()
                old_status = (prev_status or '').strip().lower()
                if tx_chat_id is not None and new_status in {'ref', 'refunded'} and old_status not in {'ref', 'refunded'}:
                    ref_user_id = self._resolve_ref_user_for_chat(cursor, int(tx_chat_id)) or tx_user_id
                    if ref_user_id:
                        self._upsert_ref_stars_stats(
                            int(ref_user_id),
                            int(tx_chat_id),
                            spent=int(tx_amount or 0),
                            refunded=int(tx_amount or 0)
                        )
            conn.commit()
            return cursor.rowcount > 0
    
    def get_baits_for_location(self, location: str) -> List[Dict[str, Any]]:
        """Получить наживки, подходящие для рыбы на данной локации"""
        with self._connect() as conn:
            cursor = conn.cursor()
            # Получаем все уникальные наживки для рыб на этой локации
            cursor.execute('''
                SELECT DISTINCT b.*
                FROM baits b
                WHERE EXISTS (
                    SELECT 1 FROM fish f
                    WHERE f.locations LIKE ? 
                    AND (f.suitable_baits LIKE '%' || b.name || '%' OR f.suitable_baits = 'Все')
                )
                ORDER BY b.price
            ''', (f'%{location}%',))
            rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
    
    def get_player_baits_for_location(self, user_id: int, location: str) -> List[Dict[str, Any]]:
        """Получить наживки игрока, подходящие для локации"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT b.*, COALESCE(pb.quantity, 0) as player_quantity
                FROM baits b
                LEFT JOIN player_baits pb ON b.name = pb.bait_name AND pb.user_id = ?
                WHERE EXISTS (
                    SELECT 1 FROM fish f
                    WHERE f.locations LIKE ?
                    AND (f.suitable_baits LIKE '%' || b.name || '%' OR f.suitable_baits = 'Все')
                )
                AND COALESCE(pb.quantity, 0) > 0
                ORDER BY b.name
            ''', (user_id, f'%{location}%'))
            rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            return [dict(zip(columns, row)) for row in rows]

    def get_weather(self, location: str) -> Optional[Dict[str, Any]]:
        """Получить погоду локации"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM weather WHERE location = ?', (location,))
            row = cursor.fetchone()
            if row:
                columns = [description[0] for description in cursor.description]
                return dict(zip(columns, row))
            return None

    def get_or_update_weather(self, location: str) -> Dict[str, Any]:
        """Получить или обновить информацию о погоде"""
        from weather import weather_system

        with self._connect() as conn:
            cursor = conn.cursor()

            cursor.execute('SELECT * FROM weather WHERE location = ?', (location,))
            row = cursor.fetchone()

            if not row:
                condition, temp = weather_system.generate_weather(location)
                cursor.execute('''
                    INSERT INTO weather (location, condition, temperature)
                    VALUES (?, ?, ?)
                ''', (location, condition, temp))
                conn.commit()
                cursor.execute('SELECT * FROM weather WHERE location = ?', (location,))
                row = cursor.fetchone()

            columns = [description[0] for description in cursor.description]
            weather = dict(zip(columns, row))

            if weather_system.should_update_weather(weather['last_updated']):
                new_condition, new_temp = weather_system.generate_weather(location)
                cursor.execute('''
                    UPDATE weather 
                    SET condition = ?, temperature = ?, last_updated = CURRENT_TIMESTAMP
                    WHERE location = ? AND last_updated = ?
                ''', (new_condition, new_temp, location, weather['last_updated']))
                conn.commit()

                if cursor.rowcount:
                    cursor.execute('SELECT * FROM weather WHERE location = ?', (location,))
                    row = cursor.fetchone()
                    columns = [description[0] for description in cursor.description]
                    weather = dict(zip(columns, row))
                else:
                    # Погода уже обновлена другим процессом/запросом
                    cursor.execute('SELECT * FROM weather WHERE location = ?', (location,))
                    row = cursor.fetchone()
                    columns = [description[0] for description in cursor.description]
                    weather = dict(zip(columns, row))

            return weather

    def update_weather(self, location: str, condition: str, temperature: int):
        """Обновить погоду локации"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE weather 
                SET condition = ?, temperature = ?, last_updated = CURRENT_TIMESTAMP
                WHERE location = ?
            ''', (condition, temperature, location))
            conn.commit()

    def init_player_rod(self, user_id: int, rod_name: str, chat_id: int):
        """Инициализировать удочку для игрока"""
        with self._connect() as conn:
            cursor = conn.cursor()
            rod = self.get_rod(rod_name)
            if not rod:
                return False
            uses = self._get_temp_rod_uses(rod_name)
            if uses is None:
                uses = rod['max_durability']

            # Initialize as a GLOBAL rod entry (chat_id = -1) so rod state is shared across chats
            cursor.execute('''
                INSERT OR IGNORE INTO player_rods (user_id, rod_name, current_durability, max_durability, chat_id)
                VALUES (?, ?, ?, ?, -1)
            ''', (user_id, rod_name, uses, uses))
            conn.commit()
            return True

    def get_player_rod(self, user_id: int, rod_name: str, chat_id: int) -> Optional[Dict[str, Any]]:
        """Получить состояние удочки игрока"""
        with self._connect() as conn:
            cursor = conn.cursor()
            # Prefer a global rod row (chat_id IS NULL or <1)
            cursor.execute('SELECT * FROM player_rods WHERE user_id = ? AND (chat_id IS NULL OR chat_id < 1) AND rod_name = ? LIMIT 1', (user_id, rod_name))
            row = cursor.fetchone()
            if not row:
                # Fallback to any per-chat row for compatibility
                cursor.execute('SELECT * FROM player_rods WHERE user_id = ? AND rod_name = ? LIMIT 1', (user_id, rod_name))
                row = cursor.fetchone()
            if row:
                columns = [description[0] for description in cursor.description]
                return dict(zip(columns, row))
            return None

    def consume_temp_rod_use(self, user_id: int, rod_name: str, chat_id: int) -> Dict[str, Any]:
        """Списать один удачный улов для временной удочки"""
        if rod_name == BAMBOO_ROD:
            return {"remaining": None, "broken": False}

        if rod_name not in TEMP_ROD_RANGES:
            return {"remaining": None, "broken": False}

        with self._connect() as conn:
            cursor = conn.cursor()
            # Prefer global rod row
            cursor.execute('''
                SELECT current_durability, max_durability FROM player_rods
                WHERE user_id = ? AND (chat_id IS NULL OR chat_id < 1) AND rod_name = ?
            ''', (user_id, rod_name))
            row = cursor.fetchone()
            if not row:
                # No global rod found - initialize a global rod
                self.init_player_rod(user_id, rod_name, chat_id)
                cursor.execute('''
                    SELECT current_durability, max_durability FROM player_rods
                    WHERE user_id = ? AND (chat_id IS NULL OR chat_id < 1) AND rod_name = ?
                ''', (user_id, rod_name))
                row = cursor.fetchone()

            current_dur, max_dur = row if row else (0, 0)
            current_dur = max(0, current_dur - 1)
            if current_dur <= 0:
                # Delete the global rod entry
                cursor.execute('DELETE FROM player_rods WHERE user_id = ? AND (chat_id IS NULL OR chat_id < 1) AND rod_name = ?', (user_id, rod_name))
                conn.commit()
                return {"remaining": 0, "max": max_dur, "broken": True}

            cursor.execute('''
                UPDATE player_rods SET current_durability = ?
                WHERE user_id = ? AND (chat_id IS NULL OR chat_id < 1) AND rod_name = ?
            ''', (current_dur, user_id, rod_name))
            conn.commit()
            return {"remaining": current_dur, "max": max_dur, "broken": False}

    def reduce_rod_durability(self, user_id: int, rod_name: str, damage: int, chat_id: int):
        """Уменьшить прочность удочки"""
        if rod_name != BAMBOO_ROD:
            return
        with self._connect() as conn:
            cursor = conn.cursor()
            
            # Проверяем, существует ли запись для этой удочки
            if type(cursor).__module__.startswith('sqlite3'):
                cursor.execute('''
                    SELECT current_durability FROM player_rods 
                    WHERE user_id = ? AND (chat_id IS NULL OR chat_id < 1) AND rod_name = ?
                ''', (user_id, rod_name))
            else:
                cursor.execute('''
                    SELECT current_durability FROM player_rods 
                    WHERE user_id = %s AND (chat_id IS NULL OR chat_id < 1) AND rod_name = %s
                ''', (user_id, rod_name))
            
            result = cursor.fetchone()
            if not result:
                # Если записи нет - инициализируем удочку в этом чате
                self.init_player_rod(user_id, rod_name, chat_id=chat_id)
            
            # Уменьшаем прочность
            if type(cursor).__module__.startswith('sqlite3'):
                cursor.execute('''
                    UPDATE player_rods 
                    SET current_durability = MAX(0, current_durability - ?)
                    WHERE user_id = ? AND (chat_id IS NULL OR chat_id < 1) AND rod_name = ?
                ''', (damage, user_id, rod_name))
            else:
                cursor.execute('''
                    UPDATE player_rods 
                    SET current_durability = GREATEST(0, current_durability - %s)
                    WHERE user_id = %s AND (chat_id IS NULL OR chat_id < 1) AND rod_name = %s
                ''', (damage, user_id, rod_name))
            conn.commit()
            
            # Запускаем процесс восстановления, если еще не запущен
            self.start_rod_recovery(user_id, rod_name, chat_id)

    def repair_rod(self, user_id: int, rod_name: str, chat_id: int):
        """Полностью восстановить удочку"""
        if rod_name != BAMBOO_ROD:
            return
        with self._connect() as conn:
            cursor = conn.cursor()
            rod = self.get_rod(rod_name)
            if rod:
                cursor.execute('''
                    UPDATE player_rods 
                    SET current_durability = ?, recovery_start_time = NULL, last_repair_time = CURRENT_TIMESTAMP
                    WHERE user_id = ? AND (chat_id IS NULL OR chat_id < 1) AND rod_name = ?
                ''', (rod['max_durability'], user_id, rod_name))
                conn.commit()

    def start_rod_recovery(self, user_id: int, rod_name: str, chat_id: int):
        """Начать процесс восстановления удочки"""
        if rod_name != BAMBOO_ROD:
            return
        with self._connect() as conn:
            cursor = conn.cursor()
            if type(cursor).__module__.startswith('sqlite3'):
                cursor.execute('''
                    UPDATE player_rods 
                    SET recovery_start_time = CURRENT_TIMESTAMP
                    WHERE user_id = ? AND (chat_id IS NULL OR chat_id < 1) AND rod_name = ?
                ''', (user_id, rod_name))
            else:
                cursor.execute('''
                    UPDATE player_rods 
                    SET recovery_start_time = CURRENT_TIMESTAMP
                    WHERE user_id = %s AND (chat_id IS NULL OR chat_id < 1) AND rod_name = %s
                ''', (user_id, rod_name))
            conn.commit()

    def recover_rod_durability(self, user_id: int, rod_name: str, recovery_amount: int, chat_id: int):
        """Восстановить прочность удочки на указанное значение"""
        if rod_name != BAMBOO_ROD:
            return
        with self._connect() as conn:
            cursor = conn.cursor()
            rod = self.get_rod(rod_name)
            if rod:
                cursor.execute('''
                    UPDATE player_rods 
                    SET current_durability = LEAST(%s, current_durability + %s)
                    WHERE user_id = %s AND (chat_id IS NULL OR chat_id < 1) AND rod_name = %s
                ''', (rod['max_durability'], recovery_amount, user_id, rod_name))
                conn.commit()

    # ==================== МЕТОДЫ ДЛЯ РАБОТЫ С СЕТЯМИ ====================
    
    def get_nets(self) -> List[Dict[str, Any]]:
        """Получить список всех сетей"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM nets ORDER BY price')
            rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
    
    def get_net(self, net_name: str) -> Optional[Dict[str, Any]]:
        """Получить информацию о сети"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM nets WHERE name = ?', (net_name,))
            row = cursor.fetchone()
            if row:
                columns = [description[0] for description in cursor.description]
                return dict(zip(columns, row))
            return None
    
    def init_player_net(self, user_id: int, net_name: str, chat_id: int):
        """Инициализировать сеть для игрока в конкретном чате"""
        net = self.get_net(net_name)
        if not net:
            return
        
        with self._connect() as conn:
            cursor = conn.cursor()
            # Initialize as a GLOBAL player_net (chat_id = -1) so nets/uses are shared across chats
            cursor.execute('''
                INSERT OR IGNORE INTO player_nets (user_id, net_name, uses_left, chat_id)
                VALUES (?, ?, ?, -1)
            ''', (user_id, net_name, net['max_uses']))
            conn.commit()
    
    def get_player_net(self, user_id: int, net_name: str, chat_id: int) -> Optional[Dict[str, Any]]:
        """Получить информацию о сети игрока в конкретном чате"""
        with self._connect() as conn:
            cursor = conn.cursor()
            # Prefer a global player_net row (chat_id IS NULL or <1)
            cursor.execute('''
                SELECT pn.*, n.price, n.fish_count, n.cooldown_hours, n.max_uses, n.description
                FROM player_nets pn
                JOIN nets n ON pn.net_name = n.name
                WHERE pn.user_id = ? AND (pn.chat_id IS NULL OR pn.chat_id < 1) AND pn.net_name = ?
                LIMIT 1
            ''', (user_id, net_name))
            row = cursor.fetchone()
            if row:
                columns = [description[0] for description in cursor.description]
                return dict(zip(columns, row))
            return None
    
    def get_player_nets(self, user_id: int, chat_id: int) -> List[Dict[str, Any]]:
        """Получить все сети игрока в конкретном чате"""
        with self._connect() as conn:
            cursor = conn.cursor()
            # Prefer global entries for player nets
            cursor.execute('''
                SELECT pn.*, n.price, n.fish_count, n.cooldown_hours, n.max_uses, n.description
                FROM player_nets pn
                JOIN nets n ON pn.net_name = n.name
                WHERE pn.user_id = ? AND (pn.chat_id IS NULL OR pn.chat_id < 1)
                ORDER BY n.price
            ''', (user_id,))
            rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            nets = [dict(zip(columns, row)) for row in rows]
            if not nets:
                # Initialize global default net and re-query
                self.init_player_net(user_id, 'Базовая сеть', chat_id)
                cursor.execute('''
                    SELECT pn.*, n.price, n.fish_count, n.cooldown_hours, n.max_uses, n.description
                    FROM player_nets pn
                    JOIN nets n ON pn.net_name = n.name
                    WHERE pn.user_id = ? AND (pn.chat_id IS NULL OR pn.chat_id < 1)
                    ORDER BY n.price
                ''', (user_id,))
                rows = cursor.fetchall()
                nets = [dict(zip(columns, row)) for row in rows]
            return nets

    def grant_net(self, user_id: int, net_name: str, chat_id: int, count: int = 1) -> bool:
        """Выдать пользователю указанную сеть (глобально).
        Если запись уже есть — увеличиваем `uses_left`, иначе создаём запись.
        """
        net = self.get_net(net_name)
        if not net:
            return False

        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT uses_left FROM player_nets
                WHERE user_id = ? AND (chat_id IS NULL OR chat_id < 1) AND net_name = ?
            ''', (user_id, net_name))
            row = cursor.fetchone()
            if row:
                current = row[0]
                if current == -1:
                    # Уже бесконечная сеть
                    return True
                # Увеличиваем на count * max_uses (если max_uses == -1 — делаем -1)
                if net.get('max_uses', -1) == -1:
                    new = -1
                else:
                    new = current + int(count) * int(net.get('max_uses', 1))
                cursor.execute('''
                    UPDATE player_nets SET uses_left = ?
                    WHERE user_id = ? AND (chat_id IS NULL OR chat_id < 1) AND net_name = ?
                ''', (new, user_id, net_name))
            else:
                if net.get('max_uses', -1) == -1:
                    uses = -1
                else:
                    uses = int(count) * int(net.get('max_uses', 1))
                cursor.execute('''
                    INSERT OR REPLACE INTO player_nets (user_id, net_name, uses_left, chat_id)
                    VALUES (?, ?, ?, -1)
                ''', (user_id, net_name, uses))
            conn.commit()
            return True

    def grant_rod(self, user_id: int, rod_name: str, chat_id: int) -> bool:
        """Выдать пользователю удочку (глобально). Если уже есть — восстанавливаем до полной прочности."""
        rod = self.get_rod(rod_name)
        if not rod:
            return False

        with self._connect() as conn:
            cursor = conn.cursor()
            # Проверяем наличие глобальной записи
            cursor.execute('''
                SELECT 1 FROM player_rods
                WHERE user_id = ? AND (chat_id IS NULL OR chat_id < 1) AND rod_name = ?
            ''', (user_id, rod_name))
            if cursor.fetchone():
                cursor.execute('''
                    UPDATE player_rods
                    SET current_durability = ?, max_durability = ?
                    WHERE user_id = ? AND (chat_id IS NULL OR chat_id < 1) AND rod_name = ?
                ''', (rod.get('max_durability', rod.get('durability', 0)), rod.get('max_durability', rod.get('durability', 0)), user_id, rod_name))
            else:
                cursor.execute('''
                    INSERT OR REPLACE INTO player_rods (user_id, rod_name, current_durability, max_durability, chat_id)
                    VALUES (?, ?, ?, ?, -1)
                ''', (user_id, rod_name, rod.get('max_durability', rod.get('durability', 0)), rod.get('max_durability', rod.get('durability', 0))))
            conn.commit()
            return True
    
    def buy_net(self, user_id: int, net_name: str, chat_id: int) -> bool:
        """Купить сеть в конкретном чате"""
        net = self.get_net(net_name)
        if not net:
            return False
        
        player = self.get_player(user_id, chat_id)
        if not player or player['coins'] < net['price']:
            return False
        
        # Проверяем, есть ли уже эта сеть у игрока
        player_net = self.get_player_net(user_id, net_name, chat_id)
        
        with self._connect() as conn:
            cursor = conn.cursor()
            
            if player_net:
                # Если сеть уже есть, добавляем использования
                if net['max_uses'] == -1:
                    # Бесконечная сеть - не добавляем
                    return False
                cursor.execute('''
                    UPDATE player_nets
                    SET uses_left = uses_left + ?
                    WHERE user_id = ? AND (chat_id IS NULL OR chat_id < 1) AND net_name = ?
                ''', (net['max_uses'], user_id, net_name))
            else:
                # Создаем новую сеть
                # Insert as a GLOBAL player_net (chat_id = -1)
                cursor.execute('''
                    INSERT INTO player_nets (user_id, net_name, uses_left, chat_id)
                    VALUES (?, ?, ?, -1)
                ''', (user_id, net_name, net['max_uses']))
            
            # Списываем монеты
            cursor.execute('''
                UPDATE players
                SET coins = coins - ?
                WHERE user_id = ?
            ''', (net['price'], user_id))
            
            conn.commit()
            return True
    
    def use_net(self, user_id: int, net_name: str, chat_id: int) -> bool:
        """Использовать сеть (уменьшить количество использований) в конкретном чате"""
        player_net = self.get_player_net(user_id, net_name, chat_id)
        if not player_net:
            return False
        
        with self._connect() as conn:
            cursor = conn.cursor()
            
            # Обновляем ГЛОБАЛЬНОЕ время последнего использования ЛЮБОЙ сети
            cursor.execute('''
                UPDATE players
                SET last_net_use_time = CURRENT_TIMESTAMP
                WHERE user_id = ?
            ''', (user_id,))
            
            # Обновляем время последнего использования конкретной сети (для архива)
            cursor.execute('''
                UPDATE player_nets
                SET last_use_time = CURRENT_TIMESTAMP
                WHERE user_id = ? AND (chat_id IS NULL OR chat_id < 1) AND net_name = ?
            ''', (user_id, net_name))
            
            # Уменьшаем количество использований (только если не бесконечная)
            if player_net['max_uses'] != -1:
                cursor.execute('''
                    UPDATE player_nets
                    SET uses_left = uses_left - 1
                    WHERE user_id = ? AND (chat_id IS NULL OR chat_id < 1) AND net_name = ?
                ''', (user_id, net_name))
            
            conn.commit()
            return True
    
    def get_net_cooldown_remaining(self, user_id: int, net_name: str, chat_id: int) -> int:
        """Получить оставшееся время кулдауна для ЛЮБОЙ сети (глобальный кулдаун) в чате"""
        # Получаем информацию о сети для получения её кулдауна
        net = self.get_net(net_name)
        if not net:
            return 0
        
        # Получаем глобальное время последнего использования ЛЮБОЙ сети
        player = self.get_player(user_id, chat_id)
        if not player or not player['last_net_use_time']:
            return 0
        
        # Use timezone-aware UTC datetimes to avoid comparing naive and aware datetimes
        from datetime import datetime, timedelta, timezone
        try:
            last_use = datetime.fromisoformat(player['last_net_use_time'])
        except Exception:
            return 0

        # Treat stored naive timestamps as UTC
        if last_use.tzinfo is None:
            last_use = last_use.replace(tzinfo=timezone.utc)

        cooldown_hours = net['cooldown_hours']  # Используем кулдаун ЭТОЙ сети
        cooldown_end = last_use + timedelta(hours=cooldown_hours)

        now = datetime.now(timezone.utc)
        if now >= cooldown_end:
            return 0

        remaining = (cooldown_end - now).total_seconds()
        return int(remaining)
    
    # ===== РЕФЕРАЛЬНАЯ СИСТЕМА =====

    def add_ref_access(self, user_id: int, chat_id: int) -> bool:
        """Выдать пользователю доступ к реферальной статистике чата."""
        with self._connect() as conn:
            cursor = conn.cursor()
            if type(cursor).__module__.startswith('sqlite3'):
                cursor.execute(
                    'INSERT OR IGNORE INTO chat_configs (chat_id, admin_user_id, is_configured) VALUES (?, ?, 1)',
                    (chat_id, user_id)
                )
            else:
                cursor.execute(
                    'INSERT INTO chat_configs (chat_id, admin_user_id, is_configured) VALUES (%s, %s, 1) ON CONFLICT (chat_id) DO UPDATE SET admin_user_id = EXCLUDED.admin_user_id, is_configured = 1',
                    (chat_id, user_id)
                )
            self._upsert_ref_stars_stats(user_id, chat_id)
            conn.commit()
            return True

    def get_ref_stars_stats(self, user_id: int, chat_id: int) -> Dict[str, int]:
        """Получить агрегированную статистику по реф-звёздам для пары user/chat."""
        with self._connect() as conn:
            cursor = conn.cursor()
            if type(cursor).__module__.startswith('sqlite3'):
                cursor.execute(
                    '''
                    SELECT
                        COALESCE(stars_received, 0),
                        COALESCE(stars_spent, 0),
                        COALESCE(stars_refunded, 0),
                        COALESCE(stars_withdrawn, 0)
                    FROM ref_stars_stats
                    WHERE user_id = ? AND chat_id = ?
                    ''',
                    (user_id, chat_id)
                )
            else:
                cursor.execute(
                    '''
                    SELECT
                        COALESCE(stars_received, 0),
                        COALESCE(stars_spent, 0),
                        COALESCE(stars_refunded, 0),
                        COALESCE(stars_withdrawn, 0)
                    FROM ref_stars_stats
                    WHERE user_id = %s AND chat_id = %s
                    ''',
                    (user_id, chat_id)
                )
            row = cursor.fetchone()
            if not row:
                return {
                    'stars_received': 0,
                    'stars_spent': 0,
                    'stars_refunded': 0,
                    'stars_withdrawn': 0,
                    'stars_available': 0,
                }

            received = int(row[0] or 0)
            spent = int(row[1] or 0)
            refunded = int(row[2] or 0)
            withdrawn = int(row[3] or 0)
            available = max(0, received - spent)
            return {
                'stars_received': received,
                'stars_spent': spent,
                'stars_refunded': refunded,
                'stars_withdrawn': withdrawn,
                'stars_available': available,
            }

    def get_ref_access_chats(self, user_id: int) -> List[int]:
        """Список chat_id, где пользователь имеет доступ к реферальной статистике."""
        with self._connect() as conn:
            cursor = conn.cursor()
            if type(cursor).__module__.startswith('sqlite3'):
                cursor.execute(
                    'SELECT chat_id FROM chat_configs WHERE admin_user_id = ? AND is_configured = 1',
                    (user_id,)
                )
            else:
                cursor.execute(
                    'SELECT chat_id FROM chat_configs WHERE admin_user_id = %s AND is_configured = 1',
                    (user_id,)
                )
            rows = cursor.fetchall()
            return [int(row[0]) for row in rows] if rows else []

    def get_chat_title(self, chat_id: int) -> Optional[str]:
        """Название чата из chat_configs."""
        with self._connect() as conn:
            cursor = conn.cursor()
            if type(cursor).__module__.startswith('sqlite3'):
                cursor.execute('SELECT chat_title FROM chat_configs WHERE chat_id = ?', (chat_id,))
            else:
                cursor.execute('SELECT chat_title FROM chat_configs WHERE chat_id = %s', (chat_id,))
            row = cursor.fetchone()
            return row[0] if row and row[0] else None

    def get_chat_stars_total(self, chat_id: int) -> int:
        """Общее количество звёзд, пришедших от чата."""
        with self._connect() as conn:
            cursor = conn.cursor()
            if type(cursor).__module__.startswith('sqlite3'):
                cursor.execute('SELECT COALESCE(stars_total, 0) FROM chat_configs WHERE chat_id = ?', (chat_id,))
            else:
                cursor.execute('SELECT COALESCE(stars_total, 0) FROM chat_configs WHERE chat_id = %s', (chat_id,))
            row = cursor.fetchone()
            return int(row[0]) if row else 0

    def get_chat_refunds_total(self, chat_id: int) -> int:
        """Сумма refund/withdraw для чата по транзакциям."""
        with self._connect() as conn:
            cursor = conn.cursor()
            if type(cursor).__module__.startswith('sqlite3'):
                cursor.execute(
                    "SELECT COALESCE(SUM(total_amount), 0) FROM star_transactions WHERE chat_id = ? AND refund_status IN ('approved', 'refunded', 'ref')",
                    (chat_id,)
                )
            else:
                cursor.execute(
                    "SELECT COALESCE(SUM(total_amount), 0) FROM star_transactions WHERE chat_id = %s AND refund_status IN ('approved', 'refunded', 'ref')",
                    (chat_id,)
                )
            row = cursor.fetchone()
            return int(row[0]) if row else 0

    def get_available_stars_for_withdraw(self, user_id: int, chat_id: int) -> int:
        """Доступные к выводу звезды для чата (совместимый интерфейс с bot.py)."""
        _ = user_id
        total = self.get_chat_stars_total(chat_id)
        refunded = self.get_chat_refunds_total(chat_id)
        return max(0, total - refunded)

    def get_withdrawn_stars(self, user_id: int, chat_id: int) -> int:
        """Уже выведенные звезды для чата (совместимый интерфейс с bot.py)."""
        _ = user_id
        return self.get_chat_refunds_total(chat_id)

    def mark_stars_withdrawn(self, user_id: int, amount: int, chat_id: Optional[int] = None, status: str = 'approved') -> bool:
        """Записать факт вывода звёзд в транзакции."""
        with self._connect() as conn:
            cursor = conn.cursor()
            title = self.get_chat_title(chat_id) if chat_id is not None else None
            if type(cursor).__module__.startswith('sqlite3'):
                cursor.execute(
                    "INSERT INTO star_transactions (user_id, telegram_payment_charge_id, total_amount, chat_id, chat_title, refund_status) VALUES (?, ?, ?, ?, ?, ?)",
                    (user_id, f"withdraw_{user_id}_{int(datetime.now().timestamp())}", int(amount), chat_id, title, status)
                )
            else:
                cursor.execute(
                    "INSERT INTO star_transactions (user_id, telegram_payment_charge_id, total_amount, chat_id, chat_title, refund_status) VALUES (%s, %s, %s, %s, %s, %s)",
                    (user_id, f"withdraw_{user_id}_{int(datetime.now().timestamp())}", int(amount), chat_id, title, status)
                )

            if chat_id is not None:
                self._upsert_ref_stars_stats(
                    int(user_id),
                    int(chat_id),
                    spent=int(amount),
                    withdrawn=int(amount)
                )

            conn.commit()
            return True
    
    def set_player_ref(self, user_id: int, chat_id: int, ref_user_id: int) -> bool:
        """Установить реферера для пользователя"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE players
                SET ref = ?
                WHERE user_id = ?
            ''', (ref_user_id, user_id))
            conn.commit()
            return True
    
    def get_player_ref(self, user_id: int, chat_id: int) -> Optional[int]:
        """Получить реферера пользователя"""
        player = self.get_player(user_id, chat_id)
        if player:
            return player.get('ref')
        return None
    
    def set_ref_link(self, user_id: int, chat_id: int, ref_link: str) -> bool:
        """Сохранить реф ссылку для пользователя"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE players
                SET ref_link = ?
                WHERE user_id = ?
            ''', (ref_link, user_id))
            conn.commit()
            return True
    
    def get_ref_link(self, user_id: int, chat_id: int) -> Optional[str]:
        """Получить реф ссылку пользователя"""
        player = self.get_player(user_id, chat_id)
        if player:
            return player.get('ref_link')
        return None
    
    def configure_chat(self, chat_id: int, admin_user_id: int) -> bool:
        """Настроить чат для реферальной системы"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO chat_configs (chat_id, admin_user_id, is_configured)
                VALUES (?, ?, 1)
            ''', (chat_id, admin_user_id))
            conn.commit()
            return True
    
    def is_chat_configured(self, chat_id: int) -> bool:
        """Проверить, настроен ли чат"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT 1 FROM chat_configs 
                WHERE chat_id = ? AND is_configured = 1
            ''', (chat_id,))
            return cursor.fetchone() is not None
    
    def get_chat_admin(self, chat_id: int) -> Optional[int]:
        """Получить админа, настроившего чат"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT admin_user_id FROM chat_configs 
                WHERE chat_id = ? AND is_configured = 1
            ''', (chat_id,))
            row = cursor.fetchone()
            return row[0] if row else None
    
    def set_user_ref_link(self, user_id: int, ref_link: str) -> bool:
        """Сохранить реф-ссылку пользователя (Telegram Affiliate)"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO user_ref_links (user_id, ref_link, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            ''', (user_id, ref_link))
            conn.commit()
            return True
    
    def get_user_ref_link(self, user_id: int) -> Optional[str]:
        """Получить сохранённую реф-ссылку пользователя"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT ref_link FROM user_ref_links WHERE user_id = ?
            ''', (user_id,))
            row = cursor.fetchone()
            return row[0] if row else None
    
    def set_user_chat_link(self, user_id: int, chat_invite_link: str) -> bool:
        """Сохранить ссылку на чат пользователя"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE user_ref_links
                SET chat_invite_link = ?, updated_at = CURRENT_TIMESTAMP
                WHERE user_id = ?
            ''', (chat_invite_link, user_id))
            conn.commit()
            return cursor.rowcount > 0
    
    def get_user_chat_link(self, user_id: int) -> Optional[str]:
        """Получить сохранённую ссылку на чат пользователя"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT chat_invite_link FROM user_ref_links WHERE user_id = ?
            ''', (user_id,))
            row = cursor.fetchone()
            return row[0] if row else None
    
    def set_chat_ref_link(self, chat_id: int, ref_link: str, chat_invite_link: str = None) -> bool:
        """Установить реф-ссылку администратора для чата"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE chat_configs 
                SET admin_ref_link = ?, chat_invite_link = ?
                WHERE chat_id = ?
            ''', (ref_link, chat_invite_link, chat_id))
            conn.commit()
            return cursor.rowcount > 0
    
    def get_chat_ref_link(self, chat_id: int) -> Optional[str]:
        """Получить реф-ссылку для чата"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT admin_ref_link FROM chat_configs 
                WHERE chat_id = ? AND is_configured = 1
            ''', (chat_id,))
            row = cursor.fetchone()
            return row[0] if row else None
    
    def get_user_registered_chats(self, user_id: int) -> List[Dict[str, Any]]:
        """Получить все чаты, зарегистрированные этим юзером"""
        with self._connect() as conn:
            cursor = conn.cursor()
            if type(cursor).__module__.startswith('sqlite3'):
                cursor.execute('''
                    SELECT chat_id, admin_ref_link, chat_invite_link
                    FROM chat_configs
                    WHERE admin_user_id = ? AND is_configured = 1
                ''', (user_id,))
            else:
                cursor.execute('''
                    SELECT chat_id, admin_ref_link, chat_invite_link
                    FROM chat_configs
                    WHERE admin_user_id = %s AND is_configured = 1
                ''', (user_id,))
            rows = cursor.fetchall()
            cols = [d[0] for d in cursor.description] if cursor.description else []
            return [dict(zip(cols, row)) for row in rows]


# Экземпляр базы данных для импорта в других модулях
db = Database()