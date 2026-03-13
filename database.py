import os
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
        # Convert empty double-quoted string literals ("") to PostgreSQL single-quoted ('').
        # SQLite allows "" as an empty string; Postgres treats "" as an invalid zero-length identifier.
        s = s.replace('""', "''")
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
                    logger.debug("Postgres executing SQL: %s PARAMS: %s", out_sql, params)
                    cur.execute(out_sql, params)
                except Exception:
                    logger.exception("DB execute failed. SQL: %s PARAMS: %s", out_sql, params)
                    raise
            else:
                try:
                    logger.debug("Postgres executing SQL: %s (no params)", out_sql)
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
    try:
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
    "Удачливая удочка": (140, 160),
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
    "Мифическая": 50,
}

RARITY_XP_MULTIPLIERS = {
    "Обычная": 1.0,
    "Редкая": 1.1,
    "Легендарная": 1.2,
    "Мифическая": 1.15,
}

class Database:
    def __init__(self):
        self._cached_conn: Optional['PostgresConnWrapper'] = None
        self.init_db()

    def _connect(self):
        # Postgres-only: require DATABASE_URL in environment
        db_url = os.getenv('DATABASE_URL')
        if not db_url:
            raise RuntimeError('DATABASE_URL must be set to use Postgres')
        # Reuse the cached connection instead of creating a new one on every call
        if self._cached_conn is not None:
            try:
                cur = self._cached_conn._conn.cursor()
                cur.execute('SELECT 1')
                cur.close()
                return self._cached_conn
            except Exception:
                try:
                    self._cached_conn._conn.close()
                except Exception:
                    pass
                self._cached_conn = None
        self._cached_conn = PostgresConnWrapper(db_url)
        return self._cached_conn

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
                    diamonds INTEGER DEFAULT 0,
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
                    chat_id BIGINT,
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
            # Ensure `chat_id` column exists (some deployments may have been created without it)
            try:
                cursor.execute("ALTER TABLE caught_fish ADD COLUMN IF NOT EXISTS chat_id BIGINT")
            except Exception:
                try:
                    cursor.execute("ALTER TABLE caught_fish ADD COLUMN chat_id BIGINT")
                except Exception:
                    pass

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

            # Таблица системных флагов/миграций
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS system_flags (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            ''')

            # Таблица сокровищ игроков
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS player_treasures (
                    id INTEGER PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    chat_id BIGINT DEFAULT -1,
                    treasure_name TEXT NOT NULL,
                    quantity INTEGER DEFAULT 1,
                    obtained_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, chat_id, treasure_name)
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
        
        # Заполняем начальными данными
        self._fill_default_data()
    
    def _run_migrations(self):
        """Выполнение миграций для обновления схемы БД"""
        with self._connect() as conn:
            cursor = conn.cursor()

            def get_columns(table_name: str):
                cursor.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = %s AND table_schema = 'public'",
                    (table_name,)
                )
                return [r[0] for r in cursor.fetchall()]

            # Проверяем наличие колонок в таблице players (Postgres-friendly)
            columns = get_columns('players')

            if 'ref' not in columns:
                cursor.execute('ALTER TABLE players ADD COLUMN ref INTEGER')
                conn.commit()

            if 'ref_link' not in columns:
                cursor.execute('ALTER TABLE players ADD COLUMN ref_link TEXT')
                conn.commit()

            if 'chat_id' not in columns:
                cursor.execute('ALTER TABLE players ADD COLUMN chat_id BIGINT')
                conn.commit()

            # Ensure chat_configs has columns for tracking title and total stars
            chat_conf_cols = get_columns('chat_configs')
            if 'stars_total' not in chat_conf_cols:
                try:
                    cursor.execute('ALTER TABLE chat_configs ADD COLUMN stars_total INTEGER DEFAULT 0')
                    conn.commit()
                except Exception:
                    pass
            if 'chat_title' not in chat_conf_cols:
                try:
                    cursor.execute('ALTER TABLE chat_configs ADD COLUMN chat_title TEXT')
                    conn.commit()
                except Exception:
                    pass

            if 'xp' not in columns:
                cursor.execute('ALTER TABLE players ADD COLUMN xp INTEGER DEFAULT 0')
                conn.commit()

            if 'level' not in columns:
                cursor.execute('ALTER TABLE players ADD COLUMN level INTEGER DEFAULT 0')
                conn.commit()

            # CRITICAL: Migrate players table to use composite primary key (user_id, chat_id)
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
            ensure_column('players', 'consecutive_casts_at_location', 'INTEGER DEFAULT 0')
            ensure_column('players', 'last_fishing_location', 'TEXT')
            ensure_column('players', 'population_penalty', 'REAL DEFAULT 0.0')

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
                            # Direct cast INTEGER -> BIGINT is always safe and lossless.
                            # Avoids the old '^[0-9]+$' regex which incorrectly converted
                            # negative Telegram group chat IDs (e.g. -1001234567890) to NULL.
                            cursor.execute(f'ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE BIGINT USING {column_name}::bigint')
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

            # Force caught_fish.chat_id to BIGINT unconditionally.
            # Telegram supergroup IDs like -1001234567890 exceed 32-bit INTEGER range.
            # ALTER TABLE ... TYPE BIGINT is a no-op if column is already BIGINT.
            for _tbl, _col in [
                ('caught_fish', 'chat_id'),
                ('players', 'chat_id'),
                ('players', 'user_id'),
                ('player_rods', 'chat_id'),
                ('player_rods', 'user_id'),
                ('player_nets', 'chat_id'),
                ('player_nets', 'user_id'),
                ('star_transactions', 'chat_id'),
                ('star_transactions', 'user_id'),
            ]:
                try:
                    cursor.execute(
                        f'ALTER TABLE {_tbl} ALTER COLUMN {_col} TYPE BIGINT USING {_col}::bigint'
                    )
                    conn.commit()
                    logger.info("Ensured %s.%s is BIGINT", _tbl, _col)
                except Exception as _e:
                    logger.warning("ALTER %s.%s BIGINT skipped: %s", _tbl, _col, _e)
                    try:
                        conn.rollback()
                    except Exception:
                        pass

            # Populate chat_id in player_rods and player_nets and caught_fish
            # Use p.chat_id directly — the old regex '^[0-9]+$' incorrectly excluded
            # negative Telegram group chat IDs, setting them to NULL.
            cursor.execute('''
                UPDATE player_rods
                SET chat_id = (
                    SELECT p.chat_id
                    FROM players p
                    WHERE p.user_id = player_rods.user_id AND p.chat_id IS NOT NULL AND p.chat_id != 0
                    ORDER BY p.chat_id
                    LIMIT 1
                )
                WHERE chat_id IS NULL OR chat_id = 0
            ''')
            conn.commit()

            cursor.execute('''
                UPDATE player_nets
                SET chat_id = (
                    SELECT p.chat_id
                    FROM players p
                    WHERE p.user_id = player_nets.user_id AND p.chat_id IS NOT NULL AND p.chat_id != 0
                    ORDER BY p.chat_id
                    LIMIT 1
                )
                WHERE chat_id IS NULL OR chat_id = 0
            ''')
            conn.commit()

            cursor.execute('''
                UPDATE caught_fish
                SET chat_id = (
                    SELECT p.chat_id
                    FROM players p
                    WHERE p.user_id = caught_fish.user_id AND p.chat_id IS NOT NULL AND p.chat_id != 0
                    ORDER BY p.chat_id
                    LIMIT 1
                )
                WHERE chat_id IS NULL OR chat_id = 0
            ''')
            conn.commit()

            # Инициализация погоды для локаций
            cursor.execute('SELECT name FROM locations')
            locations = cursor.fetchall()

            from weather import weather_system
            for location in locations:
                loc_name = location[0]
                cursor.execute('SELECT 1 FROM weather WHERE location = %s', (loc_name,))
                if not cursor.fetchone():
                    condition, temp = weather_system.generate_weather(loc_name)
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
            
            # ===== МИГРАЦИЯ: переименование рыб (убраны подписи в скобках) =====
            # Рыбы, пойманные до переименования, теряли стоимость — исправляем имена в caught_fish.
            _fish_renames = [
                ("Бестер (гибрид)", "Бестер"),
                ("Бестер (Гибрид) (Крупный)", "Бестер"),
                ("Ишхан (Форель)", "Ишхан"),
                ("Валаамка (Сиг)", "Валаамка"),
                ("Белуга (Монстр)", "Белуга"),
                ("Сом (Гигант)", "Сом"),
                ("Калуга (Гигант)", "Калуга"),
                ("Лещ (Крупный)", "Лещ"),
                ("Судак (Хищник)", "Судак"),
                ("Налим (Ночной)", "Налим"),
                ("Нельма (Крупная)", "Нельма"),
                ("Веслонос (Редкая)", "Веслонос"),
                ("Плотва (Частая)", "Плотва"),
                ("Уклейка (Мелочь)", "Уклейка"),
                ("Ёрш (Сорная)", "Ёрш"),
                ("Ряпушка (Мелочь)", "Ряпушка"),
                ("Колюшка (Крошечная)", "Колюшка"),
                ("Тигровая акула (Монстр)", "Тигровая акула"),
                ("Акула-молот (Гигант)", "Акула-молот"),
                ("Парусник (Быстрая)", "Парусник"),
                ("Палтус синекорый (Дно)", "Палтус синекорый"),
                ("Конгер (Морской угорь)", "Конгер"),
                ("Лаврак (Сибас)", "Лаврак"),
                ("Зубан (Дентекс)", "Зубан"),
                ("Серриола (Амберджек)", "Серриола"),
                ("Пеламида (Бонито)", "Пеламида"),
                ("Пилорыл (Редкая)", "Пилорыл"),
                ("Рыба-луна (Экзотика)", "Рыба-луна"),
                ("Сагрина (Зеленушка)", "Сагрина"),
                ("Скорпена (Ёрш)", "Скорпена"),
                ("Сариола (Желтохвост)", "Сариола"),
                ("Анчоус (Мелочь)", "Анчоус"),
                ("Шпрот (Мелочь)", "Шпрот"),
                ("Луна-рыба (Опах)", "Луна-рыба"),
                ("Морской петух (Монстр)", "Морской петух"),
            ]
            for old_name, new_name in _fish_renames:
                try:
                    cursor.execute(
                        "UPDATE caught_fish SET fish_name = %s WHERE fish_name = %s",
                        (new_name, old_name)
                    )
                except Exception:
                    try:
                        cursor.execute(
                            "UPDATE caught_fish SET fish_name = ? WHERE fish_name = ?",
                            (new_name, old_name)
                        )
                    except Exception:
                        pass

            # Migrate echosounder to be per-user (chat_id=0) instead of per-chat
            try:
                cursor.execute(
                    '''
                    INSERT INTO player_echosounder (user_id, chat_id, expires_at)
                    SELECT user_id, 0, MAX(expires_at)
                    FROM player_echosounder
                    WHERE chat_id != 0
                    GROUP BY user_id
                    ON CONFLICT (user_id, chat_id) DO UPDATE
                        SET expires_at = EXCLUDED.expires_at
                    '''
                )
                cursor.execute("DELETE FROM player_echosounder WHERE chat_id != 0")
            except Exception:
                pass

            # Ensure tournaments table exists and has all required columns
            try:
                cursor.execute(
                    '''CREATE TABLE IF NOT EXISTS tournaments (
                        id SERIAL PRIMARY KEY,
                        chat_id BIGINT,
                        created_by BIGINT,
                        title TEXT NOT NULL,
                        tournament_type TEXT DEFAULT 'total_weight',
                        starts_at TIMESTAMP NOT NULL,
                        ends_at TIMESTAMP NOT NULL,
                        target_fish TEXT,
                        prize_pool INTEGER DEFAULT 50,
                        target_location TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )'''
                )
            except Exception:
                pass
            try:
                cursor.execute("ALTER TABLE tournaments ADD COLUMN IF NOT EXISTS prize_pool INTEGER DEFAULT 50")
            except Exception:
                pass
            try:
                cursor.execute("ALTER TABLE tournaments ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            except Exception:
                pass
            try:
                cursor.execute("ALTER TABLE tournaments ADD COLUMN IF NOT EXISTS target_location TEXT")
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
                ("Бамбуковая удочка", 0, 100, 100, 0, 30),            # стартовая удочка, макс вес 30 кг
                ("Углепластиковая удочка", 1500, 150, 150, 5, 60),    # макс вес 60 кг
                ("Карбоновая удочка", 4500, 200, 200, 10, 120),        # макс вес 120 кг
                ("Золотая удочка", 15000, 300, 300, 20, 350),          # макс вес 350 кг
                ("Удачливая удочка", 25000, 150, 150, 15, 650),        # макс вес 650 кг, ломка 140-160 уловов
            ]
            
            cursor.executemany('''
                INSERT OR IGNORE INTO rods (name, price, durability, max_durability, fish_bonus, max_weight)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', rods_data)

            # Принудительное обновление max_weight для уже существующих удочек
            rods_weight_updates = [
                (30, "Бамбуковая удочка"),
                (60, "Углепластиковая удочка"),
                (120, "Карбоновая удочка"),
                (350, "Золотая удочка"),
                (650, "Удачливая удочка"),
            ]
            for max_w, rod_name in rods_weight_updates:
                cursor.execute('UPDATE rods SET max_weight = ? WHERE name = ?', (max_w, rod_name))
            
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
                
                # ===== ПРУД (новые виды) =====
                ("Канальный сомик", "Редкая", 0.3, 2.5, 25, 55, 45, "Городской пруд", "Весна,Лето,Осень", "Кусочки рыбы,Выползок,Черви,Печень,Пучок червей", 8, None),
                ("Амурский чебачок", "Обычная", 0.01, 0.05, 5, 10, 3, "Городской пруд", "Весна,Лето,Осень", "Мотыль,Опарыш,Тесто,Хлеб", 3, None),
                ("Солнечный окунь", "Обычная", 0.05, 0.3, 8, 18, 8, "Городской пруд", "Весна,Лето,Осень", "Черви,Муха,Мотыль,Опарыш", 4, None),
                ("Шиповка", "Обычная", 0.01, 0.03, 4, 8, 4, "Городской пруд", "Все", "Мотыль,Мормыш,Черви", 3, None),
                ("Бестер", "Легендарная", 1.0, 8.0, 40, 100, 200, "Городской пруд", "Все", "Сельдь,Выползок,Кусочки рыбы,Кусок мяса,Живец", 20, None),
                ("Колюшка", "Обычная", 0.005, 0.02, 3, 7, 2, "Городской пруд", "Все", "Мотыль,Икра,Опарыш", 3, None),
                ("Веслонос", "Легендарная", 2.0, 10.0, 50, 120, 350, "Городской пруд", "Весна,Лето,Осень", "Каша,Опарыш,Мотыль,Тесто", 30, None),
                ("Бычок-песочник", "Обычная", 0.01, 0.08, 5, 12, 5, "Городской пруд", "Все", "Черви,Кусочки рыбы,Мотыль,Опарыш", 4, None),
                ("Гольян", "Обычная", 0.01, 0.04, 4, 9, 3, "Городской пруд", "Весна,Лето,Осень", "Муха,Мотыль,Хлеб,Манка,Опарыш", 3, None),
                ("Аллигаторовый панцирник", "Мифическая", 10.0, 80.0, 80, 250, 850, "Городской пруд", "Лето", "Крупный живец,Лягушонок,Кусок мяса,Блесна,Воблер", 60, None),

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

                # ===== РЕКА (новые виды) =====
                ("Щука", "Редкая", 1.0, 10.0, 40, 120, 60, "Река", "Все", "Блесна,Воблер,Живец,Лягушонок,Узкая блесна,Кусочки рыбы", 18, None),
                ("Линь", "Редкая", 0.2, 2.0, 15, 45, 30, "Река", "Весна,Лето", "Выползок,Черви,Опарыш,Тесто,Мотыль,Кукуруза,Горох", 8, None),
                ("Усач", "Редкая", 0.5, 3.0, 25, 60, 55, "Река", "Весна,Лето,Осень", "Личинка короеда,Моллюск,Выползок,Каша,Черви", 12, None),
                ("Чехонь", "Обычная", 0.1, 0.7, 18, 40, 15, "Река", "Весна,Лето,Осень", "Мушка,Опарыш,Мотыль,Маленькая блесна,Муха", 6, None),
                ("Берш", "Редкая", 0.2, 1.2, 20, 45, 40, "Река", "Все", "Кусочки рыбы,Маленькая блесна,Черви,Живец,Блесна", 8, None),
                ("Пескарь", "Обычная", 0.01, 0.05, 5, 12, 5, "Река", "Все", "Мотыль,Черви,Опарыш,Манка", 3, None),
                ("Густера", "Обычная", 0.05, 0.4, 10, 30, 10, "Река", "Все", "Каша,Горох,Мотыль,Опарыш,Черви", 5, None),
                ("Елец", "Обычная", 0.01, 0.08, 7, 16, 6, "Река", "Все", "Мушка,Личинка короеда,Мотыль,Хлеб,Опарыш", 4, None),
                ("Рыбец", "Редкая", 0.3, 2.0, 20, 45, 45, "Река", "Все", "Моллюск,Опарыш,Мотыль,Личинка короеда,Черви", 10, None),
                ("Подуст", "Обычная", 0.2, 1.0, 15, 35, 12, "Река", "Весна,Лето,Осень", "Каша,Мотыль,Опарыш,Черви,Тесто", 6, None),
                ("Синец", "Обычная", 0.1, 0.7, 15, 35, 10, "Река", "Все", "Мотыль,Опарыш,Каша,Тесто,Черви", 5, None),
                ("Белоглазка", "Обычная", 0.05, 0.3, 10, 25, 8, "Река", "Все", "Каша,Опарыш,Черви,Кукуруза,Тесто", 5, None),
                ("Угорь", "Редкая", 0.2, 2.0, 30, 100, 50, "Река", "Лето,Осень", "Выползок,Кусочки рыбы,Живец,Лягушонок,Кусок мяса", 12, None),
                ("Красноперка", "Обычная", 0.05, 0.5, 10, 25, 10, "Река", "Весна,Лето,Осень", "Кукуруза,Мушка,Хлеб,Тесто,Опарыш", 5, None),
                ("Форель ручьевая", "Редкая", 0.2, 1.5, 20, 50, 55, "Река", "Все", "Мушка,Маленькая блесна,Кузнечик,Мотыль,Опарыш", 10, None),
                ("Ленок", "Редкая", 0.5, 3.0, 30, 70, 90, "Река", "Весна,Лето,Осень", "Мышь,Блесна,Мушка,Личинка короеда,Воблер", 14, None),
                ("Нельма", "Легендарная", 2.0, 15.0, 50, 130, 300, "Река", "Все", "Крупный живец,Блесна,Воблер,Кусочки рыбы,Живец", 35, None),
                ("Муксун", "Редкая", 0.5, 3.0, 30, 70, 60, "Река", "Весна,Осень,Зима", "Мушка,Мотыль,Моллюск,Мормыш,Икра", 12, None),
                ("Чир", "Редкая", 0.5, 2.5, 25, 60, 55, "Река", "Весна,Осень,Зима", "Моллюск,Личинка короеда,Мотыль,Мормыш", 12, None),
                ("Сиг", "Редкая", 0.3, 2.0, 20, 55, 45, "Река", "Весна,Осень,Зима", "Мушка,Икра,Маленькая блесна,Мотыль,Мормыш", 10, None),
                ("Осетр русский", "Легендарная", 5.0, 40.0, 70, 200, 400, "Река", "Все", "Моллюск,Пучок червей,Выползок,Кусочки рыбы,Кусок мяса", 50, None),
                ("Севрюга", "Легендарная", 3.0, 25.0, 60, 180, 220, "Река,Море", "Весна,Осень", "Кусочки рыбы,Моллюск,Выползок,Кусок мяса", 40, None),
                ("Шип", "Легендарная", 2.0, 12.0, 40, 120, 180, "Река,Море", "Весна,Осень", "Пучок червей,Моллюск,Личинка короеда,Черви", 30, None),
                ("Бычок-кругляк", "Обычная", 0.01, 0.08, 5, 12, 5, "Река", "Все", "Черви,Кусочки рыбы,Мотыль,Опарыш", 4, None),
                ("Верхогляд", "Редкая", 0.5, 3.0, 30, 70, 120, "Река", "Весна,Лето,Осень", "Живец,Воблер,Блесна,Кусочки рыбы", 18, None),
                ("Ауха", "Редкая", 0.5, 2.0, 25, 60, 110, "Река", "Лето,Осень", "Живец,Блесна,Воблер,Кусочки рыбы", 16, None),
                ("Калуга", "Мифическая", 20.0, 200.0, 100, 400, 800, "Река", "Весна,Лето,Осень", "Крупный живец,Кусочки рыбы,Печень,Кусок мяса,Сельдь", 70, None),
                ("Шемая", "Редкая", 0.1, 1.0, 15, 35, 50, "Река", "Весна,Лето,Осень", "Мушка,Опарыш,Мотыль,Черви", 8, None),
                ("Вырезуб", "Редкая", 0.5, 3.0, 25, 60, 55, "Река", "Весна,Лето,Осень", "Моллюск,Выползок,Кукуруза,Черви", 12, None),
                ("Минога", "Редкая", 0.01, 0.2, 10, 40, 30, "Река", "Все", "Мотыль,Черви,Личинка,Опарыш", 5, None),
                ("Голец арктический", "Редкая", 0.5, 3.0, 25, 60, 100, "Река", "Весна,Осень,Зима", "Икра,Блесна,Мормыш,Мушка", 14, None),
                ("Байкальский омуль", "Редкая", 0.3, 1.5, 20, 50, 70, "Река", "Весна,Осень,Зима", "Мормыш,Муха,Икра,Мотыль", 12, None),
                ("Ряпушка", "Обычная", 0.01, 0.05, 5, 12, 5, "Река", "Все", "Мотыль,Опарыш,Муха,Черви", 3, None),
                ("Корюшка", "Редкая", 0.01, 0.1, 7, 15, 15, "Река", "Весна,Зима", "Кусочки рыбы,Мотыль,Опарыш", 5, None),
                ("Сибирский осетр", "Легендарная", 5.0, 50.0, 70, 200, 420, "Река", "Все", "Моллюск,Выползок,Кусочки рыбы,Пучок червей,Живец", 50, None),
                ("Кумжа", "Редкая", 1.0, 7.0, 30, 80, 180, "Река", "Весна,Лето,Осень,Зима", "Воблер,Блесна,Муха,Мушка,Живец", 18, None),
                ("Палия", "Редкая", 1.0, 8.0, 40, 90, 180, "Река", "Весна,Осень,Зима", "Блесна,Живец,Икра,Мотыль", 18, None),
                ("Подкаменщик", "Обычная", 0.05, 0.3, 8, 20, 6, "Река", "Все", "Мотыль,Черви,Опарыш,Кусочки рыбы", 4, None),
                ("Чебак", "Обычная", 0.05, 0.3, 10, 22, 7, "Река", "Все", "Опарыш,Тесто,Хлеб,Мотыль,Манка", 4, None),
                ("Голубой сом", "Легендарная", 10.0, 80.0, 80, 200, 350, "Река", "Лето,Осень", "Крупный живец,Выползок,Печень,Кусок мяса,Сельдь", 50, None),
                ("Мальма", "Редкая", 0.5, 3.0, 25, 65, 60, "Река", "Весна,Осень,Зима", "Блесна,Мушка,Живец,Икра,Мотыль", 12, None),
                ("Ишхан", "Легендарная", 1.0, 8.0, 35, 80, 280, "Река", "Весна,Лето,Осень,Зима", "Блесна,Мушка,Живец,Икра,Воблер", 25, None),
                ("Зеркальный карп", "Редкая", 2.0, 15.0, 40, 90, 80, "Река", "Весна,Лето,Осень", "Кукуруза,Горох,Каша,Тесто,Бойлы,Картофель", 18, None),
                ("Пестрый толстолобик", "Редкая", 3.0, 15.0, 50, 110, 90, "Река", "Лето", "Технопланктон,Каша,Хлеб,Тесто,Зелень", 25, None),
                ("Валаамка", "Редкая", 0.5, 3.0, 25, 60, 130, "Озеро", "Осень,Зима", "Мормыш,Мотыль,Маленькая блесна,Икра", 18, None),

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
                ("Форель озерная", "Легендарная", 1.5, 6.0, 40, 70, 200, "Озеро", "Весна,Осень", "Воблер,Блесна,Живец,Икра,Кузнечик,Мушка,Опарыш,Черви,Маленькая блесна", 16, None),
                ("Угорь", "Легендарная", 1.0, 5.0, 50, 80, 180, "Озеро", "Лето", "Выползок,Живец,Кусочки рыбы,Пучок червей,Лягушонок,Кусок мяса", 18, None),
                ("Осетр", "Легендарная", 3.0, 25.0, 70, 140, 260, "Озеро", "Лето,Осень", "Сельдь,Кусочки рыбы,Моллюск,Выползок,Крупный живец,Живец,Икра", 35, None),

                # ===== ОЗЕРО (новые виды) =====
                ("Белуга", "Мифическая", 100.0, 500.0, 150, 450, 1100, "Озеро", "Весна,Осень", "Сельдь,Кусочки рыбы,Живец,Крупный живец", 80, None),
                ("Сом", "Легендарная", 20.0, 150.0, 100, 350, 500, "Озеро,Река", "Лето,Осень", "Выползок,Живец,Кусочки рыбы,Сельдь,Кусок мяса", 60, None),
                ("Калуга", "Мифическая", 50.0, 500.0, 150, 450, 1000, "Озеро,Река", "Весна,Лето,Осень", "Живец,Кусочки рыбы,Выползок,Сельдь", 80, None),
                ("Лещ", "Редкая", 2.0, 7.0, 40, 70, 80, "Озеро", "Все", "Каша,Горох,Кукуруза,Мотыль,Пучок червей,Опарыш", 18, None),
                ("Судак", "Редкая", 2.0, 12.0, 40, 100, 90, "Озеро", "Все", "Воблер,Блесна,Живец,Узкая блесна,Кусочки рыбы", 20, None),
                ("Налим", "Редкая", 1.0, 8.0, 40, 100, 85, "Озеро", "Весна,Осень,Зима", "Кусочки рыбы,Живец,Выползок,Пучок червей", 18, None),
                ("Радужная форель", "Редкая", 0.5, 5.0, 30, 70, 80, "Озеро", "Все", "Икра,Муха,Воблер,Блесна,Живец,Мушка", 14, None),
                ("Плотва", "Обычная", 0.05, 0.5, 10, 25, 8, "Озеро", "Все", "Тесто,Хлеб,Опарыш,Мотыль,Манка", 4, None),
                ("Карп зеркальный", "Редкая", 2.0, 15.0, 40, 90, 85, "Озеро", "Весна,Лето,Осень", "Кукуруза,Горох,Каша,Тесто,Бойлы", 20, None),
                ("Язь", "Редкая", 0.5, 3.0, 25, 60, 45, "Озеро", "Весна,Лето,Осень", "Горох,Кукуруза,Муха,Хлеб,Черви,Опарыш", 10, None),
                ("Голавль", "Редкая", 0.5, 2.5, 20, 50, 40, "Озеро", "Весна,Лето,Осень", "Муха,Хлеб,Кукуруза,Блесна,Кузнечик", 10, None),
                ("Уклейка", "Обычная", 0.01, 0.03, 5, 10, 3, "Озеро", "Весна,Лето,Осень", "Муха,Опарыш,Хлеб,Манка", 3, None),
                ("Ёрш", "Обычная", 0.01, 0.05, 5, 12, 3, "Озеро", "Все", "Мотыль,Черви,Мормыш,Опарыш", 3, None),
                ("Толстолобик пестрый", "Редкая", 3.0, 15.0, 50, 120, 180, "Озеро,Река", "Лето,Осень", "Огурец,Камыш,Каша,Тесто,Хлеб", 30, None),
                ("Арктический голец", "Редкая", 0.5, 3.0, 25, 60, 120, "Озеро", "Весна,Осень,Зима", "Икра,Блесна,Мормыш,Мотыль", 18, None),
                ("Омуль", "Редкая", 0.3, 2.0, 20, 55, 60, "Озеро", "Все", "Мормыш,Муха,Икра,Мотыль,Опарыш", 12, None),
                ("Нельма", "Легендарная", 5.0, 20.0, 60, 130, 320, "Озеро", "Весна,Осень,Зима", "Блесна,Воблер,Живец,Кусочки рыбы", 40, None),
                ("Веслонос", "Легендарная", 2.0, 10.0, 50, 120, 320, "Озеро", "Весна,Лето,Осень", "Каша,Тесто,Мотыль,Опарыш", 30, None),
                ("Кумжа", "Редкая", 1.0, 7.0, 30, 80, 170, "Озеро", "Весна,Лето,Осень,Зима", "Воблер,Блесна,Муха,Мушка,Живец", 18, None),
                ("Палия", "Редкая", 1.0, 8.0, 40, 90, 170, "Озеро", "Весна,Осень,Зима", "Блесна,Живец,Икра,Мотыль", 18, None),
                ("Ряпушка", "Обычная", 0.01, 0.05, 5, 12, 4, "Озеро", "Все", "Мотыль,Опарыш,Муха,Черви", 3, None),
                ("Корюшка", "Редкая", 0.01, 0.1, 7, 15, 15, "Озеро", "Весна,Зима", "Кусочки рыбы,Мотыль,Опарыш", 5, None),
                ("Берш", "Редкая", 0.2, 1.2, 20, 45, 40, "Озеро", "Все", "Живец,Блесна,Черви,Кусочки рыбы", 8, None),
                ("Белоглазка", "Обычная", 0.05, 0.3, 10, 25, 7, "Озеро", "Все", "Каша,Опарыш,Черви,Тесто", 4, None),
                ("Хариус", "Редкая", 0.2, 1.5, 20, 50, 45, "Озеро", "Весна,Лето,Осень,Зима", "Муха,Блесна,Икра,Мотыль,Мушка", 10, None),
                ("Колюшка", "Обычная", 0.005, 0.02, 3, 7, 2, "Озеро", "Все", "Мотыль,Икра,Опарыш", 3, None),
                ("Американский сомик", "Редкая", 0.3, 2.5, 25, 55, 45, "Озеро", "Весна,Лето,Осень", "Выползок,Кусочки рыбы,Хлеб,Черви", 8, None),
                ("Озерный гольян", "Обычная", 0.01, 0.04, 4, 9, 3, "Озеро", "Весна,Лето,Осень", "Мотыль,Муха,Хлеб,Манка", 3, None),
                ("Бестер", "Легендарная", 3.0, 15.0, 50, 130, 260, "Озеро", "Все", "Сельдь,Выползок,Кусочки рыбы,Живец,Кусок мяса", 35, None),

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

                # ===== МОРЕ (новые виды) =====
                ("Тигровая акула", "Мифическая", 100.0, 700.0, 200, 550, 1500, "Море", "Лето,Осень,Зима", "Туша рыбы,Крупный кусок мяса,Спрут,Кальмар", 80, None),
                ("Акула-молот", "Мифическая", 80.0, 400.0, 150, 600, 1200, "Море", "Весна,Лето,Осень", "Крупный живец,Кальмар,Туша рыбы,Спрут", 80, None),
                ("Акула Мако", "Легендарная", 50.0, 300.0, 150, 400, 750, "Море", "Весна,Лето,Осень,Зима", "Сардина,Пилькер,Крупный живец,Кальмар", 65, None),
                ("Лисья акула", "Легендарная", 40.0, 250.0, 150, 500, 650, "Море", "Весна,Лето,Осень,Зима", "Живец,Кальмар,Блесна,Пилькер", 60, None),
                ("Парусник", "Легендарная", 15.0, 100.0, 100, 340, 600, "Море", "Весна,Лето,Осень", "Воблер,Пилькер,Сардина,Спрут,Кальмар", 55, None),
                ("Ваху", "Редкая", 5.0, 50.0, 60, 200, 200, "Море", "Весна,Лето,Осень", "Пилькер,Воблер,Кусочки рыбы,Сардина", 30, None),
                ("Барракуда", "Редкая", 3.0, 30.0, 50, 180, 150, "Море", "Все", "Воблер,Блесна,Живец,Кусочки рыбы,Пилькер", 25, None),
                ("Палтус синекорый", "Легендарная", 20.0, 200.0, 80, 250, 600, "Море", "Осень,Зима", "Моллюск,Кусочки рыбы,Кальмар,Сельдь", 55, None),
                ("Скат-хвостокол", "Редкая", 5.0, 60.0, 40, 180, 160, "Море", "Все", "Морской червь,Моллюск,Кусочки рыбы,Кальмар", 25, None),
                ("Морской чёрт", "Мифическая", 5.0, 40.0, 40, 180, 300, "Море", "Все", "Кальмар,Живец,Кусочки рыбы,Крупный живец", 40, None),
                ("Конгер", "Редкая", 5.0, 50.0, 80, 300, 320, "Море", "Все", "Крупный кусок мяса,Кальмар,Живец,Кусок мяса", 40, None),
                ("Луфарь", "Обычная", 1.0, 15.0, 30, 100, 35, "Море", "Весна,Лето,Осень", "Пилькер,Блесна,Сардина,Живец", 18, None),
                ("Лаврак", "Обычная", 1.0, 12.0, 30, 100, 40, "Море", "Все", "Креветка,Воблер,Морской червь,Блесна", 18, None),
                ("Зубан", "Редкая", 2.0, 15.0, 40, 100, 120, "Море", "Все", "Кальмар,Живец,Пилькер,Кусочки рыбы", 25, None),
                ("Групер гигантский", "Мифическая", 50.0, 200.0, 100, 250, 950, "Море", "Лето,Осень", "Крупный живец,Спрут,Кусок мяса,Кальмар", 75, None),
                ("Серриола", "Редкая", 5.0, 80.0, 60, 190, 200, "Море", "Все", "Живец,Пилькер,Кальмар,Воблер", 30, None),
                ("Пеламида", "Обычная", 1.0, 10.0, 35, 90, 40, "Море", "Все", "Блесна,Сардина,Воблер,Живец,Пилькер", 18, None),
                ("Пилорыл", "Мифическая", 100.0, 400.0, 200, 700, 1300, "Море", "Лето,Осень", "Моллюск,Кусочки рыбы,Кальмар,Крупный кусок мяса", 80, None),
                ("Рыба-луна", "Мифическая", 200.0, 1500.0, 100, 330, 2000, "Море", "Лето,Осень", "Спрут,Кальмар,Моллюск,Медуза", 80, None),
                ("Сагрина", "Обычная", 0.1, 0.5, 10, 25, 10, "Море", "Весна,Лето,Осень", "Креветка,Тесто,Мякиш хлеба,Морской червь", 5, None),
                ("Морской петух", "Мифическая", 30.0, 180.0, 70, 230, 900, "Море", "Все", "Крупный живец,Туша рыбы,Кальмар,Морской червь", 75, None),
                ("Скорпена", "Редкая", 0.2, 2.0, 15, 40, 55, "Море", "Все", "Кусочки рыбы,Креветка,Живец,Морской червь", 12, None),
                ("Лихия", "Редкая", 3.0, 25.0, 40, 110, 150, "Море", "Весна,Лето,Осень", "Воблер,Живец,Сардина,Пилькер", 25, None),
                ("Сариола", "Редкая", 5.0, 40.0, 50, 150, 160, "Море", "Все", "Пилькер,Кальмар,Блесна,Живец", 25, None),
                ("Морской дракон", "Редкая", 0.1, 1.0, 10, 40, 55, "Море", "Все", "Морской червь,Опарыш,Креветка,Мотыль", 10, None),
                ("Анчоус", "Обычная", 0.01, 0.03, 5, 10, 4, "Море", "Все", "Опарыш,Тесто,Мякиш хлеба,Морской червь", 3, None),
                ("Шпрот", "Обычная", 0.01, 0.03, 5, 10, 4, "Море", "Все", "Мякиш хлеба,Тесто,Опарыш,Креветка", 3, None),
                ("Луна-рыба", "Легендарная", 30.0, 150.0, 60, 180, 550, "Море", "Лето,Осень", "Кальмар,Спрут,Сардина,Живец", 55, None),
                ("Каменный окунь", "Редкая", 0.2, 3.0, 15, 50, 60, "Море", "Все", "Креветка,Морской червь,Сало,Кусочки рыбы", 12, None),
                ("Морская лисица", "Редкая", 5.0, 60.0, 40, 180, 350, "Море", "Все", "Кусочки рыбы,Сало,Моллюск,Кальмар", 45, None),
                ("Морской черт", "Мифическая", 15.0, 100.0, 60, 200, 850, "Море", "Все", "Крупный живец,Туша рыбы,Кальмар,Спрут,Кусок мяса", 75, None),
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
                    # Сезоны берём из database.py (корректны по локации);
                    # FISH_INFO переопределял бы их одинаково для всех локаций
                    # из-за дублирующихся ключей (реки/озёра с одним именем).
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
            # Никакой рыбе не нужно набирать уровень — сбрасываем required_level в 0 для всех существующих записей
            cursor.execute('UPDATE fish SET required_level = 0')
            
            # Добавление мусора для реки
            trash_data = [
                ("Коряга", 0.5, 2, "Все", None),
                ("Старая шина", 2.0, 1, "Все", None),
                ("Консервная банка", 0.1, 1, "Все", None),
                ("Ботинок", 0.3, 2, "Все", None),
                ("Пластиковая бутылка", 0.05, 0, "Все", None),
                ("Ржавый крючок", 0.02, 5, "Все", None),
                ("Кусок трубы", 1.5, 3, "Все", None),
                ("Поломанная удочка", 1.0, 10, "Все", None),
                ("Рыболовная сетка", 0.8, 5, "Все", None),
                ("Деревянная доска", 2.5, 4, "Все", None),
                ("Старый якорь", 3.0, 15, "Все", None),
                ("Веревка", 0.3, 1, "Все", None),
            ]
            
            cursor.executemany('''
                INSERT OR IGNORE INTO trash (name, weight, price, locations, sticker_id)
                VALUES (?, ?, ?, ?, ?)
            ''', trash_data)

            # Обновляем locations всех мусорных предметов до Все (чтобы они попадали в сеть на любой локации)
            cursor.execute("UPDATE trash SET locations = 'Все' WHERE locations = 'Река'")
            
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
            # min_level игнорируется: никакой рыбе не нужно уровень
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
            # min_level игнорируется: никакой рыбе не нужно уровень
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

    def get_fish_for_location(self, location: str, season: str = "Лето", min_level: Optional[int] = None) -> List[Dict[str, Any]]:
        """Совместимость со старым API game_logic: вернуть рыбу по локации."""
        return self.get_fish_by_location(location, season, min_level=min_level)
    
    def add_caught_fish(self, user_id: int, chat_id: int, fish_name: str, weight: float, location: str, length: float = 0):
        """Добавить пойманную рыбу"""
        normalized_name = fish_name.strip() if isinstance(fish_name, str) else fish_name
        try:
            chat_id_to_store = int(chat_id) if chat_id is not None else None
        except (TypeError, ValueError):
            chat_id_to_store = None

        logger.info(
            "add_caught_fish INPUT: user_id=%s chat_id=%s (raw=%s, type=%s) fish=%s weight=%s length=%s location=%s",
            user_id, chat_id_to_store, chat_id, type(chat_id).__name__,
            normalized_name, weight, length, location
        )

        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'INSERT INTO caught_fish (user_id, chat_id, fish_name, weight, length, location)'
                ' VALUES (%s, %s, %s, %s, %s, %s)'
                ' RETURNING id, user_id, chat_id, fish_name, weight, length, location, caught_at',
                (user_id, chat_id_to_store, normalized_name, float(weight), float(length), location)
            )
            saved = cursor.fetchone()

        if saved:
            logger.info(
                "add_caught_fish SAVED IN DB: id=%s user_id=%s chat_id=%s fish=%s weight=%s length=%s location=%s caught_at=%s",
                saved[0], saved[1], saved[2], saved[3], saved[4], saved[5], saved[6], saved[7]
            )
        else:
            logger.warning(
                "add_caught_fish: INSERT returned no row — possible constraint violation. user_id=%s chat_id=%s fish=%s",
                user_id, chat_id_to_store, normalized_name
            )
    
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

    def get_total_fish_species(self) -> int:
        """Возвращает общее количество видов рыб в каталоге."""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM fish')
            row = cursor.fetchone()
            return row[0] if row else 0

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
            #
            # JOIN uses LOWER(TRIM(...)) for case-insensitive matching so that fish stored with
            # minor casing differences still resolve correctly from the fish/trash catalogs.
            # trash_name is included to distinguish actual trash (t.name IS NOT NULL) from a
            # failed JOIN with the fish table (both f.name and t.name are NULL).
            cursor.execute('''
                SELECT cf.*, 
                       COALESCE(f.name, t.name) AS name,
                       COALESCE(f.rarity, 'Мусор') AS rarity,
                       COALESCE(f.price, t.price, 0) AS price,
                       f.min_weight AS min_weight,
                       f.max_weight AS max_weight,
                       f.min_length AS min_length,
                       f.max_length AS max_length,
                       CASE WHEN f.name IS NULL THEN 1 ELSE 0 END AS is_trash,
                       t.name AS trash_name
                FROM caught_fish cf
                LEFT JOIN fish f ON LOWER(TRIM(cf.fish_name)) = LOWER(f.name)
                LEFT JOIN trash t ON LOWER(TRIM(cf.fish_name)) = LOWER(t.name)
                WHERE cf.user_id = ? AND (cf.chat_id = ? OR cf.chat_id IS NULL OR cf.chat_id < 1)
                ORDER BY cf.weight DESC
            ''', (user_id, chat_id))
            
            rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            results = [dict(zip(columns, row)) for row in rows]

            # Collect items where the JOIN failed (is_trash=1 meaning f.name IS NULL,
            # AND trash_name IS NULL meaning not in trash table either).
            # These are real fish whose names don't match due to encoding/case differences.
            # We do a single batch secondary lookup to recover their catalog data.
            orphan_indices = [
                i for i, item in enumerate(results)
                if item.get('is_trash') and item.get('trash_name') is None
            ]
            if orphan_indices:
                orphan_names = [results[i].get('fish_name', '') for i in orphan_indices]
                try:
                    placeholders = ','.join(['?' for _ in orphan_names])
                    cursor.execute(
                        f"SELECT name, rarity, price, min_weight, max_weight, min_length, max_length "
                        f"FROM fish WHERE LOWER(name) IN ({placeholders})",
                        [n.lower().strip() for n in orphan_names]
                    )
                    lookup_rows = cursor.fetchall()
                    fish_by_lower = {}
                    for row in lookup_rows:
                        fish_by_lower[str(row[0]).lower()] = {
                            'rarity': row[1], 'price': row[2],
                            'min_weight': row[3], 'max_weight': row[4],
                            'min_length': row[5], 'max_length': row[6],
                        }
                    for i in orphan_indices:
                        item = results[i]
                        key = str(item.get('fish_name', '')).lower().strip()
                        fish_row = fish_by_lower.get(key)
                        if fish_row:
                            item.update(fish_row)
                            item['is_trash'] = 0
                            item['name'] = item.get('fish_name', '')
                except Exception:
                    logger.exception("get_caught_fish: secondary orphan lookup failed")

            for item in results:
                # Only skip price recalculation for genuine trash items (in the trash catalog).
                # Fish with is_trash=1 but no trash_name match were not found in either catalog;
                # they still get a price so they don't show as 0 coins in the shop.
                if item.get('is_trash') and item.get('trash_name') is not None:
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
            'Мифическая': 5.0,
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
            'Легендарная': 3.0,
            'Мифическая': 0.05,
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

    def get_chat_leaderboard_period(self, chat_id: int, limit: int = 10, since: Optional[datetime] = None, until: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """Получить топ по общему весу улова в конкретном чате за период.
        Логика идентична get_leaderboard_period (sold=0, JOIN fish),
        но с обязательным фильтром cf.chat_id = chat_id.
        """
        with self._connect() as conn:
            cursor = conn.cursor()

            where_clauses: List[str] = ['cf.chat_id = %s', 'cf.sold = 0']
            params: List = [chat_id]

            if since is not None:
                where_clauses.append('cf.caught_at >= %s')
                params.append(since.strftime('%Y-%m-%d %H:%M:%S'))
            if until is not None:
                where_clauses.append('cf.caught_at <= %s')
                params.append(until.strftime('%Y-%m-%d %H:%M:%S'))

            where_sql = 'WHERE ' + ' AND '.join(where_clauses)

            query = f'''
                SELECT
                    COALESCE(MAX(p.username), 'Неизвестно') as username,
                    cf.user_id,
                    COUNT(cf.id) as total_fish,
                    COALESCE(SUM(cf.weight), 0) as total_weight
                FROM caught_fish cf
                JOIN fish f ON TRIM(cf.fish_name) = f.name
                LEFT JOIN players p ON p.user_id = cf.user_id
                {where_sql}
                GROUP BY cf.user_id
                ORDER BY total_weight DESC, total_fish DESC
                LIMIT %s
            '''

            params.append(limit)
            logger.info(
                'get_chat_leaderboard_period QUERY: chat_id=%s since=%s until=%s limit=%s',
                chat_id, since, until, limit
            )
            try:
                cursor.execute(query, params)
                rows = cursor.fetchall()
                logger.info(
                    'get_chat_leaderboard_period RESULT: %d rows for chat_id=%s',
                    len(rows), chat_id
                )
                for row in rows:
                    logger.info(
                        '  row: username=%s user_id=%s total_fish=%s total_weight=%s',
                        row[0], row[1], row[2], row[3]
                    )
                return [
                    {
                        'username': row[0],
                        'user_id': row[1],
                        'total_fish': row[2],
                        'total_weight': row[3],
                    }
                    for row in rows
                ]
            except Exception:
                logger.exception('get_chat_leaderboard_period failed')
                return []

    def get_users_weight_leaderboard(
        self,
        user_ids: List[int],
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """Топ по общему весу непроданной рыбы для заданного списка user_id за период."""
        if not user_ids:
            return []
        with self._connect() as conn:
            cursor = conn.cursor()
            placeholders = ','.join(['%s'] * len(user_ids))
            where_clauses = [f'cf.user_id IN ({placeholders})', 'cf.sold = 0']
            params: List = list(user_ids)
            if since is not None:
                where_clauses.append('cf.caught_at >= %s')
                params.append(since.strftime('%Y-%m-%d %H:%M:%S'))
            if until is not None:
                where_clauses.append('cf.caught_at <= %s')
                params.append(until.strftime('%Y-%m-%d %H:%M:%S'))
            where_sql = 'WHERE ' + ' AND '.join(where_clauses)
            query = f'''
                SELECT
                    cf.user_id,
                    COALESCE(MAX(p.username), 'Неизвестно') AS username,
                    COUNT(cf.id) AS total_fish,
                    COALESCE(SUM(cf.weight), 0) AS total_weight
                FROM caught_fish cf
                LEFT JOIN players p ON p.user_id = cf.user_id
                {where_sql}
                GROUP BY cf.user_id
                ORDER BY total_weight DESC, total_fish DESC
            '''
            try:
                cursor.execute(query, params)
                rows = cursor.fetchall()
                return [
                    {
                        'user_id': row[0],
                        'username': row[1],
                        'total_fish': row[2],
                        'total_weight': float(row[3]),
                    }
                    for row in rows
                ]
            except Exception:
                logger.exception('get_users_weight_leaderboard failed')
                return []

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

    def get_leaderboard_period(self, limit: int = 10, since: Optional[datetime] = None, until: Optional[datetime] = None, chat_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """Получить таблицу лидеров за период (с фильтром по началу и концу) и/или по чату"""
        with self._connect() as conn:
            cursor = conn.cursor()

            where_clauses: List[str] = []
            params: List = []

            # Always join players to get username
            join_clause = "LEFT JOIN players p ON p.user_id = cf.user_id"

            # NOTE: Per configuration, leaderboard no longer supports filtering by chat_id.
            # The `chat_id` parameter is accepted for compatibility but ignored.

            if since is not None:
                where_clauses.append("datetime(cf.caught_at) >= datetime(?)")
                params.append(since.strftime("%Y-%m-%d %H:%M:%S"))
            if until is not None:
                where_clauses.append("datetime(cf.caught_at) <= datetime(?)")
                params.append(until.strftime("%Y-%m-%d %H:%M:%S"))

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
    
    def get_rods(self) -> List[Dict[str, Any]]:
        """Получить список всех удочек"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM rods ORDER BY price')
            rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            return [dict(zip(columns, row)) for row in rows]

    def ensure_rod_catalog(self):
        """Гарантировать наличие базового каталога удочек и корректного max_weight."""
        rods_data = [
            ("Бамбуковая удочка", 0, 100, 100, 0, 30),
            ("Углепластиковая удочка", 1500, 150, 150, 5, 60),
            ("Карбоновая удочка", 4500, 200, 200, 10, 120),
            ("Золотая удочка", 15000, 300, 300, 20, 350),
            ("Удачливая удочка", 25000, 150, 150, 15, 650),
        ]
        rods_weight_updates = [
            (30, "Бамбуковая удочка"),
            (60, "Углепластиковая удочка"),
            (120, "Карбоновая удочка"),
            (350, "Золотая удочка"),
            (650, "Удачливая удочка"),
        ]

        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.executemany(
                '''
                INSERT OR IGNORE INTO rods (name, price, durability, max_durability, fish_bonus, max_weight)
                VALUES (?, ?, ?, ?, ?, ?)
                ''',
                rods_data,
            )

            for max_w, rod_name in rods_weight_updates:
                cursor.execute('UPDATE rods SET max_weight = ? WHERE name = ?', (max_w, rod_name))

            conn.commit()
    
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

    def get_active_feeder_bonus(self, user_id: int, chat_id: int) -> int:
        """Получить активный бонус кормушки для игрока.

        Возвращает процентный бонус (целое число). Если таблиц/колонок кормушек
        в текущей схеме нет, возвращает 0.
        """
        candidate_tables = [
            'player_feeders',
            'active_feeders',
            'player_feeder_effects',
            'feeders_active',
        ]
        bonus_columns_priority = ['bonus_percent', 'fish_bonus', 'bonus', 'effect_bonus']
        time_columns_priority = ['expires_at', 'active_until', 'ends_at', 'end_at']
        active_columns_priority = ['is_active', 'active', 'enabled']

        with self._connect() as conn:
            cursor = conn.cursor()

            for table in candidate_tables:
                try:
                    cursor.execute(
                        "SELECT column_name FROM information_schema.columns WHERE table_name = %s AND table_schema = 'public'",
                        (table,)
                    )
                    columns = {row[0] for row in cursor.fetchall()}
                    if not columns or 'user_id' not in columns:
                        continue

                    bonus_col = next((col for col in bonus_columns_priority if col in columns), None)
                    if not bonus_col:
                        continue

                    time_col = next((col for col in time_columns_priority if col in columns), None)
                    active_col = next((col for col in active_columns_priority if col in columns), None)

                    where_parts = ["user_id = ?"]
                    params: List[Union[int, str]] = [user_id]

                    if 'chat_id' in columns:
                        where_parts.append("(chat_id = ? OR chat_id IS NULL OR chat_id < 1)")
                        params.append(chat_id)

                    if active_col:
                        where_parts.append(f"({active_col} = 1 OR {active_col} IS NULL)")

                    if time_col:
                        where_parts.append(f"({time_col} IS NULL OR {time_col} > CURRENT_TIMESTAMP)")

                    query = f"SELECT COALESCE(MAX({bonus_col}), 0) FROM {table} WHERE " + " AND ".join(where_parts)
                    cursor.execute(query, params)
                    row = cursor.fetchone()
                    value = int((row[0] if row else 0) or 0)
                    return max(0, value)
                except Exception:
                    # Пробуем следующую таблицу/схему без падения игрового цикла.
                    continue

        return 0

    def _ensure_booster_tables(self):
        """Создать таблицы бустеров (кормушки/эхолот), если их еще нет."""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS player_feeders (
                    id INTEGER PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    chat_id BIGINT DEFAULT 0,
                    feeder_type TEXT NOT NULL,
                    bonus_percent INTEGER NOT NULL,
                    expires_at TIMESTAMP NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, chat_id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS player_echosounder (
                    id INTEGER PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    chat_id BIGINT DEFAULT 0,
                    expires_at TIMESTAMP NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, chat_id)
                )
            ''')
            conn.commit()

    def get_active_feeder(self, user_id: int, chat_id: int) -> Optional[Dict[str, Any]]:
        """Вернуть активную кормушку пользователя для чата (или глобальную)."""
        self._ensure_booster_tables()
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                SELECT user_id, chat_id, feeder_type, bonus_percent, expires_at
                FROM player_feeders
                WHERE user_id = ?
                  AND (chat_id = ? OR chat_id IS NULL OR chat_id < 1)
                  AND expires_at > CURRENT_TIMESTAMP
                ORDER BY expires_at DESC
                LIMIT 1
                ''',
                (user_id, chat_id),
            )
            row = cursor.fetchone()
            if not row:
                return None
            cols = [d[0] for d in cursor.description]
            return dict(zip(cols, row))

    def get_feeder_cooldown_remaining(self, user_id: int, chat_id: int) -> int:
        """Вернуть оставшееся время активной кормушки в секундах."""
        self._ensure_booster_tables()
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                SELECT COALESCE(EXTRACT(EPOCH FROM (expires_at - CURRENT_TIMESTAMP)), 0)
                FROM player_feeders
                WHERE user_id = ?
                  AND (chat_id = ? OR chat_id IS NULL OR chat_id < 1)
                  AND expires_at > CURRENT_TIMESTAMP
                ORDER BY expires_at DESC
                LIMIT 1
                ''',
                (user_id, chat_id),
            )
            row = cursor.fetchone()
            if not row or row[0] is None:
                return 0
            return max(0, int(row[0]))

    def activate_feeder(self, user_id: int, chat_id: int, feeder_type: str, bonus_percent: int, duration_minutes: int):
        """Активировать кормушку для пользователя в текущем чате."""
        self._ensure_booster_tables()
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                INSERT INTO player_feeders (user_id, chat_id, feeder_type, bonus_percent, expires_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP + (? || ' minutes')::interval)
                ON CONFLICT (user_id, chat_id) DO UPDATE SET
                    feeder_type = EXCLUDED.feeder_type,
                    bonus_percent = EXCLUDED.bonus_percent,
                    expires_at = EXCLUDED.expires_at
                ''',
                (user_id, chat_id, feeder_type, int(bonus_percent), int(duration_minutes)),
            )
            conn.commit()

    def get_echosounder_remaining_seconds(self, user_id: int, chat_id: int) -> int:
        """Вернуть оставшееся время эхолота (глобально на пользователя) в секундах."""
        self._ensure_booster_tables()
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                SELECT COALESCE(EXTRACT(EPOCH FROM (expires_at - CURRENT_TIMESTAMP)), 0)
                FROM player_echosounder
                WHERE user_id = ?
                  AND chat_id = 0
                  AND expires_at > CURRENT_TIMESTAMP
                ORDER BY expires_at DESC
                LIMIT 1
                ''',
                (user_id,),
            )
            row = cursor.fetchone()
            if not row or row[0] is None:
                return 0
            return max(0, int(row[0]))

    def is_echosounder_active(self, user_id: int, chat_id: int) -> bool:
        """Проверить, активен ли эхолот у пользователя."""
        return self.get_echosounder_remaining_seconds(user_id, chat_id) > 0

    def activate_echosounder(self, user_id: int, chat_id: int, duration_hours: int):
        """Активировать эхолот (глобально на пользователя, независимо от чата)."""
        self._ensure_booster_tables()
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                INSERT INTO player_echosounder (user_id, chat_id, expires_at)
                VALUES (?, 0, CURRENT_TIMESTAMP + (? || ' hours')::interval)
                ON CONFLICT (user_id, chat_id) DO UPDATE SET
                    expires_at = EXCLUDED.expires_at
                ''',
                (user_id, int(duration_hours)),
            )
            conn.commit()
    
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
                WHERE locations LIKE ? OR locations = 'Все'
                ORDER BY name
            ''', (f"%{location}%",))
            rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            result = [dict(zip(columns, row)) for row in rows]
            # Если нет мусора для конкретной локации — вернём весь мусор 
            if not result:
                cursor.execute('SELECT * FROM trash ORDER BY name')
                rows = cursor.fetchall()
                result = [dict(zip(columns, row)) for row in rows]
            return result
    
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
                # Ensure row exists
                cursor.execute('INSERT OR IGNORE INTO chat_configs (chat_id, admin_user_id, is_configured, chat_title, stars_total) VALUES (?, ?, 1, ?, 0)', (chat_id, 0, chat_title))
                # Update title if provided
                if chat_title is not None:
                    cursor.execute('UPDATE chat_configs SET chat_title = ? WHERE chat_id = ?', (chat_title, chat_id))
                cursor.execute('UPDATE chat_configs SET stars_total = COALESCE(stars_total, 0) + ? WHERE chat_id = ?', (amount, chat_id))
                conn.commit()
                return True
        except Exception as e:
            logger.error("increment_chat_stars error: %s", e)
            return False

    def get_all_chat_stars(self) -> List[Dict[str, Any]]:
        """Return list of chats with their title and total stars, sourced from star_transactions."""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT
                    s.chat_id,
                    COALESCE(c.chat_title, '') AS chat_title,
                    COALESCE(s.stars_total, 0) AS stars_total,
                    COALESCE(s.occurrences, 0) AS occurrences
                FROM (
                    SELECT chat_id,
                           SUM(total_amount) AS stars_total,
                           COUNT(*) AS occurrences
                    FROM star_transactions
                    WHERE chat_id IS NOT NULL
                      AND COALESCE(refund_status, 'none') = 'none'
                    GROUP BY chat_id
                ) s
                LEFT JOIN chat_configs c ON c.chat_id = s.chat_id
                ORDER BY s.stars_total DESC NULLS LAST
            ''')
            rows = cursor.fetchall()
            cols = [d[0] for d in cursor.description]
            return [dict(zip(cols, r)) for r in rows]

    def get_chat_occurrences(self, chat_id: int) -> int:
        """Return number of star_transactions rows for a given chat_id."""
        if chat_id is None:
            return 0
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM star_transactions WHERE chat_id = ?', (chat_id,))
            row = cursor.fetchone()
            return int(row[0]) if row else 0

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
            cursor.execute('''
                UPDATE star_transactions
                SET refund_status = ?
                WHERE telegram_payment_charge_id = ?
            ''', (refund_status, telegram_payment_charge_id))
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
            cursor.execute('''
                SELECT current_durability FROM player_rods 
                WHERE user_id = %s AND (chat_id IS NULL OR chat_id < 1) AND rod_name = %s
            ''', (user_id, rod_name))
            
            result = cursor.fetchone()
            if not result:
                # Если записи нет - инициализируем удочку в этом чате
                self.init_player_rod(user_id, rod_name, chat_id=chat_id)
            
            # Уменьшаем прочность
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
                # If this is a temporary rod (uses range), initialize uses accordingly
                uses = self._get_temp_rod_uses(rod_name)
                if uses is None:
                    max_dur = rod.get('max_durability', rod.get('durability', 0))
                    current = max_dur
                else:
                    max_dur = uses
                    current = uses

                cursor.execute('''
                    UPDATE player_rods
                    SET current_durability = ?, max_durability = ?
                    WHERE user_id = ? AND (chat_id IS NULL OR chat_id < 1) AND rod_name = ?
                ''', (current, max_dur, user_id, rod_name))
            else:
                uses = self._get_temp_rod_uses(rod_name)
                if uses is None:
                    max_dur = rod.get('max_durability', rod.get('durability', 0))
                    current = max_dur
                else:
                    max_dur = uses
                    current = uses

                cursor.execute('''
                    INSERT OR REPLACE INTO player_rods (user_id, rod_name, current_durability, max_durability, chat_id)
                    VALUES (?, ?, ?, ?, -1)
                ''', (user_id, rod_name, current, max_dur))
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

    def reset_net_cooldowns(self, user_id: int) -> None:
        """Сбросить кулдаун всех сетей игрока (обнулить время последнего использования)"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'UPDATE players SET last_net_use_time = NULL WHERE user_id = %s',
                (user_id,)
            )
            cursor.execute(
                'UPDATE player_nets SET last_use_time = NULL WHERE user_id = %s',
                (user_id,)
            )
            conn.commit()

    # ===== РЕФЕРАЛЬНАЯ СИСТЕМА =====
    
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
            cursor.execute('''
                SELECT chat_id, admin_ref_link, chat_invite_link
                FROM chat_configs 
                WHERE admin_user_id = %s AND is_configured = 1
            ''', (user_id,))
            rows = cursor.fetchall()
            cols = [d[0] for d in cursor.description] if cursor.description else []
            return [dict(zip(cols, row)) for row in rows]

    def update_population_state(self, user_id: int, current_location: str) -> tuple:
        """
        Обновить состояние популяции рыб на локации.
        Отслеживает, сколько раз подряд игрок ловит на одной локации.
        Возвращает (location_changed, consecutive_casts, show_warning)
        """
        with self._connect() as conn:
            cursor = conn.cursor()
            
            # Получаем текущее состояние игрока
            cursor.execute('''
                SELECT consecutive_casts_at_location, last_fishing_location, population_penalty
                FROM players
                WHERE user_id = %s AND chat_id = -1
            ''', (user_id,))
            row = cursor.fetchone()
            
            if not row:
                # Новый игрок
                location_changed = True
                consecutive_casts = 1
                population_penalty = 0.0
            else:
                last_casts, last_location, population_penalty = row
                last_casts = last_casts or 0
                population_penalty = population_penalty or 0.0
                
                # Проверяем, изменилась ли локация
                location_changed = (last_location != current_location)
                
                if location_changed:
                    # Игрок переместился на новую локацию
                    consecutive_casts = 1
                    population_penalty = 0.0  # Штраф сбрасывается при смене локации
                else:
                    # Остался на той же локации
                    consecutive_casts = last_casts + 1
                    
                    # Рассчитываем штраф на основе количества забросов
                    if consecutive_casts >= 60:
                        population_penalty = 15.0
                    elif consecutive_casts >= 50:
                        population_penalty = 11.0
                    elif consecutive_casts >= 40:
                        population_penalty = 8.0
                    elif consecutive_casts >= 30:
                        population_penalty = 5.0
                    else:
                        population_penalty = 0.0
            
            # Обновляем состояние в базе
            cursor.execute('''
                UPDATE players
                SET consecutive_casts_at_location = %s,
                    last_fishing_location = %s,
                    population_penalty = %s
                WHERE user_id = %s AND chat_id = -1
            ''', (consecutive_casts, current_location, population_penalty, user_id))
            conn.commit()
            
            # show_warning если достигли 30 забросов
            show_warning = (consecutive_casts == 30 and not location_changed)
            
            return (location_changed, consecutive_casts, show_warning)
    
    def get_consecutive_casts(self, user_id: int) -> int:
        """Получить количество консекутивных забросов на текущей локации"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT consecutive_casts_at_location
                FROM players
                WHERE user_id = %s AND chat_id = -1
            ''', (user_id,))
            row = cursor.fetchone()
            return row[0] if (row and row[0]) else 0
    
    def get_population_penalty(self, user_id: int) -> float:
        """Получить текущий штраф на популяцию рыб для игрока"""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT population_penalty
                FROM players
                WHERE user_id = %s AND chat_id = -1
            ''', (user_id,))
            row = cursor.fetchone()
            return row[0] if (row and row[0]) else 0.0

    def add_treasure(self, user_id: int, treasure_name: str, quantity: int = 1, chat_id: int = -1):
        """Добавить сокровище игроку"""
        with self._connect() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute('''
                    INSERT INTO player_treasures (user_id, chat_id, treasure_name, quantity)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (user_id, chat_id, treasure_name) DO UPDATE
                    SET quantity = quantity + %s
                ''', (user_id, chat_id, treasure_name, quantity, quantity))
                conn.commit()
            except Exception as e:
                logger.error(f"Error adding treasure: {e}")
                conn.rollback()

    def get_player_treasures(self, user_id: int, chat_id: int) -> List[Dict[str, Any]]:
        """Получить все сокровища игрока"""
        with self._connect() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute('''
                    SELECT treasure_name, quantity, obtained_at
                    FROM player_treasures
                    WHERE user_id = %s AND chat_id = %s AND quantity > 0
                    ORDER BY obtained_at DESC
                ''', (user_id, chat_id))
                rows = cursor.fetchall()
                cols = [d[0] for d in cursor.description] if cursor.description else []
                return [dict(zip(cols, row)) for row in rows]
            except Exception as e:
                logger.error(f"Error getting player treasures: {e}")
                return []

    def remove_treasure(self, user_id: int, chat_id: int, treasure_name: str, quantity: int = 1):
        """Удалить сокровище у игрока"""
        with self._connect() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute('''
                    UPDATE player_treasures
                    SET quantity = quantity - %s
                    WHERE user_id = %s AND chat_id = %s AND treasure_name = %s
                ''', (quantity, user_id, chat_id, treasure_name))
                conn.commit()
            except Exception as e:
                logger.error(f"Error removing treasure: {e}")
                conn.rollback()


# Экземпляр базы данных для импорта в других модулях
db = Database()
