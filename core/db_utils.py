import sqlite3
from typing import List, Dict, Any, Tuple


def get_connection(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_retail_schema(conn: sqlite3.Connection):
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS retail_products (
        pid TEXT PRIMARY KEY,
        name TEXT,
        category TEXT,
        subcat TEXT,
        brand TEXT,
        cost REAL,
        selling REAL,
        shelf_life INTEGER
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS retail_stores (
        sid TEXT PRIMARY KEY,
        name TEXT,
        location TEXT,
        manager TEXT,
        stype TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS retail_mem (
        sale_id TEXT,
        inv_id TEXT
    )
    """)

    conn.commit()


def init_mfg_schema(conn: sqlite3.Connection):
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS mfg_equipments (
        eq_id TEXT PRIMARY KEY,
        name TEXT,
        etype TEXT,
        manufacturer TEXT,
        install_date TEXT,
        cycle_days INTEGER,
        location TEXT,
        capacity INTEGER,
        criticality INTEGER
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS mfg_technicians (
        tid TEXT PRIMARY KEY,
        name TEXT,
        age INTEGER,
        phone TEXT,
        level TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS mfg_mem (
        downtime_id TEXT,
        maintenance_id TEXT
    )
    """)

    conn.commit()


def init_edu_schema(conn: sqlite3.Connection):
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS edu_students (
        sid TEXT PRIMARY KEY,
        name TEXT,
        age INTEGER,
        gender TEXT,
        course TEXT,
        enroll_date TEXT,
        style TEXT,
        grade TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS edu_modules (
        mid TEXT PRIMARY KEY,
        mname TEXT,
        cname TEXT,
        diff INTEGER,
        mtype TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS edu_mem (
        record_id TEXT,
        resource_id TEXT
    )
    """)

    conn.commit()


def fetch_all(conn: sqlite3.Connection, table: str) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table}")
    rows = cur.fetchall()
    return [dict(r) for r in rows]


def insert_row(conn: sqlite3.Connection, table: str, data: Dict[str, Any]):
    cur = conn.cursor()
    cols = ", ".join(data.keys())
    placeholders = ", ".join(["?"] * len(data))
    cur.execute(f"INSERT OR REPLACE INTO {table} ({cols}) VALUES ({placeholders})", tuple(data.values()))
    conn.commit()


def insert_many(conn: sqlite3.Connection, table: str, rows: List[Dict[str, Any]]):
    if not rows:
        return
    cur = conn.cursor()
    cols = list(rows[0].keys())
    col_str = ", ".join(cols)
    placeholder_str = ", ".join(["?"] * len(cols))
    values = [tuple(r[c] for c in cols) for r in rows]
    cur.executemany(
        f"INSERT OR REPLACE INTO {table} ({col_str}) VALUES ({placeholder_str})",
        values
    )
    conn.commit()
