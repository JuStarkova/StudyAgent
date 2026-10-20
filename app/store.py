import json
import sqlite3
import threading
import time

from .config import DB_PATH

_lock = threading.Lock()


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with _lock, _conn() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS sessions(
            id TEXT PRIMARY KEY,
            title TEXT,
            created_at REAL
        );
        CREATE TABLE IF NOT EXISTS corpus(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT,
            source TEXT,
            page INTEGER,
            content TEXT
        );
        CREATE TABLE IF NOT EXISTS topics(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT,
            idx INTEGER,
            title TEXT,
            summary TEXT,
            pages_json TEXT,
            status TEXT
        );
        CREATE TABLE IF NOT EXISTS cards(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT,
            topic TEXT,
            original_md TEXT,
            simplified_md TEXT,
            has_errors INTEGER,
            errors_json TEXT,
            examples_json TEXT,
            created_at REAL
        );
        CREATE TABLE IF NOT EXISTS chat(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            card_id INTEGER,
            role TEXT,
            content TEXT,
            created_at REAL
        );
        CREATE TABLE IF NOT EXISTS events(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT,
            ts REAL,
            stage TEXT,
            status TEXT,
            message TEXT,
            progress REAL,
            eta REAL,
            topic TEXT
        );
        CREATE TABLE IF NOT EXISTS threads(
            session_id TEXT PRIMARY KEY,
            thread_id TEXT,
            updated_at REAL
        );
        """)


def create_session(session_id: str, title: str = ""):
    with _lock, _conn() as c:
        c.execute("INSERT OR IGNORE INTO sessions(id, title, created_at) VALUES(?,?,?)",
                  (session_id, title, time.time()))


def set_thread_id(session_id: str, thread_id: str):
    with _lock, _conn() as c:
        c.execute(
            "INSERT INTO threads(session_id, thread_id, updated_at) VALUES(?,?,?) "
            "ON CONFLICT(session_id) DO UPDATE SET thread_id=excluded.thread_id, "
            "updated_at=excluded.updated_at",
            (session_id, thread_id, time.time()))


def get_thread_id(session_id: str) -> str | None:
    with _lock, _conn() as c:
        row = c.execute("SELECT thread_id FROM threads WHERE session_id=?",
                        (session_id,)).fetchone()
        return row["thread_id"] if row else None


def clear_all_thread_ids():
    """Чекпоинты графа живут в памяти — после рестарта все thread_id протухшие."""
    with _lock, _conn() as c:
        c.execute("DELETE FROM threads")


def clear_thread_id(session_id: str):
    with _lock, _conn() as c:
        c.execute("DELETE FROM threads WHERE session_id=?", (session_id,))


def clear_session_data(session_id: str):
    """Удаляет корпус, темы, карточки, чаты, события и thread_id сессии."""
    with _lock, _conn() as c:
        ids = [r["id"] for r in c.execute(
            "SELECT id FROM cards WHERE session_id=?", (session_id,)).fetchall()]
        if ids:
            marks = ",".join("?" * len(ids))
            c.execute(f"DELETE FROM chat WHERE card_id IN ({marks})", ids)
        for table in ("corpus", "topics", "cards", "events", "threads"):
            c.execute(f"DELETE FROM {table} WHERE session_id=?", (session_id,))


def session_exists(session_id: str) -> bool:
    with _lock, _conn() as c:
        row = c.execute("SELECT 1 FROM sessions WHERE id=?", (session_id,)).fetchone()
        return row is not None


def save_corpus_doc(session_id: str, source: str, page: int, content: str):
    with _lock, _conn() as c:
        c.execute("INSERT INTO corpus(session_id, source, page, content) VALUES(?,?,?,?)",
                  (session_id, source, page, content))


def get_corpus(session_id: str) -> list[dict]:
    with _lock, _conn() as c:
        rows = c.execute(
            "SELECT source, page, content FROM corpus WHERE session_id=? ORDER BY source, page",
            (session_id,)).fetchall()
        return [dict(r) for r in rows]


def corpus_count(session_id: str) -> int:
    with _lock, _conn() as c:
        return c.execute("SELECT COUNT(*) n FROM corpus WHERE session_id=?", (session_id,)).fetchone()["n"]


def save_topics(session_id: str, topics: list[dict], status: str):
    with _lock, _conn() as c:
        c.execute("DELETE FROM topics WHERE session_id=?", (session_id,))
        for i, t in enumerate(topics):
            c.execute(
                "INSERT INTO topics(session_id, idx, title, summary, pages_json, status) VALUES(?,?,?,?,?,?)",
                (session_id, i, t.get("title", f"Тема {i+1}"), t.get("summary", ""),
                 json.dumps(t.get("pages", []), ensure_ascii=False), status))


def get_topics(session_id: str) -> list[dict]:
    with _lock, _conn() as c:
        rows = c.execute(
            "SELECT title, summary, pages_json, status FROM topics WHERE session_id=? ORDER BY idx",
            (session_id,)).fetchall()
        return [{"title": r["title"], "summary": r["summary"],
                 "pages": json.loads(r["pages_json"] or "[]"), "status": r["status"]} for r in rows]


def save_card(session_id: str, topic: str, original_md: str, simplified_md: str,
              has_errors: bool, errors: list, examples: list) -> int:
    with _lock, _conn() as c:
        cur = c.execute(
            "INSERT INTO cards(session_id, topic, original_md, simplified_md, has_errors, errors_json, examples_json, created_at)"
            " VALUES(?,?,?,?,?,?,?,?)",
            (session_id, topic, original_md, simplified_md, int(has_errors),
             json.dumps(errors, ensure_ascii=False), json.dumps(examples, ensure_ascii=False), time.time()))
        return cur.lastrowid


def delete_card_by_topic(session_id: str, topic: str):
    with _lock, _conn() as c:
        rows = c.execute("SELECT id FROM cards WHERE session_id=? AND topic=?",
                         (session_id, topic)).fetchall()
        ids = [r["id"] for r in rows]
        if ids:
            marks = ",".join("?" * len(ids))
            c.execute(f"DELETE FROM chat WHERE card_id IN ({marks})", ids)
            c.execute("DELETE FROM cards WHERE session_id=? AND topic=?", (session_id, topic))


def delete_cards_not_in(session_id: str, topics: list[str]):
    with _lock, _conn() as c:
        if not topics:
            rows = c.execute("SELECT id FROM cards WHERE session_id=?", (session_id,)).fetchall()
        else:
            marks = ",".join("?" * len(topics))
            rows = c.execute(
                f"SELECT id FROM cards WHERE session_id=? AND topic NOT IN ({marks})",
                (session_id, *topics)).fetchall()
        ids = [r["id"] for r in rows]
        if ids:
            marks = ",".join("?" * len(ids))
            c.execute(f"DELETE FROM chat WHERE card_id IN ({marks})", ids)
            c.execute(f"DELETE FROM cards WHERE id IN ({marks})", ids)


def get_cards(session_id: str) -> list[dict]:
    with _lock, _conn() as c:
        rows = c.execute(
            "SELECT id, topic, has_errors, created_at FROM cards WHERE session_id=? ORDER BY id",
            (session_id,)).fetchall()
        return [dict(r) for r in rows]


def get_card(card_id: int) -> dict | None:
    with _lock, _conn() as c:
        r = c.execute("SELECT * FROM cards WHERE id=?", (card_id,)).fetchone()
        if not r:
            return None
        d = dict(r)
        d["errors"] = json.loads(d.pop("errors_json") or "[]")
        d["examples"] = json.loads(d.pop("examples_json") or "[]")
        return d


def add_chat(card_id: int, role: str, content: str):
    with _lock, _conn() as c:
        c.execute("INSERT INTO chat(card_id, role, content, created_at) VALUES(?,?,?,?)",
                  (card_id, role, content, time.time()))


def get_chat(card_id: int) -> list[dict]:
    with _lock, _conn() as c:
        rows = c.execute("SELECT role, content FROM chat WHERE card_id=? ORDER BY id", (card_id,)).fetchall()
        return [dict(r) for r in rows]


def log_event(session_id: str, stage: str, status: str, message: str,
              progress: float | None = None, eta: float | None = None, topic: str | None = None):
    with _lock, _conn() as c:
        c.execute(
            "INSERT INTO events(session_id, ts, stage, status, message, progress, eta, topic) VALUES(?,?,?,?,?,?,?,?)",
            (session_id, time.time(), stage, status, message, progress, eta, topic))


def get_events(session_id: str, limit: int = 200) -> list[dict]:
    with _lock, _conn() as c:
        rows = c.execute(
            "SELECT ts, stage, status, message, progress, eta, topic FROM events"
            " WHERE session_id=? ORDER BY id DESC LIMIT ?",
            (session_id, limit)).fetchall()
        return [dict(r) for r in reversed(rows)]
