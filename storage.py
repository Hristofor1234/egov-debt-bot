import sqlite3
from pathlib import Path
from typing import Optional


class Storage:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._init_db()

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    original_name TEXT NOT NULL,
                    input_path TEXT NOT NULL,
                    output_path TEXT,
                    status TEXT NOT NULL DEFAULT 'uploaded',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS check_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fio TEXT,
                    iin TEXT,
                    duration_seconds REAL NOT NULL,
                    status TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()

    def save_file_record(self, user_id: int, original_name: str, input_path: str) -> int:
        with self._connect() as conn:
            cursor = conn.execute("""
                INSERT INTO files (user_id, original_name, input_path)
                VALUES (?, ?, ?)
            """, (user_id, original_name, input_path))
            conn.commit()
            return cursor.lastrowid

    def mark_processed(self, file_id: int, output_path: str):
        with self._connect() as conn:
            conn.execute("""
                UPDATE files
                SET output_path = ?, status = 'processed'
                WHERE id = ?
            """, (output_path, file_id))
            conn.commit()

    def mark_failed(self, file_id: int):
        with self._connect() as conn:
            conn.execute("""
                UPDATE files
                SET status = 'failed'
                WHERE id = ?
            """, (file_id,))
            conn.commit()

    def get_last_result_by_user(self, user_id: int) -> Optional[tuple]:
        with self._connect() as conn:
            cursor = conn.execute("""
                SELECT id, original_name, input_path, output_path, status, created_at
                FROM files
                WHERE user_id = ? AND status = 'processed'
                ORDER BY id DESC
                LIMIT 1
            """, (user_id,))
            return cursor.fetchone()

    def save_check_stat(self, fio: str, iin: str, duration_seconds: float, status: str):
        with self._connect() as conn:
            conn.execute("""
                INSERT INTO check_stats (fio, iin, duration_seconds, status)
                VALUES (?, ?, ?, ?)
            """, (fio, iin, duration_seconds, status))
            conn.commit()

    def get_average_check_duration(self) -> Optional[float]:
        with self._connect() as conn:
            cursor = conn.execute("""
                SELECT AVG(duration_seconds)
                FROM check_stats
                WHERE duration_seconds IS NOT NULL
            """)
            row = cursor.fetchone()
            if not row or row[0] is None:
                return None
            return float(row[0])

    def get_recent_average_check_duration(self, limit: int = 100) -> Optional[float]:
        with self._connect() as conn:
            cursor = conn.execute(f"""
                SELECT AVG(duration_seconds)
                FROM (
                    SELECT duration_seconds
                    FROM check_stats
                    WHERE duration_seconds IS NOT NULL
                    ORDER BY id DESC
                    LIMIT ?
                )
            """, (limit,))
            row = cursor.fetchone()
            if not row or row[0] is None:
                return None
            return float(row[0])