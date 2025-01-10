import sqlite3
import os
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Optional
from datetime import datetime
import logging

class DatabaseService:
    def __init__(self, db_path: str = "./points.db", max_connections: int = 60):
        self.db_path = db_path
        self.pool = ThreadPoolExecutor(max_workers=max_connections)
        self.conn = sqlite3.connect(
            db_path,
            timeout=30.0,
            isolation_level='EXCLUSIVE'
        )
        self.conn.execute('PRAGMA journal_mode=WAL')
        self.conn.row_factory = sqlite3.Row
        self.initialize()

    def initialize(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS user_points (
                user_id TEXT PRIMARY KEY,
                username TEXT NOT NULL,
                points INTEGER DEFAULT 0,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS monitored_tweets (
                tweet_id TEXT PRIMARY KEY,
                is_active BOOLEAN DEFAULT TRUE,
                like_points FLOAT DEFAULT 1,
                retweet_points FLOAT DEFAULT 2,
                reply_points FLOAT DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        self.conn.commit()

    async def get_points(self, user_id: str) -> int:
        def db_operation():
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                result = conn.execute(
                    "SELECT points FROM user_points WHERE user_id = ?",
                    (user_id,)
                ).fetchone()
                return result['points'] if result else 0

        return await asyncio.get_event_loop().run_in_executor(
            self.pool, 
            db_operation
        )

    async def add_monitored_tweet(self, tweet_id: str, points: Dict[str, float]) -> None:
        def db_operation():
            try:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()
                    
                    # Check active tweets count
                    active_count = cursor.execute(
                        "SELECT COUNT(*) FROM monitored_tweets WHERE is_active = TRUE"
                    ).fetchone()[0]

                    if active_count >= 3:
                        cursor.execute("""
                            UPDATE monitored_tweets SET is_active = FALSE
                            WHERE tweet_id = (
                                SELECT tweet_id FROM monitored_tweets 
                                WHERE is_active = TRUE ORDER BY tweet_id ASC LIMIT 1
                            )
                        """)

                    cursor.execute("""
                        INSERT INTO monitored_tweets 
                        (tweet_id, like_points, retweet_points, reply_points)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT(tweet_id) DO UPDATE SET
                            is_active = TRUE,
                            like_points = excluded.like_points,
                            retweet_points = excluded.retweet_points,
                            reply_points = excluded.reply_points
                    """, (tweet_id, points['like'], points['retweet'], points['reply']))
                    
                    conn.commit()
            except Exception as e:
                logging.error(f"Error adding tweet {tweet_id}: {str(e)}")
                raise

        await asyncio.get_event_loop().run_in_executor(
            self.pool,
            db_operation
        )

    async def get_active_tweets(self) -> List[Dict]:
        def db_operation():
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute("""
                    SELECT 
                        tweet_id as id, 
                        like_points, 
                        retweet_points, 
                        reply_points
                    FROM monitored_tweets 
                    WHERE is_active = TRUE
                    ORDER BY created_at DESC
                """).fetchall()
                
                return [{
                    'id': row['id'],
                    'points': {
                        'like': row['like_points'],
                        'retweet': row['retweet_points'],
                        'reply': row['reply_points']
                    }
                } for row in rows]

        return await asyncio.get_event_loop().run_in_executor(
            self.pool,
            db_operation
        )

    async def update_points(self, user_id: str, username: str, points: int) -> None:
        def db_operation():
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO user_points (user_id, username, points, last_updated)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(user_id) DO UPDATE SET
                        points = points + ?,
                        username = ?,
                        last_updated = CURRENT_TIMESTAMP
                """, (user_id, username, points, points, username))
                conn.commit()

        try:
            await asyncio.get_event_loop().run_in_executor(
                self.pool,
                db_operation
            )
        except Exception as e:
            logging.error(f"Error updating points for user {user_id}: {str(e)}")
            raise

    async def remove_monitored_tweet(self, tweet_id: str) -> bool:
        def db_operation():
            try:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        "DELETE FROM monitored_tweets WHERE tweet_id = ?",
                        (tweet_id,)
                    )
                    conn.commit()
                    return cursor.rowcount > 0
            except Exception as e:
                logging.error(f"Error removing tweet {tweet_id}: {str(e)}")
                raise

        return await asyncio.get_event_loop().run_in_executor(
            self.pool,
            db_operation
        )

    async def backup_database(self, backup_dir: str = './backup') -> None:
        def db_operation():
            try:
                os.makedirs(backup_dir, mode=0o700, exist_ok=True)
                
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                backup_path = os.path.join(backup_dir, f'points_{timestamp}.db')
                
                with sqlite3.connect(self.db_path) as conn:
                    with open(backup_path, 'wb', opener=lambda p,f: os.open(p, f, 0o600)) as f:
                        for line in conn.iterdump():
                            f.write(f'{line}\n'.encode())
                        
            except Exception as e:
                logging.error(f"Backup failed: {str(e)}")
                raise

        await asyncio.get_event_loop().run_in_executor(
            self.pool,
            db_operation
        )

    async def checkpoint(self):
        def db_operation():
            self.conn.execute('PRAGMA wal_checkpoint(FULL)')
        
        await asyncio.get_event_loop().run_in_executor(
            self.pool,
            db_operation
        
        )
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pool.shutdown(wait=True)
        self.conn.close()