import sqlite3
import os
import asyncio
import random
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
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
        """Initialize database tables if they don't exist"""
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS user_points (
                user_id TEXT PRIMARY KEY,
                username TEXT NOT NULL,
                points REAL DEFAULT 0,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS monitored_tweets (
                tweet_id TEXT PRIMARY KEY,
                is_active BOOLEAN DEFAULT TRUE,
                like_points REAL DEFAULT 1,
                retweet_points REAL DEFAULT 2,
                reply_points REAL DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS confetti_balls (
                ball_id TEXT PRIMARY KEY,
                creator_id TEXT NOT NULL,
                total_points REAL NOT NULL,
                max_claims INTEGER NOT NULL,
                claimed_count INTEGER DEFAULT 0,
                message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                channel_id TEXT NOT NULL,
                is_active BOOLEAN DEFAULT TRUE
            );

            CREATE TABLE IF NOT EXISTS confetti_claims (
                claim_id TEXT PRIMARY KEY,
                ball_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                points_claimed REAL NOT NULL,
                claimed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(ball_id) REFERENCES confetti_balls(ball_id),
                UNIQUE(ball_id, user_id)
            );
            CREATE TABLE IF NOT EXISTS confetti_traps (
                trap_id TEXT PRIMARY KEY,
                creator_id TEXT NOT NULL,
                max_claims INTEGER NOT NULL,
                claimed_count INTEGER DEFAULT 0,
                message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                channel_id TEXT NOT NULL,
                is_active BOOLEAN DEFAULT TRUE
            );

            CREATE TABLE IF NOT EXISTS confetti_trap_claims (
                claim_id TEXT PRIMARY KEY,
                trap_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                points_lost REAL NOT NULL,
                claimed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(trap_id) REFERENCES confetti_traps(trap_id),
                UNIQUE(trap_id, user_id)
            );
            CREATE TABLE IF NOT EXISTS votes (
                vote_id TEXT PRIMARY KEY,
                creator_id TEXT NOT NULL,              
                target_user_id TEXT NOT NULL,          
                description TEXT NOT NULL,             
                expires_at TIMESTAMP NOT NULL,         
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE         
            );

            CREATE TABLE IF NOT EXISTS vote_options (
                option_id TEXT PRIMARY KEY,
                vote_id TEXT NOT NULL,                
                option_text TEXT NOT NULL,             
                points REAL NOT NULL,                  
                votes_count INTEGER DEFAULT 0,        
                FOREIGN KEY(vote_id) REFERENCES votes(vote_id)
            );

            CREATE TABLE IF NOT EXISTS vote_records (
                record_id TEXT PRIMARY KEY,
                vote_id TEXT NOT NULL,                 
                option_id TEXT NOT NULL,               
                voter_id TEXT NOT NULL,                
                voted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(vote_id) REFERENCES votes(vote_id),
                FOREIGN KEY(option_id) REFERENCES vote_options(option_id),
                UNIQUE(vote_id, voter_id)             
            );
        """)
        self.conn.commit()

    async def get_points(self, user_id: str) -> float:
        """
        Get points for a user
        Returns current points as float with 8 decimal precision
        """
        def db_operation():
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                result = conn.execute(
                    "SELECT ROUND(points, 8) as points FROM user_points WHERE user_id = ?",
                    (user_id,)
                ).fetchone()
                return float(result['points'] if result else 0)

        return await asyncio.get_event_loop().run_in_executor(
            self.pool, 
            db_operation
        )

    async def update_points(self, user_id: str, username: str, points: float) -> None:
        """
        Update points for a user
        Points are stored with 8 decimal precision
        """
        def db_operation():
            points_rounded = round(points, 8)
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO user_points (user_id, username, points, last_updated)
                    VALUES (?, ?, ROUND(?, 8), CURRENT_TIMESTAMP)
                    ON CONFLICT(user_id) DO UPDATE SET
                        points = ROUND(points + ?, 8),
                        username = ?,
                        last_updated = CURRENT_TIMESTAMP
                """, (user_id, username, points_rounded, points_rounded, username))
                conn.commit()

        try:
            await asyncio.get_event_loop().run_in_executor(
                self.pool,
                db_operation
            )
        except Exception as e:
            logging.error(f"Error updating points for user {user_id}: {str(e)}")
            raise

    async def transfer_points(self, from_user_id: str, to_user_id: str, amount: float) -> bool:
        """
        Transfer points from one user to another
        Amount is handled with 8 decimal precision
        Returns True if transfer was successful, False otherwise
        """
        def db_operation():
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            try:
                # Start transaction
                cursor.execute("BEGIN EXCLUSIVE TRANSACTION")
                
                # Check if sender has enough points
                current_points = cursor.execute(
                    "SELECT points FROM user_points WHERE user_id = ?",
                    (from_user_id,)
                ).fetchone()
                
                if not current_points or float(current_points[0]) < amount:
                    cursor.execute("ROLLBACK")
                    return False
                    
                # Round to 8 decimal places
                amount_rounded = round(amount, 8)
                
                # Deduct points from sender
                cursor.execute("""
                    UPDATE user_points 
                    SET points = ROUND(points - ?, 8),
                        last_updated = CURRENT_TIMESTAMP
                    WHERE user_id = ?
                """, (amount_rounded, from_user_id))
                
                # Add points to receiver
                cursor.execute("""
                    INSERT INTO user_points (user_id, username, points, last_updated)
                    VALUES (?, 'Unknown', ROUND(?, 8), CURRENT_TIMESTAMP)
                    ON CONFLICT(user_id) DO UPDATE SET
                        points = ROUND(points + ?, 8),
                        last_updated = CURRENT_TIMESTAMP
                """, (to_user_id, amount_rounded, amount_rounded))
                
                # Commit transaction
                conn.commit()
                return True
                
            except Exception as e:
                try:
                    cursor.execute("ROLLBACK")
                except:
                    pass  # Ignore rollback errors
                logging.error(f"Error in transfer_points: {str(e)}")
                raise
            finally:
                conn.close()

        return await asyncio.get_event_loop().run_in_executor(
            self.pool,
            db_operation
        )

    async def add_monitored_tweet(self, tweet_id: str, points: Dict[str, float]) -> None:
        """
        Add a tweet to monitor
        Points values are stored with 8 decimal precision
        """
        def db_operation():
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            try:
                cursor.execute("BEGIN EXCLUSIVE TRANSACTION")
                
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
                    VALUES (?, ROUND(?, 8), ROUND(?, 8), ROUND(?, 8))
                    ON CONFLICT(tweet_id) DO UPDATE SET
                        is_active = TRUE,
                        like_points = ROUND(excluded.like_points, 8),
                        retweet_points = ROUND(excluded.retweet_points, 8),
                        reply_points = ROUND(excluded.reply_points, 8)
                """, (tweet_id, points['like'], points['retweet'], points['reply']))
                
                conn.commit()
            except Exception as e:
                try:
                    cursor.execute("ROLLBACK")
                except:
                    pass  # Ignore rollback errors
                logging.error(f"Error adding tweet {tweet_id}: {str(e)}")
                raise
            finally:
                conn.close()

        await asyncio.get_event_loop().run_in_executor(
            self.pool,
            db_operation
        )

    async def get_active_tweets(self) -> List[Dict]:
        """
        Get all active monitored tweets
        Returns points values with 8 decimal precision
        """
        def db_operation():
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute("""
                    SELECT 
                        tweet_id as id, 
                        ROUND(like_points, 8) as like_points, 
                        ROUND(retweet_points, 8) as retweet_points, 
                        ROUND(reply_points, 8) as reply_points
                    FROM monitored_tweets 
                    WHERE is_active = TRUE
                    ORDER BY created_at DESC
                """).fetchall()
                
                return [{
                    'id': row['id'],
                    'points': {
                        'like': float(row['like_points']),
                        'retweet': float(row['retweet_points']),
                        'reply': float(row['reply_points'])
                    }
                } for row in rows]

        return await asyncio.get_event_loop().run_in_executor(
            self.pool,
            db_operation
        )

    async def remove_monitored_tweet(self, tweet_id: str) -> bool:
        """Remove a monitored tweet. Returns True if tweet was found and removed."""
        def db_operation():
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            try:
                cursor.execute("BEGIN EXCLUSIVE TRANSACTION")
                cursor.execute(
                    "DELETE FROM monitored_tweets WHERE tweet_id = ?",
                    (tweet_id,)
                )
                rows_affected = cursor.rowcount
                conn.commit()
                return rows_affected > 0
                
            except Exception as e:
                try:
                    cursor.execute("ROLLBACK")
                except:
                    pass  # Ignore rollback errors
                logging.error(f"Error removing tweet {tweet_id}: {str(e)}")
                raise
            finally:
                conn.close()

        return await asyncio.get_event_loop().run_in_executor(
            self.pool,
            db_operation
        )

    async def backup_database(self, backup_path: str) -> None:
        """Create a backup of the database"""
        def db_operation():
            try:
                # Ensure the backup directory exists with proper permissions
                backup_dir = os.path.dirname(backup_path)
                os.makedirs(backup_dir, mode=0o700, exist_ok=True)
                
                # Create backup with proper permissions
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

    async def checkpoint(self) -> None:
        """Force a WAL checkpoint"""
        def db_operation():
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('PRAGMA wal_checkpoint(FULL)')
        
        await asyncio.get_event_loop().run_in_executor(
            self.pool,
            db_operation
        )

    async def create_confetti_ball(self, ball_id: str, creator_id: str, total_points: float, 
                                 max_claims: int, message: str, channel_id: str) -> None:
        def db_operation():
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO confetti_balls 
                    (ball_id, creator_id, total_points, max_claims, message, channel_id)
                    VALUES (?, ?, ROUND(?, 8), ?, ?, ?)
                """, (ball_id, creator_id, total_points, max_claims, message, channel_id))
                conn.commit()

        await asyncio.get_event_loop().run_in_executor(self.pool, db_operation)

    async def get_confetti_ball(self, ball_id: str) -> Optional[Dict]:
        def db_operation():
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute("""
                    SELECT * FROM confetti_balls 
                    WHERE ball_id = ? AND is_active = TRUE
                """, (ball_id,)).fetchone()
                return dict(row) if row else None

        return await asyncio.get_event_loop().run_in_executor(self.pool, db_operation)

    async def claim_confetti_ball(self, ball_id: str, user_id: str, points: float) -> bool:
        def db_operation():
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            try:
                cursor.execute("BEGIN EXCLUSIVE TRANSACTION")
            
                ball = cursor.execute("""
                    SELECT total_points, max_claims, claimed_count 
                    FROM confetti_balls 
                    WHERE ball_id = ? AND is_active = TRUE
                """, (ball_id,)).fetchone()
            
                if not ball:
                    cursor.execute("ROLLBACK")
                    return False
                
                total_points, max_claims, claimed_count = ball
            
                # Check if user already claimed
                already_claimed = cursor.execute("""
                    SELECT 1 FROM confetti_claims 
                    WHERE ball_id = ? AND user_id = ?
                """, (ball_id, user_id)).fetchone()
            
                if already_claimed:
                    cursor.execute("ROLLBACK")
                    return False
                
                if claimed_count >= max_claims:
                    cursor.execute("ROLLBACK")
                    return False
            
                claim_id = f"{ball_id}_{user_id}"
                cursor.execute("""
                    INSERT INTO confetti_claims (claim_id, ball_id, user_id, points_claimed)
                    VALUES (?, ?, ?, ROUND(?, 8))
                """, (claim_id, ball_id, user_id, points))
            
                cursor.execute("""
                    UPDATE confetti_balls 
                    SET claimed_count = claimed_count + 1,
                        is_active = CASE 
                            WHEN claimed_count + 1 >= max_claims THEN FALSE 
                            ELSE TRUE 
                        END
                    WHERE ball_id = ?
                """, (ball_id,))
            
                cursor.execute("""
                    INSERT INTO user_points (user_id, username, points)
                    VALUES (?, 'Unknown', ROUND(?, 8))
                    ON CONFLICT(user_id) DO UPDATE SET
                        points = ROUND(points + ?, 8)
                """, (user_id, points, points))
            
                conn.commit()
                return True
            
            except Exception as e:
                cursor.execute("ROLLBACK")
                logging.error(f"Error claiming confetti ball: {str(e)}")
                return False
            finally:
                conn.close()

        return await asyncio.get_event_loop().run_in_executor(self.pool, db_operation)

    async def get_confetti_claims(self, ball_id: str) -> List[Dict]:
        def db_operation():
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute("""
                    SELECT user_id, points_claimed, claimed_at
                    FROM confetti_claims
                    WHERE ball_id = ?
                    ORDER BY claimed_at ASC
                """, (ball_id,)).fetchall()
                return [dict(row) for row in rows]

        return await asyncio.get_event_loop().run_in_executor(self.pool, db_operation)
    

    async def create_confetti_trap(self, trap_id: str, creator_id: str, max_claims: int, message: str, channel_id: str) -> None:
        def db_operation():
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO confetti_traps 
                    (trap_id, creator_id, max_claims, message, channel_id)
                    VALUES (?, ?, ?, ?, ?)
                """, (trap_id, creator_id, max_claims, message, channel_id))
                conn.commit()

        await asyncio.get_event_loop().run_in_executor(self.pool, db_operation)

    async def get_confetti_trap(self, trap_id: str) -> Optional[Dict]:
        def db_operation():
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute("""
                    SELECT * FROM confetti_traps 
                    WHERE trap_id = ? AND is_active = TRUE
                """, (trap_id,)).fetchone()
                return dict(row) if row else None

        return await asyncio.get_event_loop().run_in_executor(self.pool, db_operation)

    async def claim_confetti_trap(self, trap_id: str, user_id: str, creator_id: str) -> Tuple[bool, float]:
        def db_operation():
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            try:
                cursor.execute("BEGIN EXCLUSIVE TRANSACTION")
            
                # Get trap details
                trap = cursor.execute("""
                    SELECT max_claims, claimed_count 
                    FROM confetti_traps 
                    WHERE trap_id = ? AND is_active = TRUE
                """, (trap_id,)).fetchone()
            
                if not trap:
                    cursor.execute("ROLLBACK")
                    return False, 0
            
                max_claims, claimed_count = trap
            
                # Check if user already claimed
                already_claimed = cursor.execute("""
                    SELECT 1 FROM confetti_trap_claims 
                    WHERE trap_id = ? AND user_id = ?
                """, (trap_id, user_id)).fetchone()
            
                if already_claimed:
                    cursor.execute("ROLLBACK")
                    return False, 0
            
                if claimed_count >= max_claims:
                    cursor.execute("ROLLBACK")
                    return False, 0

                # Get user's current points
                user_points = cursor.execute("""
                    SELECT points FROM user_points WHERE user_id = ?
                """, (user_id,)).fetchone()
            
                if not user_points or user_points[0] <= 0:
                    cursor.execute("ROLLBACK")
                    return False, 0

                # Calculate random points to steal (between 0.1% and 5% of user's points)
                current_points = float(user_points[0])
                steal_percentage = random.uniform(0.001, 0.05)  # Random percentage between 0.1% and 5%
                points_to_steal = min(current_points * steal_percentage, current_points)
                points_to_steal = round(points_to_steal, 8)
            
                if points_to_steal <= 0:
                    cursor.execute("ROLLBACK")
                    return False, 0

                # Record the trap claim
                claim_id = f"{trap_id}_{user_id}"
                cursor.execute("""
                    INSERT INTO confetti_trap_claims (claim_id, trap_id, user_id, points_lost)
                    VALUES (?, ?, ?, ROUND(?, 8))
                """, (claim_id, trap_id, user_id, points_to_steal))
            
                # Update trap claimed count
                cursor.execute("""
                    UPDATE confetti_traps 
                    SET claimed_count = claimed_count + 1,
                        is_active = CASE 
                            WHEN claimed_count + 1 >= max_claims THEN FALSE 
                            ELSE TRUE 
                        END
                    WHERE trap_id = ?
                """, (trap_id,))
            
                # Transfer points from victim to creator
                cursor.execute("""
                    UPDATE user_points 
                    SET points = ROUND(points - ?, 8)
                    WHERE user_id = ?
                """, (points_to_steal, user_id))
            
                cursor.execute("""
                    UPDATE user_points 
                    SET points = ROUND(points + ?, 8)
                    WHERE user_id = ?
                """, (points_to_steal, creator_id))
            
                conn.commit()
                return True, points_to_steal
            
            except Exception as e:
                cursor.execute("ROLLBACK")
                logging.error(f"Error claiming confetti trap: {str(e)}")
                return False, 0
            finally:
                conn.close()

        return await asyncio.get_event_loop().run_in_executor(self.pool, db_operation)

    async def get_confetti_trap_claims(self, trap_id: str) -> List[Dict]:
        def db_operation():
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute("""
                    SELECT user_id, points_lost, claimed_at
                    FROM confetti_trap_claims
                    WHERE trap_id = ?
                    ORDER BY points_lost DESC
                """, (trap_id,)).fetchall()
                return [dict(row) for row in rows]

        return await asyncio.get_event_loop().run_in_executor(self.pool, db_operation)
    
    async def create_vote(
        self, 
        vote_id: str,
        creator_id: str,
        target_user_id: str,
        description: str,
        options: List[Dict[str, any]],
        expires_in_days: int = 7
    ) -> None:
        def db_operation():
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute("BEGIN EXCLUSIVE TRANSACTION")
                
                    expires_at = datetime.now() + timedelta(days=expires_in_days)
                
                    cursor.execute("""
                        INSERT INTO votes 
                        (vote_id, creator_id, target_user_id, description, expires_at)
                        VALUES (?, ?, ?, ?, ?)
                    """, (vote_id, creator_id, target_user_id, description, expires_at))
                    
                    
                    print("Options received:", options)
                    logging.error(f"Options received: {options}")

                    for option in options:
                        option_id = f"{vote_id}_{option['index']}"

                        if 'option_text' not in option:
                            logging.error(f"Missing 'option_text' in option: {option}")
                            raise ValueError(f"Missing 'option_text' in option: {option}")
                    
                        if 'points' not in option:
                            logging.error(f"Missing 'points' in option: {option}")
                            raise ValueError(f"Missing 'points' in option: {option}")
                        
                        cursor.execute("""
                            INSERT INTO vote_options 
                            (option_id, vote_id, option_text, points)
                            VALUES (?, ?, ?, ROUND(?, 8))
                        """, (option_id, vote_id, option['text'], option['points']))
                
                    conn.commit()
                except Exception as e:
                    cursor.execute("ROLLBACK")
                    logging.error(f"Error in create_vote: {str(e)}")
                    raise e

        await asyncio.get_event_loop().run_in_executor(self.pool, db_operation)

    async def get_vote(self, vote_id: str) -> Optional[Dict]:
        def db_operation():
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
            
                vote = conn.execute("""
                    SELECT * FROM votes 
                    WHERE vote_id = ? 
                    AND is_active = TRUE
                    AND datetime('now') < datetime(expires_at)
                """, (vote_id,)).fetchone()
            
                if not vote:
                    return None
                
                options = conn.execute("""
                    SELECT * FROM vote_options
                    WHERE vote_id = ?
                    ORDER BY option_id
                """, (vote_id,)).fetchall()
            
                vote_dict = dict(vote)
                vote_dict['options'] = [dict(opt) for opt in options]
                return vote_dict

        return await asyncio.get_event_loop().run_in_executor(self.pool, db_operation)

    async def record_vote(
        self,
        vote_id: str,
        option_id: str,
        voter_id: str
    ) -> Tuple[bool, float]:

        def db_operation():
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            try:
                cursor.execute("BEGIN EXCLUSIVE TRANSACTION")
            

                vote = cursor.execute("""
                    SELECT target_user_id FROM votes
                    WHERE vote_id = ? 
                    AND is_active = TRUE
                    AND datetime('now') < datetime(expires_at)
                """, (vote_id,)).fetchone()
            
                if not vote:
                    cursor.execute("ROLLBACK")
                    return False, 0
                

                existing_vote = cursor.execute("""
                    SELECT 1 FROM vote_records
                    WHERE vote_id = ? AND voter_id = ?
                """, (vote_id, voter_id)).fetchone()
            
                if existing_vote:
                    cursor.execute("ROLLBACK")
                    return False, 0
                

                option = cursor.execute("""
                    SELECT points FROM vote_options
                    WHERE option_id = ? AND vote_id = ?
                """, (option_id, vote_id)).fetchone()
            
                if not option:
                    cursor.execute("ROLLBACK")
                    return False, 0
                
                points = float(option[0])
            

                record_id = f"{vote_id}_{voter_id}"
                cursor.execute("""
                    INSERT INTO vote_records (record_id, vote_id, option_id, voter_id)
                    VALUES (?, ?, ?, ?)
                """, (record_id, vote_id, option_id, voter_id))
            

                cursor.execute("""
                    UPDATE vote_options
                    SET votes_count = votes_count + 1
                    WHERE option_id = ?
                """, (option_id,))
            

                cursor.execute("""
                    INSERT INTO user_points (user_id, username, points)
                    VALUES (?, 'Unknown', ROUND(?, 8))
                    ON CONFLICT(user_id) DO UPDATE SET
                        points = ROUND(points + ?, 8),
                        last_updated = CURRENT_TIMESTAMP
                """, (vote[0], points, points))
            
                conn.commit()
                return True, points
            
            except Exception as e:
                cursor.execute("ROLLBACK")
                raise e
            finally:
                conn.close()
            
        return await asyncio.get_event_loop().run_in_executor(self.pool, db_operation)

    async def get_vote_results(self, vote_id: str) -> Optional[Dict]:
        def db_operation():
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
            
                vote = conn.execute("""
                    SELECT v.*, 
                           COUNT(DISTINCT vr.voter_id) as total_votes,
                           SUM(vo.points * vo.votes_count) as total_points_awarded
                    FROM votes v
                    LEFT JOIN vote_records vr ON v.vote_id = vr.vote_id
                    LEFT JOIN vote_options vo ON v.vote_id = vo.vote_id
                    WHERE v.vote_id = ?
                    GROUP BY v.vote_id
                """, (vote_id,)).fetchone()
            
                if not vote:
                    return None
                
                options = conn.execute("""
                    SELECT * FROM vote_options
                    WHERE vote_id = ?
                    ORDER BY votes_count DESC
                """, (vote_id,)).fetchall()
            
                result = dict(vote)
                result['options'] = [dict(opt) for opt in options]
                return result
            
        return await asyncio.get_event_loop().run_in_executor(self.pool, db_operation)


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pool.shutdown(wait=True)
        self.conn.close()