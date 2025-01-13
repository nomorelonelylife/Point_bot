import sqlite3
import os
import asyncio
import random
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
import logging
from discord.ext import tasks

class DatabaseService:
    def __init__(self, db_path: str = "./points.db", max_connections: int = 60):
        try:
            self.db_path = db_path
            self.pool = ThreadPoolExecutor(max_workers=max_connections)
            self.conn = None
           # asyncio.create_task(self.async_initialize())

        except Exception as e:
            logging.critical(f"Database initialization failed: {str(e)}")
            if hasattr(self, 'pool'):
                self.pool.shutdown(wait=False)
            raise
    
    
    async def async_initialize(self):
        """
        Asynchronously initialize the database with comprehensive error handling
        and setup operations.
        """
        def _initialize():
            try:
                # Establish database connection
                conn = sqlite3.connect(
                    self.db_path,
                    timeout=30.0,
                    isolation_level='EXCLUSIVE'
                )
                conn.execute('PRAGMA journal_mode=WAL')
                conn.row_factory = sqlite3.Row

                # Database table initialization scripts
                conn.executescript("""
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
                        expires_at TIMESTAMP NOT NULL,
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
                        expires_at TIMESTAMP NOT NULL,
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

                # Add indexes
                conn.executescript("""
                    CREATE INDEX IF NOT EXISTS idx_confetti_claims_claimed_at 
                    ON confetti_claims(claimed_at);

                    CREATE INDEX IF NOT EXISTS idx_confetti_trap_claims_claimed_at 
                    ON confetti_trap_claims(claimed_at);

                    CREATE INDEX IF NOT EXISTS idx_user_points_last_updated 
                    ON user_points(last_updated);
                """)

                conn.commit()
                return conn

            except sqlite3.Error as e:
                logging.critical(f"SQLite initialization error: {e}")
                raise


        try:
            # Execute initialization in thread pool
            self.conn = await asyncio.get_event_loop().run_in_executor(
                self.pool, 
                _initialize
            )
        
            logging.info("Database initialized successfully")

            index_result = await self.add_necessary_indexes()

            if not index_result:
                logging.warning("Some indexes could not be created")

            if hasattr(self, 'schedule_cleanup'):
                self.schedule_cleanup.start()

                return self.conn

        except Exception as e:
            logging.critical(f"Async database initialization failed: {e}")
        
            # Attempt to close connection if it exists
            if hasattr(self, 'conn') and self.conn:
                try:
                    self.conn.close()
                except:
                    pass
        
            # Shutdown thread pool
            if hasattr(self, 'pool'):
                self.pool.shutdown(wait=False)
        
            raise
    
    def initialize(self):
        logging.warning("Synchronous initialization is deprecated. Use async_initialize() instead.")

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

    async def backup_database(self, backup_path: str, is_pre_cleanup: bool = False) -> None:
        """Create a backup of the database with essential safety measures.
        Args:
            backup_path: The target path for the backup file
            is_pre_cleanup: If True, this is a pre-cleanup backup
        """
        def db_operation():
            try:
                backup_dir = os.path.dirname(backup_path)
                os.makedirs(backup_dir, mode=0o700, exist_ok=True)
            
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                if is_pre_cleanup:
                    final_path = os.path.join(backup_dir, f'pre_cleanup_{timestamp}.db')
                else:
                    final_path = os.path.join(backup_dir, f'daily_{timestamp}.db')

                # Create WAL checkpoint to ensure data consistency
                with sqlite3.connect(self.db_path) as checkpoint_conn:
                    checkpoint_conn.execute('PRAGMA wal_checkpoint(FULL)')
            
                # Create backup with secure permissions
                with sqlite3.connect(self.db_path) as conn:
                    with open(final_path, 'wb', opener=lambda p,f: os.open(p, f, 0o600)) as f:
                        for line in conn.iterdump():
                            f.write(f'{line}\n'.encode('utf-8'))
            
                logging.info(f"{'Pre-cleanup' if is_pre_cleanup else 'Daily'} backup completed: {final_path}")
                    
            except Exception as e:
                error_msg = f"Backup failed: {str(e)}"
                logging.error(error_msg)
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

    async def create_confetti_ball(
        self, 
        ball_id: str, 
        creator_id: str, 
        total_points: float, 
        max_claims: int, 
        message: str, 
        channel_id: str,
        expires_at: Optional[datetime] = None
    ) -> None:
        def db_operation():
            with sqlite3.connect(self.db_path) as conn:
                # If no expiration provided, generate a random expiration between 1s and 30 minutes
                nonlocal expires_at
                if expires_at is None:
                    expires_at = datetime.now() + timedelta(
                       seconds=random.uniform(1, 5 * 60 * 6)
                    )
                
                conn.execute("""
                    INSERT INTO confetti_balls 
                    (ball_id, creator_id, total_points, max_claims, message, channel_id, expires_at)
                    VALUES (?, ?, ROUND(?, 8), ?, ?, ?, ?)
                """, (ball_id, creator_id, total_points, max_claims, message, channel_id, expires_at))
                conn.commit()

        await asyncio.get_event_loop().run_in_executor(self.pool, db_operation)

    async def get_confetti_ball(self, ball_id: str) -> Optional[Dict]:
        def db_operation():
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute("""
                    SELECT *, 
                           ROUND(total_points, 8) as total_points_rounded 
                    FROM confetti_balls 
                    WHERE ball_id = ? 
                    AND is_active = TRUE 
                    AND datetime('now') < datetime(expires_at)
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
    

    async def create_confetti_trap(
        self, 
        trap_id: str, 
        creator_id: str, 
        max_claims: int, 
        message: str, 
        channel_id: str,
        expires_at: Optional[datetime] = None
    ) -> None:
        def db_operation():
            with sqlite3.connect(self.db_path) as conn:
                # If no expiration provided, generate a random expiration between 1s and 30 minutes
                nonlocal expires_at
                if expires_at is None:
                    expires_at = datetime.now() + timedelta(
                        seconds=random.uniform(1, 5 * 60 * 6)
                    )
            
            conn.execute("""
                INSERT INTO confetti_traps 
                (trap_id, creator_id, max_claims, message, channel_id, expires_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (trap_id, creator_id, max_claims, message, channel_id, expires_at))
            conn.commit()

        await asyncio.get_event_loop().run_in_executor(self.pool, db_operation)

    async def get_confetti_trap(self, trap_id: str) -> Optional[Dict]:
        def db_operation():
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute("""
                    SELECT * FROM confetti_traps 
                    WHERE trap_id = ? 
                    AND is_active = TRUE 
                    AND datetime('now') < datetime(expires_at)
                """, (trap_id,)).fetchone()
                return dict(row) if row else None

        return await asyncio.get_event_loop().run_in_executor(self.pool, db_operation)

    async def claim_confetti_trap(self, trap_id: str, user_id: str, creator_id: str) -> Tuple[bool, float]:

        def db_operation():
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            try:
                cursor.execute("BEGIN EXCLUSIVE TRANSACTION")
            
                # Get trap details and check validity
                trap = cursor.execute("""
                    SELECT max_claims, claimed_count 
                    FROM confetti_traps 
                    WHERE trap_id = ? AND is_active = TRUE
                """, (trap_id,)).fetchone()
            
                if not trap:
                    cursor.execute("ROLLBACK")
                    return False, 0, None, None
            
                max_claims, claimed_count = trap
            
                # Check if user already claimed
                already_claimed = cursor.execute("""
                    SELECT 1 FROM confetti_trap_claims 
                    WHERE trap_id = ? AND user_id = ?
                """, (trap_id, user_id)).fetchone()
            
                if already_claimed:
                    cursor.execute("ROLLBACK")
                    return False, 0, None, None
            
                if claimed_count >= max_claims:
                    cursor.execute("ROLLBACK")
                    return False, 0, None, None

                # Get user's current points
                user_points = cursor.execute("""
                    SELECT points FROM user_points WHERE user_id = ?
                """, (user_id,)).fetchone()
            
                if not user_points or user_points[0] <= 0:
                    cursor.execute("ROLLBACK")
                    return False, 0, None, None

                # Calculate random points to steal (between 0.1% and 5% of user's points)
                current_points = float(user_points[0])
                steal_percentage = random.uniform(0.001, 0.05)  # Random percentage between 0.1% and 5%
                points_to_steal = min(current_points * steal_percentage, current_points)
                points_to_steal = round(points_to_steal, 8)
            
                if points_to_steal <= 0:
                    cursor.execute("ROLLBACK")
                    return False, 0, None, None


                # Get creator's current total trap earnings
                trap_earnings = cursor.execute("""
                    SELECT COALESCE(SUM(points_lost), 0) 
                    FROM confetti_trap_claims 
                    WHERE trap_id = ?
                """, (trap_id,)).fetchone()[0]

                # Get creator's current balance
                creator_balance = cursor.execute("""
                    SELECT points FROM user_points WHERE user_id = ?
                """, (creator_id,)).fetchone()

                creator_balance = float(creator_balance[0]) if creator_balance else 0
                total_earnings = float(trap_earnings) + points_to_steal

                # If creator has no balance or very small balance (less than 0.00000001)
                if creator_balance < 0.00000001:
                    # Deactivate the trap immediately
                    cursor.execute("""
                        UPDATE confetti_traps 
                        SET is_active = FALSE
                        WHERE trap_id = ?
                    """, (trap_id,))
                    conn.commit()
                    return False, 0, "NO_BALANCE", 0
                

                # Check if earnings exceed 2.33333 times the creator's original balance
                if total_earnings > creator_balance * 2.33333:
                    # Calculate penalty (0.01% to 3% of current balance)
                    penalty_percentage = random.uniform(0.0001, 0.03)
                    penalty = round(creator_balance * penalty_percentage, 8)
                
                    # Ensure penalty doesn't exceed available balance
                    penalty = min(penalty, creator_balance)
                
                    if penalty > 0:  # Only apply penalty if there's something to take
                        # Penalize creator
                        cursor.execute("""
                            UPDATE user_points 
                            SET points = ROUND(points - ?, 8)
                            WHERE user_id = ?
                        """, (penalty, creator_id))
                    # Deactivate the trap
                    cursor.execute("""
                        UPDATE confetti_traps 
                        SET is_active = FALSE
                        WHERE trap_id = ?
                    """, (trap_id,))

                    conn.commit()
                    return False, 0, "PENALTY", penalty
                

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
                return True, points_to_steal, None, None
            
            except Exception as e:
                cursor.execute("ROLLBACK")
                logging.error(f"Error claiming confetti trap: {str(e)}")
                return False, 0, None, None
            finally:
                conn.close()

        return await asyncio.get_event_loop().run_in_executor(self.pool, db_operation)
    
    async def get_and_process_expired_traps(self) -> List[Dict]:
        """Get and process expired traps in a single transaction"""
        def db_operation():
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            try:
                cursor.execute("BEGIN EXCLUSIVE TRANSACTION")
            
                # Get all expired traps
                expired_traps = cursor.execute("""
                    SELECT * FROM confetti_traps 
                    WHERE is_active = TRUE 
                    AND datetime('now') >= datetime(expires_at)
                """).fetchall()
            
                results = []
                for trap in expired_traps:
                    try:
                        # Get claims for this trap
                        claims = cursor.execute("""
                            SELECT user_id, points_lost
                            FROM confetti_trap_claims
                            WHERE trap_id = ?
                            ORDER BY claimed_at ASC
                        """, (trap['trap_id'],)).fetchall()
                    
                        # Mark trap as inactive
                        cursor.execute("""
                            UPDATE confetti_traps
                            SET is_active = FALSE
                            WHERE trap_id = ?
                        """, (trap['trap_id'],))
                    
                        results.append({
                            'trap_id': trap['trap_id'],
                            'creator_id': trap['creator_id'],
                            'channel_id': trap['channel_id'],
                            'claims': [{'user_id': c[0], 'points_lost': float(c[1])} for c in claims]
                        })
                    except Exception as e:
                        logging.error(f"Error processing trap {trap['trap_id']}: {str(e)}")
                        continue
            
                conn.commit()
                return results
            
            except Exception as e:
                cursor.execute("ROLLBACK")
                logging.error(f"Error in get_and_process_expired_traps: {str(e)}")
                return []
            finally:
                conn.close()

        return await asyncio.get_event_loop().run_in_executor(self.pool, db_operation)
    
    async def get_expired_confetti_balls(self) -> List[str]:
        """Get IDs of expired confetti balls that are still active"""
        def db_operation():
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                return [
                    row['ball_id'] for row in conn.execute("""
                        SELECT ball_id
                        FROM confetti_balls 
                        WHERE is_active = TRUE 
                        AND datetime('now') >= datetime(expires_at)
                    """).fetchall()
                ]

        return await asyncio.get_event_loop().run_in_executor(
            self.pool,
            db_operation
        )
    
    async def process_expired_confetti_ball(self, ball_id: str) -> Optional[Dict]:
        def db_operation():
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            try:
                cursor.execute("BEGIN EXCLUSIVE TRANSACTION")
        
                # Get ball details with error handling
                ball = cursor.execute("""
                    SELECT *, 
                           ROUND(total_points, 8) as total_points_rounded 
                    FROM confetti_balls 
                    WHERE ball_id = ? 
                    AND is_active = TRUE 
                    AND datetime('now') >= datetime(expires_at)
                """, (ball_id,)).fetchone()
        
                if not ball:
                    cursor.execute("ROLLBACK")
                    return None
            
                ball_dict = dict(ball)
            
                # Calculate unclaimed points
                claimed_points = cursor.execute("""
                    SELECT COALESCE(SUM(points_claimed), 0) as total_claimed
                    FROM confetti_claims
                    WHERE ball_id = ?
                """, (ball_id,)).fetchone()['total_claimed']
        
                unclaimed_points = ball_dict['total_points_rounded'] - float(claimed_points)
        
                if unclaimed_points > 0:
                    cursor.execute("""
                        UPDATE user_points 
                        SET points = ROUND(points + ?, 8)
                        WHERE user_id = ?
                    """, (unclaimed_points, ball_dict['creator_id']))
        
                # Get claims and mark ball inactive
                claims = cursor.execute("""
                    SELECT user_id, points_claimed
                    FROM confetti_claims
                    WHERE ball_id = ?
                    ORDER BY claimed_at ASC
                """, (ball_id,)).fetchall()
        
                cursor.execute("""
                    UPDATE confetti_balls
                    SET is_active = FALSE
                    WHERE ball_id = ?
                """, (ball_id,))
        
                conn.commit()
        
                return {
                    'ball_id': ball_id,
                    'creator_id': ball_dict['creator_id'],
                    'total_points': ball_dict['total_points_rounded'],
                    'unclaimed_points': unclaimed_points,
                    'claims': [{'user_id': c[0], 'points_claimed': float(c[1])} for c in claims],
                    'message': ball_dict['message']
                }
        
            except Exception as e:
                cursor.execute("ROLLBACK")
                logging.error(f"Error processing expired confetti ball: {str(e)}")
                return None
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
                

                    for option in options:

                        if not all(key in option for key in ['index', 'option_text', 'points']):
                            raise ValueError(f"Invalid option format: {option}")
                     

                        option_id = f"{vote_id}_{option['index']}"
                    

                        cursor.execute("""
                            INSERT INTO vote_options 
                            (option_id, vote_id, option_text, points)
                            VALUES (?, ?, ?, ROUND(?, 8))
                        """, (
                            option_id, 
                            vote_id, 
                            option['option_text'], 
                            option['points']
                        ))
                

                    conn.commit()
            
                except Exception as e:

                    cursor.execute("ROLLBACK")
                    logging.error(f"Error in create_vote: {str(e)}")
                    logging.error(f"Vote details - ID: {vote_id}, Creator: {creator_id}, Target: {target_user_id}")
                    logging.error(f"Options: {options}")
                    raise
    

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

    
    async def add_necessary_indexes(self):
        """
        Asynchronously add necessary indexes to the database
    
        Args:
            conn (sqlite3.Connection, optional): Existing database connection
        """
        def _add_indexes():
            try:
                
                if not self.conn:
                    logging.error("Database connection is not established")
                    return False
            
             
                index_scripts = [
                    """
                    CREATE INDEX IF NOT EXISTS idx_confetti_claims_claimed_at 
                    ON confetti_claims(claimed_at)
                    """,
                    """
                    CREATE INDEX IF NOT EXISTS idx_confetti_trap_claims_claimed_at 
                    ON confetti_trap_claims(claimed_at)
                    """,
                    """
                    CREATE INDEX IF NOT EXISTS idx_user_points_last_updated 
                    ON user_points(last_updated)
                    """,
                    """
                    CREATE INDEX IF NOT EXISTS idx_votes_expiry 
                    ON votes(is_active, expires_at)
                    """
                ]
                
                self.conn.execute("BEGIN TRANSACTION")
                
                for script in index_scripts:
                    try:
                        self.conn.execute(script)
                    except sqlite3.OperationalError as e:
                        logging.warning(f"Could not create index: {e}")
            
           
                self.conn.commit()
            
                logging.info("Successfully added database indexes")
                return True
            
            except Exception as e:
            
                try:
                    self.conn.rollback()
                except:
                    pass
            
                logging.error(f"Error adding indexes: {str(e)}")
                return False

    
        result = await asyncio.get_event_loop().run_in_executor(
            self.pool, 
            _add_indexes
        )
    
        return result


    async def cleanup_old_records(self):
        try:

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            await self.backup_database(
                os.path.join('./backup', f'points_{timestamp}.db'),
                is_pre_cleanup=True
            )
        
            def db_operation():
                conn = None 
                cursor = None
                try:
                    conn = sqlite3.connect(self.db_path)
                    cursor = conn.cursor()
                
                    try:

                        cursor.execute("BEGIN IMMEDIATE TRANSACTION")
                    

                        cleanup_operations = [
                            ("vote_records", """
                                DELETE FROM vote_records 
                                WHERE vote_id IN (
                                    SELECT vote_id 
                                    FROM votes 
                                    WHERE (is_active = FALSE OR datetime('now') > datetime(expires_at))
                                    AND datetime(expires_at) < datetime('now', '-14 days')
                                )
                            """),
                            ("vote_options", """
                                DELETE FROM vote_options
                                WHERE vote_id IN (
                                    SELECT vote_id 
                                    FROM votes 
                                    WHERE (is_active = FALSE OR datetime('now') > datetime(expires_at))
                                    AND datetime(expires_at) < datetime('now', '-14 days')
                                )
                            """),
                            ("votes", """
                                UPDATE votes 
                                SET is_active = FALSE 
                                WHERE datetime('now') > datetime(expires_at)
                                AND is_active = TRUE
                            """),
                            ("confetti_claims", """
                                DELETE FROM confetti_claims 
                                WHERE datetime(claimed_at) < datetime('now', '-7 days')
                            """),
                            ("confetti_balls", """
                                DELETE FROM confetti_balls 
                                WHERE is_active = TRUE 
                                AND datetime('now') > datetime(expires_at)
                            """),
                            ("confetti_traps", """
                                DELETE FROM confetti_traps 
                                WHERE is_active = TRUE 
                                AND datetime('now') > datetime(expires_at)
                            """)
                        ]
                    

                        for table, sql in cleanup_operations:
                            try:
                                cursor.execute(sql)
                                affected_rows = cursor.rowcount
                                logging.info(f"Cleaned {affected_rows} rows from {table}")
                            except Exception as e:
                                logging.error(f"Error cleaning {table}: {e}")
                                raise
                    

                        cursor.execute("COMMIT")
                        logging.info("Database cleanup transaction committed successfully")
                    

                        try:
                            cursor.execute("VACUUM")
                            logging.info("Database vacuum completed")
                        except Exception as e:
                            logging.warning(f"Vacuum failed (non-critical): {e}")

                        
                    except Exception as e:
                        if cursor:
                            cursor.execute("ROLLBACK")
                        logging.error(f"Error during cleanup transaction: {e}")
                        raise
                    
                except Exception as e:
                    logging.error(f"Critical error during cleanup: {e}")
                    raise
                
                finally:
                    if cursor:
                        cursor.close()
                    if conn:
                        conn.close()


            await asyncio.get_event_loop().run_in_executor(
                self.pool,
                db_operation
            )

        except Exception as e:
            logging.error(f"Failed to perform cleanup: {e}")

            return False
    
        return True


    async def cleanup_old_backups(self, keep_days=14):
        def db_operation():
            backup_dir = './backup'
            os.makedirs(backup_dir, exist_ok=True)
            current_time = datetime.now()
        
            for filename in os.listdir(backup_dir):
                if filename.startswith('points_') and filename.endswith('.db'):
                    try:

                        timestamp_str = filename[7:-3]  
                        file_time = datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S')
                    
                        if (current_time - file_time).days > keep_days:
                            os.remove(os.path.join(backup_dir, filename))
                            logging.info(f"Removed old backup: {filename}")
                    except Exception as e:
                        logging.error(f"Error cleaning backup {filename}: {e}")

        await asyncio.get_event_loop().run_in_executor(self.pool, db_operation)



    @tasks.loop(hours=24)
    async def schedule_cleanup(self):
        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            try:
                success_records = await self.cleanup_old_records()

                try:
                    await self.cleanup_old_backups(keep_days=14)
                except Exception as e:
                    logging.error(f"Backup cleanup failed: {e}")

                if success_records:
                    logging.info("Scheduled cleanup completed successfully")
                    break
                else:
                    retry_count += 1
                    wait_time = 2 ** retry_count
                    logging.warning(f"Cleanup attempt {retry_count} failed, retrying in {wait_time} seconds")
                    await asyncio.sleep(wait_time)
                
            except sqlite3.OperationalError as e:
                retry_count += 1
                wait_time = 2 ** retry_count
                logging.warning(f"Database locked, retry {retry_count} in {wait_time} seconds: {str(e)}")
                if retry_count == max_retries:
                    logging.error(f"Cleanup failed after {max_retries} attempts")
                    break
                await asyncio.sleep(wait_time)
            
            except Exception as e:
                logging.error(f"Unhandled error during cleanup: {str(e)}")
                break

        if retry_count == max_retries:
            logging.critical("All cleanup attempts failed. Manual intervention may be required.")

    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if hasattr(self, 'schedule_cleanup'):
            self.schedule_cleanup.cancel()
        self.pool.shutdown(wait=True)
        if self.conn:
            self.conn.close()

    async def close(self):
        if hasattr(self, 'schedule_cleanup'):
            self.schedule_cleanup.cancel()
    
        if self.pool:
            self.pool.shutdown(wait=True)
    
        if self.conn:
            self.conn.close()
            self.conn = None

    def __enter__(self):
        return self

