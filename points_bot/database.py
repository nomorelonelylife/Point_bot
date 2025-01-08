# points_bot/database.py
import redis
import json
from typing import List, Dict, Optional
from datetime import datetime
import logging
import os

class DatabaseService:
    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        self.redis = redis.from_url(
            redis_url,
            decode_responses=True
        )
        self.KEY_PREFIX = {
            'user_points': 'user:points:',
            'user_info': 'user:info:',
            'active_tweets': 'tweets:active',
            'tweet_info': 'tweet:info:',
        }

    def add_monitored_tweet(self, tweet_id: str, points: Dict[str, float]) -> None:
        try:
            active_tweets = self.redis.smembers(self.KEY_PREFIX['active_tweets'])
            
            if len(active_tweets) >= 3:
                oldest_tweet = min(active_tweets)
                self.redis.srem(self.KEY_PREFIX['active_tweets'], oldest_tweet)
                self.redis.delete(f"{self.KEY_PREFIX['tweet_info']}{oldest_tweet}")

            tweet_data = {
                'like_points': points['like'],
                'retweet_points': points['retweet'],
                'reply_points': points['reply'],
                'created_at': datetime.now().isoformat()
            }
            
            self.redis.sadd(self.KEY_PREFIX['active_tweets'], tweet_id)
            self.redis.hset(
                f"{self.KEY_PREFIX['tweet_info']}{tweet_id}",
                mapping=tweet_data
            )

        except Exception as e:
            logging.error(f"Error adding tweet {tweet_id}: {str(e)}")
            raise

    def get_active_tweets(self) -> List[Dict]:
        try:
            active_tweets = self.redis.smembers(self.KEY_PREFIX['active_tweets'])
            result = []
            
            for tweet_id in active_tweets:
                tweet_data = self.redis.hgetall(f"{self.KEY_PREFIX['tweet_info']}{tweet_id}")
                if tweet_data:
                    result.append({
                        'id': tweet_id,
                        'points': {
                            'like': float(tweet_data['like_points']),
                            'retweet': float(tweet_data['retweet_points']),
                            'reply': float(tweet_data['reply_points'])
                        }
                    })
            
            return sorted(result, key=lambda x: x.get('created_at', ''), reverse=True)

        except Exception as e:
            logging.error(f"Error getting active tweets: {str(e)}")
            raise

    def update_points(self, user_id: str, username: str, points: int) -> None:
        try:
            pipeline = self.redis.pipeline()
            
            pipeline.hincrby(f"{self.KEY_PREFIX['user_points']}{user_id}", "points", points)
            
            pipeline.hset(
                f"{self.KEY_PREFIX['user_info']}{user_id}",
                mapping={
                    'username': username,
                    'last_updated': datetime.now().isoformat()
                }
            )
            
            pipeline.execute()

        except Exception as e:
            logging.error(f"Error updating points for user {user_id}: {str(e)}")
            raise

    def get_points(self, user_id: str) -> int:
        try:
            points = self.redis.hget(f"{self.KEY_PREFIX['user_points']}{user_id}", "points")
            return int(points) if points else 0
        except Exception as e:
            logging.error(f"Error getting points for user {user_id}: {str(e)}")
            raise

    def remove_monitored_tweet(self, tweet_id: str) -> bool:
        try:
            pipeline = self.redis.pipeline()
            
            pipeline.delete(f"{self.KEY_PREFIX['tweet_info']}{tweet_id}")
            pipeline.srem(self.KEY_PREFIX['active_tweets'], tweet_id)
            
            results = pipeline.execute()
            return any(result for result in results)

        except Exception as e:
            logging.error(f"Error removing tweet {tweet_id}: {str(e)}")
            raise

    def backup_database(self, backup_dir: str = './backup') -> None:
        try:
            os.makedirs(backup_dir, mode=0o700, exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_path = os.path.join(backup_dir, f'points_{timestamp}.json')
            
            backup_data = {
                'user_points': {},
                'user_info': {},
                'active_tweets': list(self.redis.smembers(self.KEY_PREFIX['active_tweets'])),
                'tweet_info': {}
            }
            
            for key in self.redis.scan_iter(f"{self.KEY_PREFIX['user_points']}*"):
                user_id = key.split(':')[-1]
                backup_data['user_points'][user_id] = self.redis.hgetall(key)
                info_key = f"{self.KEY_PREFIX['user_info']}{user_id}"
                backup_data['user_info'][user_id] = self.redis.hgetall(info_key)
            
            for tweet_id in backup_data['active_tweets']:
                info_key = f"{self.KEY_PREFIX['tweet_info']}{tweet_id}"
                backup_data['tweet_info'][tweet_id] = self.redis.hgetall(info_key)
            
            with open(backup_path, 'w', opener=lambda p,f: os.open(p, f, 0o600)) as f:
                json.dump(backup_data, f, indent=2)
                
        except Exception as e:
            logging.error(f"Backup failed: {str(e)}")
            raise

    def restore_from_backup(self, backup_path: str) -> None:
        try:
            with open(backup_path, 'r') as f:
                backup_data = json.load(f)
            
            pipeline = self.redis.pipeline()
            
            for user_id, points_data in backup_data['user_points'].items():
                pipeline.hset(f"{self.KEY_PREFIX['user_points']}{user_id}", mapping=points_data)
            
            for user_id, info_data in backup_data['user_info'].items():
                pipeline.hset(f"{self.KEY_PREFIX['user_info']}{user_id}", mapping=info_data)
            
            pipeline.delete(self.KEY_PREFIX['active_tweets'])
            if backup_data['active_tweets']:
                pipeline.sadd(self.KEY_PREFIX['active_tweets'], *backup_data['active_tweets'])
            
            for tweet_id, tweet_data in backup_data['tweet_info'].items():
                pipeline.hset(f"{self.KEY_PREFIX['tweet_info']}{tweet_id}", mapping=tweet_data)
            
            pipeline.execute()
            
        except Exception as e:
            logging.error(f"Restore failed: {str(e)}")
            raise

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.redis.close()