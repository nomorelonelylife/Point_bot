import time
import asyncio
import tweepy
import aiohttp
from typing import Dict, Optional
from datetime import datetime

class TwitterService:
    def __init__(self, bearer_token: str):
        self.client = tweepy.Client(bearer_token=bearer_token)
        self.last_request_time = 0
        self.min_request_interval = 1.0
        self.rate_limit_reset = 1200
        self.request_timeout = 30

    async def rate_limited_request(self, tweet_id: str) -> Optional[tweepy.Response]:
        now = time.time()
        time_since_last = now - self.last_request_time
        
        if time_since_last < self.min_request_interval:
            await asyncio.sleep(self.min_request_interval - time_since_last)
        
        try:
            timeout = aiohttp.ClientTimeout(total=self.request_timeout)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                tweet = await asyncio.wait_for(
                    self.client.get_tweet(
                        tweet_id,
                        tweet_fields=['public_metrics']
                    ),
                    timeout=self.request_timeout
                )
                self.last_request_time = time.time()
                return tweet

        except tweepy.TooManyRequests:
            print(f"Rate limit exceeded, waiting {self.rate_limit_reset} seconds")
            await asyncio.sleep(self.rate_limit_reset)
            return await self.rate_limited_request(tweet_id)
            
        except asyncio.TimeoutError:
            print(f"Request timeout for tweet {tweet_id}")
            return None
            
        except Exception as e:
            print(f"Error fetching tweet {tweet_id}: {str(e)}")
            return None

    async def calculate_points(self, tweet_id: str, weights: Dict[str, float]) -> int:
        try:
            tweet = await self.rate_limited_request(tweet_id)
            if not tweet or not tweet.data:
                return 0
                
            metrics = tweet.data.public_metrics
            return int(
                (metrics['reply_count'] * weights['reply']) +
                (metrics['retweet_count'] * weights['retweet']) +
                (metrics['like_count'] * weights['like'])
            )
            
        except Exception as e:
            print(f"Error calculating points for tweet {tweet_id}: {str(e)}")
            return 0