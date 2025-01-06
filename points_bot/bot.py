import discord
from discord import app_commands
from discord.ext import tasks
from typing import Optional
import re
import logging
from collections import deque
import asyncio
import os
import shutil
import aiohttp
from datetime import datetime, timedelta
from .database import DatabaseService
from .twitter_service import TwitterService

class ErrorLogger:
    def __init__(self, max_logs=3):
        self.errors = deque(maxlen=max_logs)
        
    def log_error(self, error: Exception, context: str):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.errors.append({
            'timestamp': timestamp,
            'error': str(error),
            'type': type(error).__name__,
            'context': context
        })

class PointsBot(discord.Client):
    def __init__(
        self, 
        twitter_token: str, 
        channel_id: str, 
        og_role_id: str,
        heartbeat_url: Optional[str] = None,
        db_path: Optional[str] = None
    ):
        intents = discord.Intents.default()
        intents.members = True
        super().__init__(intents=intents)
        
        self.tree = app_commands.CommandTree(self)
        self.db = DatabaseService(db_path or './points.db')
        self.twitter = TwitterService(twitter_token)
        self.channel_id = int(channel_id)
        self.og_role_id = int(og_role_id)
        self.heartbeat_url = heartbeat_url
        self.heartbeat_session = None
        self.error_logger = ErrorLogger()

    def validate_tweet_url(self, url: str) -> Optional[str]:
        patterns = [
            r'https?://(?:www\.)?twitter\.com/\w+/status/(\d+)',
            r'https?://(?:www\.)?x\.com/\w+/status/(\d+)'
        ]
        
        for pattern in patterns:
            if match := re.match(pattern, url):
                return match.group(1)
        return None

    async def setup_hook(self):
        self.heartbeat_session = aiohttp.ClientSession()
        await self.register_commands()
        self.check_tweets.start()
        self.backup_database.start()
        self.send_heartbeat.start()

    async def register_commands(self):
        @self.tree.command(name="addtweet", description="Add tweet to monitor")
        @app_commands.checks.has_permissions(administrator=True)
        @app_commands.guilds(interaction.guild_id)
        @app_commands.describe(
            url="Tweet URL",
            like_points="Points per like",
            retweet_points="Points per retweet",
            reply_points="Points per reply"
        )
        async def addtweet(
            interaction: discord.Interaction,
            url: str,
            like_points: Optional[float] = 1.0,
            retweet_points: Optional[float] = 2.0,
            reply_points: Optional[float] = 1.0
        ):
            try:
                if not interaction.guild:
                    if not interaction.response.is_done():
                        await interaction.response.send_message("This command can only be used in a server", ephemeral=True)
                    else:
                        await interaction.followup.send("This command can only be used in a server", ephemeral=True)
                    return

                tweet_id = self.validate_tweet_url(url)
                if not tweet_id:
                    if not interaction.response.is_done():
                        await interaction.response.send_message("Invalid tweet URL format", ephemeral=True)
                    else:
                        await interaction.followup.send("Invalid tweet URL format", ephemeral=True)
                    return

                if any(p < 0 for p in [like_points, retweet_points, reply_points]):
                    if not interaction.response.is_done():
                        await interaction.response.send_message("Points values cannot be negative", ephemeral=True)
                    else:
                        await interaction.followup.send("Points values cannot be negative", ephemeral=True)
                    return

                self.db.add_monitored_tweet(tweet_id, {
                    'like': like_points,
                    'retweet': retweet_points,
                    'reply': reply_points
                })
        
                if not interaction.response.is_done():
                    await interaction.response.send_message("Tweet added!", ephemeral=True)
                else:
                    await interaction.followup.send("Tweet added!", ephemeral=True)
            
            except Exception as e:
                self.error_logger.log_error(e, "addtweet command")
                if not interaction.response.is_done():
                    await interaction.response.send_message("An error occurred while adding the tweet", ephemeral=True)
                else:
                    await interaction.followup.send("An error occurred while adding the tweet", ephemeral=True)




        @self.tree.command(name="points", description="Check your points")
        @app_commands.guilds(interaction.guild_id)
        async def points(interaction: discord.Interaction):
            try:
                points = self.db.get_points(str(interaction.user.id))
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        f"You have {points} points",
                        ephemeral=True
                    )
                else:
                    await interaction.followup.send(
                        f"You have {points} points",
                        ephemeral=True
                    )
            except Exception as e:
                self.error_logger.log_error(e, "points command")
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        "An error occurred while fetching points",
                        ephemeral=True
                    )
                else:
                    await interaction.followup.send(
                        "An error occurred while fetching points",
                        ephemeral=True
                    )

        @self.tree.command(name="activeposts", description="View monitored posts")
        @app_commands.checks.has_permissions(administrator=True)
        @app_commands.guilds(interaction.guild_id)
        async def activeposts(interaction: discord.Interaction):
            try:
                tweets = self.db.get_active_tweets()
                content = '\n'.join(
                    f"https://twitter.com/x/status/{tweet['id']}"
                    for tweet in tweets
                )
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        content or "No active tweets",
                        ephemeral=True
                    )
                else:
                    await interaction.followup.send(
                        content or "No active tweets",
                        ephemeral=True
                    )
            except Exception as e:
                self.error_logger.log_error(e, "activeposts command")
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        "An error occurred while fetching active posts",
                        ephemeral=True
                    )
                else:
                    await interaction.followup.send(
                        "An error occurred while fetching active posts",
                        ephemeral=True
                    )



        await self.tree.sync()

    @tasks.loop(hours=24)
    async def check_tweets(self):
        try:
            tweets = self.db.get_active_tweets()
            guild = self.guilds[0] if self.guilds else None
            channel = self.get_channel(self.channel_id)
            
            if not guild or not isinstance(channel, discord.TextChannel):
                raise ValueError("Invalid guild or channel configuration")

            og_role = discord.utils.get(guild.roles, id=self.og_role_id)
            if not og_role:
                raise ValueError("OG role not found")

            for tweet in tweets:
                points = await self.twitter.calculate_points(
                    tweet['id'],
                    tweet['points']
                )
                
                if points <= 0:
                    continue

                og_members = [m for m in guild.members if og_role in m.roles]
                for member in og_members:
                    self.db.update_points(
                        str(member.id),
                        member.name,
                        points
                    )

                await channel.send(
                    f"🎉 Points Update:\n"
                    f"Tweet: https://twitter.com/x/status/{tweet['id']}\n"
                    f"Each OG member received {points} points!"
                )
                
        except Exception as e:
            self.error_logger.log_error(e, "check_tweets task")

    @tasks.loop(hours=24)
    async def backup_database(self):
       try:
           timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
           backup_dir = './backup'
           backup_path = os.path.join(backup_dir, f'points_{timestamp}.db')
       
           if os.path.exists(backup_dir):
               stat = os.stat(backup_dir)
               if stat.st_mode & 0o777 != 0o700:
                   os.chmod(backup_dir, 0o700)
           else:
               os.makedirs(backup_dir, mode=0o700)
           
           self.db.backup_database(backup_path)
           os.chmod(backup_path, 0o600)
       
           cutoff = (datetime.now() - timedelta(days=7))
           for f in os.listdir(backup_dir):
               if f.startswith('points_') and f.endswith('.db'):
                   file_path = os.path.join(backup_dir, f)
                   if datetime.fromtimestamp(os.path.getctime(file_path)) < cutoff:
                       os.remove(file_path)
                   
       except Exception as e:
           self.error_logger.log_error(e, "database backup")

    @tasks.loop(minutes=5)
    async def send_heartbeat(self):
        if not self.heartbeat_url:
            return
            
        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with self.heartbeat_session.get(self.heartbeat_url, timeout=timeout) as resp:
                if resp.status != 200:
                    raise ValueError(f"Heartbeat failed with status {resp.status}")
        except Exception as e:
            self.error_logger.log_error(e, "heartbeat")
            await asyncio.sleep(5)  # Backoff before next attempt

    @check_tweets.before_loop
    @backup_database.before_loop
    @send_heartbeat.before_loop
    async def before_tasks(self):
        await self.wait_until_ready()

    async def close(self):
        if self.heartbeat_session:
            await self.heartbeat_session.close()
        if hasattr(self, 'db'):
            self.db.conn.close()
        await super().close()

    async def on_ready(self):
        print(f'Logged in as {self.user} (ID: {self.user.id})')
        print('------')
