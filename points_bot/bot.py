import discord
from discord import app_commands
from discord.ext import tasks
from typing import Optional, List, Dict
import re
import logging
import random
import csv
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
        self.error_logger = ErrorLogger()
        os.makedirs('output', exist_ok=True)


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
        print("Setup hook started...")
        logging.info("Setup hook started...")
    
        try:

            await self.register_commands()
            print("Commands registered in setup_hook")
            logging.info("Commands registered in setup_hook")
        
            self.check_tweets.start()
            self.backup_database.start()
        
            print("Background tasks started.")
            logging.info("Background tasks started.")
        except Exception as e:
            print(f"Error in setup_hook: {e}")
            logging.error(f"Error in setup_hook: {e}")
            raise

    async def register_commands(self):
        @self.tree.command(name="addtweet", description="Add tweet to monitor")
        @app_commands.checks.has_permissions(administrator=True)
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
                    await interaction.response.send_message(
                        "This command can only be used in a server",
                        ephemeral=True
                    )
                    return

                tweet_id = self.validate_tweet_url(url)
                if not tweet_id:
                    await interaction.response.send_message(
                        "Invalid tweet URL format",
                        ephemeral=True
                    )
                    return

                if any(p < 0 for p in [like_points, retweet_points, reply_points]):
                    await interaction.response.send_message(
                        "Points values cannot be negative",
                        ephemeral=True
                    )
                    return

                await self.db.add_monitored_tweet(tweet_id, {
                    'like': like_points,
                    'retweet': retweet_points,
                    'reply': reply_points
                })

                await interaction.response.send_message(
                    "Tweet added!",
                    ephemeral=True
                )

            except Exception as e:
                self.error_logger.log_error(e, "addtweet command")
                await interaction.response.send_message(
                    "An error occurred while adding the tweet",
                    ephemeral=True
                )
        

        @self.tree.command(name="removetweet", description="Remove a monitored tweet")
        @app_commands.checks.has_permissions(administrator=True)
        @app_commands.describe(
            tweet_id="Tweet ID or full URL to remove from monitoring"
        )
        async def removetweet(
            interaction: discord.Interaction,
            tweet_id: str
        ):
            try:
                # Check if input is a URL
                if "twitter.com" in tweet_id or "x.com" in tweet_id:
                    extracted_id = self.validate_tweet_url(tweet_id)
                    if not extracted_id:
                        await interaction.response.send_message(
                            "Invalid tweet URL format",
                            ephemeral=True
                        )
                        return
                    tweet_id = extracted_id
        
                # Remove from database
                if await self.db.remove_monitored_tweet(tweet_id):
                    await interaction.response.send_message(
                        f"Tweet {tweet_id} has been removed from monitoring",
                        ephemeral=True
                    )
                else:
                    await interaction.response.send_message(
                        "Tweet not found in monitored list",
                        ephemeral=True
                    )

            except Exception as e:
                self.error_logger.log_error(e, "removetweet command")
                await interaction.response.send_message(
                    "An error occurred while removing the tweet",
                    ephemeral=True
                )



        @self.tree.command(name="points", description="Check your points")
        async def points(interaction: discord.Interaction):
            try:
                points = await self.db.get_points(str(interaction.user.id))
                formatted_points = f"{points:.8f}"
                await interaction.response.send_message(
                    f"You have {formatted_points} points",
                    ephemeral=True
                )
            except Exception as e:
                self.error_logger.log_error(e, "points command")
                await interaction.response.send_message(
                    "An error occurred while fetching points",
                    ephemeral=True
               )


        @self.tree.command(name="activeposts", description="View monitored posts")
        @app_commands.checks.has_permissions(administrator=True)
        async def activeposts(interaction: discord.Interaction):
            try:
                tweets = await self.db.get_active_tweets()
                content = '\n'.join(
                    f"https://twitter.com/x/status/{tweet['id']}"
                    for tweet in tweets
                )
                await interaction.response.send_message(
                    content or "No active tweets",
                    ephemeral=True
                )

            except Exception as e:
                self.error_logger.log_error(e, "activeposts command")
                await interaction.response.send_message(
                    "An error occurred while fetching active posts",
                    ephemeral=True
                )

        @self.tree.command(name="checktweets", description="Manually check tweets for points (Admin only)")
        @app_commands.checks.has_permissions(administrator=True)
        async def checktweets(interaction: discord.Interaction):
            try:
                await interaction.response.defer(ephemeral=True)
                
                points_updates = await self._process_tweets()
                
                if points_updates:
                    summary = "\n".join([
                        f"Tweet {update['tweet_id']}: {update['points']:.8f} points awarded"
                        for update in points_updates
                    ])
                    await interaction.followup.send(
                        f"Manual check completed!\n{summary}",
                        ephemeral=True
                    )
                else:
                    await interaction.followup.send(
                        "Check completed. No points were awarded.",
                        ephemeral=True
                    )

            except Exception as e:
                self.error_logger.log_error(e, "checktweets command")
                await interaction.followup.send(
                    "An error occurred while checking tweets",
                    ephemeral=True
                )
        

        @self.tree.command(name="tip", description="Tip points to another user")
        @app_commands.describe(
            user="User to tip points to",
            amount="Amount of points to tip (can be decimal, up to 8 decimal places)"
        )
        async def tip(
            interaction: discord.Interaction,
            user: discord.User,
            amount: float
        ):
            try:
                amount = round(amount, 8)

                if amount <= 0:
                    await interaction.response.send_message(
                        "Tip amount must be positive",
                        ephemeral=True
                    )
                    return
            
                if amount < 0.00000001:  
                    await interaction.response.send_message(
                        "Minimum tip amount is 0.00000001 points",
                        ephemeral=True
                    )
                    return
            
                if user.id == interaction.user.id:
                    await interaction.response.send_message(
                        "You cannot tip points to yourself",
                        ephemeral=True
                    )
                    return
            
                # Attempt to transfer the points
                success = await self.db.transfer_points(
                    str(interaction.user.id),
                    str(user.id),
                    amount
                )
        
                if success:
                    # Get updated points for both users
                    sender_points = await self.db.get_points(str(interaction.user.id))
                    receiver_points = await self.db.get_points(str(user.id))
            
                    # Format points with 8 decimal places
                    formatted_amount = f"{amount:.8f}"
                    formatted_sender = f"{sender_points:.8f}"
                    formatted_receiver = f"{receiver_points:.8f}"
            
                    # Send ephemeral confirmation to the tipper
                    await interaction.response.send_message(
                        f"Successfully tipped {formatted_amount} points to {user.mention}!\n"
                        f"Your new balance: {formatted_sender} points\n"
                        f"{user.name}'s new balance: {formatted_receiver} points",
                        ephemeral=True
                    )
            
                    # Send announcement in the current channel
                    channel = interaction.channel
                    if channel:  # Check if channel exists (includes both TextChannel and Thread)
                        try:
                            await channel.send(
                                f"💰 Tip successful!!! {interaction.user.mention} tipped {formatted_amount} points to {user.mention}!",
                                allowed_mentions=discord.AllowedMentions(users=[interaction.user, user])  # Only mention involved users
                            )
                        except discord.errors.Forbidden:
                        # If bot doesn't have permission to send messages, silently continue
                            pass
                
                else:
                    await interaction.response.send_message(
                        "You don't have enough points for this tip",
                        ephemeral=True
                    )
            
            except Exception as e:
                error_msg = f"Error in tip command: {str(e)}"
                print(error_msg)  
                logging.error(error_msg) 
                self.error_logger.log_error(e, "tip command")
                await interaction.response.send_message(
                    f"An error occurred while processing the tip: {str(e)}",
                    ephemeral=True
                )

        
        @self.tree.command(name="airdrop", description="Airdrop points to users with a specific role or to specific user (Admin only)")
        @app_commands.checks.has_permissions(administrator=True)
        @app_commands.describe(
            role="Role to airdrop points to (optional)",
            user="Specific user to airdrop points to (optional)",
            amount="Amount of points to airdrop (can be decimal)"
        )
        async def airdrop(
            interaction: discord.Interaction,
            amount: float,
            role: Optional[discord.Role] = None,
            user: Optional[discord.User] = None
        ):
            try:
                if not interaction.guild:
                    await interaction.response.send_message(
                        "This command can only be used in a server",
                        ephemeral=True
                    )
                    return

                if role is None and user is None:
                    await interaction.response.send_message(
                        "You must specify either a role or a user to airdrop points to",
                        ephemeral=True
                    )
                    return

                if role is not None and user is not None:
                    await interaction.response.send_message(
                        "Please specify either a role OR a user, not both",
                        ephemeral=True
                    )
                    return

                amount = round(amount, 8)
                if amount <= 0:
                    await interaction.response.send_message(
                        "Amount must be positive",
                        ephemeral=True
                    )
                    return

                recipients = []
                if role:
                    # Get all members with the specified role
                    recipients = [member for member in interaction.guild.members if role in member.roles]
                    if not recipients:
                        await interaction.response.send_message(
                            f"No members found with the role {role.name}",
                            ephemeral=True
                        )
                        return
                else:
                    # Single user airdrop
                    recipients = [user]

                # Update points for all recipients
                for recipient in recipients:
                    await self.db.update_points(
                        str(recipient.id),
                        recipient.name,
                        amount
                    )

                # Format points for display
                formatted_amount = f"{amount:.8f}"

                # Create mention string for announcement
                if role:
                    mention_str = role.mention
                else:
                    mention_str = user.mention

                # Send ephemeral confirmation to admin
                await interaction.response.send_message(
                    f"Successfully airdropped {formatted_amount} points to {'the role ' + role.name if role else user.name}!",
                    ephemeral=True
                )

                # Send public announcement in the channel
                channel = interaction.channel
                if channel:
                    announcement = f"🪂 Airdrop coming! {mention_str} check your {formatted_amount} points!!"
                    try:
                        await channel.send(
                            announcement,
                            allowed_mentions=discord.AllowedMentions(
                                roles=True if role else False,
                                users=False if role else True
                            )
                        )
                    except discord.errors.Forbidden:
                        logging.error("Failed to send airdrop announcement due to permissions")

            except Exception as e:
                self.error_logger.log_error(e, "airdrop command")
                await interaction.response.send_message(
                    f"An error occurred while processing the airdrop: {str(e)}",
                    ephemeral=True
                )


        @self.tree.command(name="exportlog", description="Export bot log file (Admin only)")
        @app_commands.checks.has_permissions(administrator=True)
        async def exportlog(
            interaction: discord.Interaction, 
            lines: Optional[int] = 100
        ):
            try:
                if not interaction.guild:
                    await interaction.response.send_message(
                        "This command can only be used in a server", 
                        ephemeral=True
                    )
                    return

                log_path = 'bot.log'
                if not os.path.exists(log_path):
                    await interaction.response.send_message(
                        "Log file not found.", 
                        ephemeral=True
                    )
                    return

                def read_last_lines(file_path, num_lines):
                    with open(file_path, 'r') as f:
                        lines = f.readlines()
                        return lines[-num_lines:]

                last_lines = read_last_lines(log_path, lines)
        

                if len(''.join(last_lines)) > 8000:
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    temp_log_path = f'./temp_log_{timestamp}.txt'
                    with open(temp_log_path, 'w') as f:
                        f.writelines(last_lines)
            
                    try:
                        await interaction.response.send_message(
                            f"Last {lines} lines of log file:",
                            file=discord.File(temp_log_path, filename=f'bot_log_{timestamp}.txt'),
                            ephemeral=True
                        )
                    finally:
                
                        if os.path.exists(temp_log_path):
                            os.remove(temp_log_path)
                else:

                    log_content = '```\n' + ''.join(last_lines) + '\n```'
                    await interaction.response.send_message(
                        f"Last {lines} lines of log file:\n" + log_content, 
                        ephemeral=True
                    )

            except Exception as e:
                self.error_logger.log_error(e, "exportlog command")
                await interaction.response.send_message(
                    "An error occurred while exporting the log file", 
                    ephemeral=True
                )


        @self.tree.command(name="exportbackup", description="Export a backup database file (Admin only)")
        @app_commands.checks.has_permissions(administrator=True)
        async def exportbackup(
            interaction: discord.Interaction,
            days_ago: Optional[int] = 1
        ):
            try:
                if not interaction.guild:
                    await interaction.response.send_message(
                        "This command can only be used in a server",
                        ephemeral=True
                    )
                    return

                await interaction.response.defer(ephemeral=True)

                backup_dir = './backup'
                if not os.path.exists(backup_dir):
                    await interaction.followup.send(
                        "No backup directory found.",
                        ephemeral=True
                    )
                    return

                # Get list of backup files sorted by creation time
                backup_files = []
                for f in os.listdir(backup_dir):
                    if f.startswith('points_') and f.endswith('.db'):
                        file_path = os.path.join(backup_dir, f)
                        backup_files.append((file_path, os.path.getctime(file_path)))
                
                backup_files.sort(key=lambda x: x[1], reverse=True)

                if not backup_files:
                    await interaction.followup.send(
                        "No backup files found.",
                        ephemeral=True
                    )
                    return

                # Get the requested backup file
                try:
                    requested_file = backup_files[days_ago - 1][0]
                except IndexError:
                    await interaction.followup.send(
                        f"No backup file found from {days_ago} days ago. Available backups: {len(backup_files)}",
                        ephemeral=True
                    )
                    return

                # Send the backup file
                try:
                    await interaction.followup.send(
                        f"Here's your backup database from {days_ago} day(s) ago:",
                        file=discord.File(requested_file),
                        ephemeral=True
                    )
                except Exception as e:
                    await interaction.followup.send(
                        f"Error sending backup file: {str(e)}",
                        ephemeral=True
                    )

            except Exception as e:
                self.error_logger.log_error(e, "exportbackup command")
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        f"An error occurred while exporting the backup: {str(e)}",
                        ephemeral=True
                    )
                else:
                    await interaction.followup.send(
                        f"An error occurred while exporting the backup: {str(e)}",
                        ephemeral=True
                    )
        @self.tree.command(name="members", description="Export server members to a CSV file")
        @app_commands.checks.has_permissions(administrator=True)
        async def members(interaction: discord.Interaction):
            if not interaction.guild:
                await interaction.response.send_message( 
                    "This command can only be used in a server",
                    ephemeral=True
                )
                return
    
            try:
                await interaction.response.defer(ephemeral=True)
                # Get all members
                members = await interaction.guild.fetch_members().flatten()
        
                # Prepare CSV data
                filepath = os.path.join('output', 'members.csv')
                with open(filepath, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=['id', 'username', 'joinDate', 'roles'])
                    writer.writeheader()
                    for member in members:
                        writer.writerow({
                            'id': member.id,
                            'username': str(member),
                            'joinDate': member.joined_at.strftime('%Y-%m-%d') if member.joined_at else 'Unknown',
                            'roles': ', '.join(role.name for role in member.roles)
                        })
        
                await interaction.followup.send(
                    f"Here's a list of {len(members)} members!",
                    file=discord.File(filepath),
                    ephemeral=True
                )
        
                # Cleanup
                if os.path.exists(filepath):
                    os.remove(filepath)
            
            except Exception as e:
                self.error_logger.log_error(e, "members command")
                await interaction.followup.send(
                    f"An error occurred while executing this command: {str(e)}",  
                    ephemeral=True
                )

        
        @self.tree.command(name="rolemembers", description="Export members with a specific role to CSV")
        @app_commands.checks.has_permissions(administrator=True)
        @app_commands.describe(role="The role to export members of")
        async def rolemembers(interaction: discord.Interaction, role: discord.Role):
            if not interaction.guild:
                await interaction.response.send_message(
                    "This command can only be used in a server",
                    ephemeral=True
                )
                return
    
            try:
                await interaction.response.defer(ephemeral=True)
                # Get members with the specified role
                members = [member for member in await interaction.guild.fetch_members().flatten()
                          if role in member.roles]
        
                if not members:
                    await interaction.followup.send(
                        f"No members found with the role {role.name}",
                        ephemeral=True
                    )
                    return
        
                # Prepare CSV data
                filename = f'role_members_{role.name}.csv'
                filepath = os.path.join('output', filename)
                with open(filepath, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=['id', 'username', 'joinDate', 'roles'])
                    writer.writeheader()
                    for member in members:
                        writer.writerow({
                            'id': member.id,
                            'username': str(member),
                            'joinDate': member.joined_at.strftime('%Y-%m-%d') if member.joined_at else 'Unknown',
                            'roles': ', '.join(role.name for role in member.roles)
                        })
        
                await interaction.followup.send(
                    f"Here's a list of {len(members)} members with the role {role.name}!",
                    file=discord.File(filepath),
                    ephemeral=True
                )
        
                # Cleanup
                if os.path.exists(filepath):
                    os.remove(filepath)
            
            except Exception as e:
                self.error_logger.log_error(e, "members command")
                await interaction.followup.send(
                    f"An error occurred while executing this command: {str(e)}",  
                    ephemeral=True
                )



        @self.tree.command(name="exportdb", description="Export current database (Admin only)")
        @app_commands.checks.has_permissions(administrator=True)
        async def exportdb(interaction: discord.Interaction):
            try:
                if not interaction.guild:
                    await interaction.response.send_message(
                        "This command can only be used in a server",
                        ephemeral=True
                    )
                    return

                await interaction.response.defer(ephemeral=True)

                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                temp_path = f'./temp_export_{timestamp}.db'
                try:
                    await self.db.checkpoint()

                    shutil.copy2(self.db.db_path, temp_path)
            
                    await interaction.followup.send(
                        "Here's your database export:",
                        file=discord.File(temp_path, filename=f'points_{timestamp}.db'),
                        ephemeral=True
                    )

                except Exception as e:
                    logging.error(f"Error during database export: {str(e)}")
                    await interaction.followup.send(
                        f"An unexpected error occurred while exporting the database: {str(e)}",
                        ephemeral=True
                    )
                finally:
                    if os.path.exists(temp_path):
                        try:
                            os.remove(temp_path)
                        except Exception as e:
                            logging.error(f"Error removing temp file: {str(e)}")

            except Exception as e:
                self.error_logger.log_error(e, "exportdb command")
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        f"An error occurred while exporting the database: {str(e)}",
                        ephemeral=True
                    )
                else:
                    await interaction.followup.send(
                        f"An error occurred while exporting the database: {str(e)}",
                        ephemeral=True
                    )

        await self.tree.sync(guild=None)

    async def _process_tweets(self) -> list:
        """
        Process tweets and update points
        Returns a list of updates made
        """
        updates = []
        try:
            tweets = await self.db.get_active_tweets()
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
                    await self.db.update_points(
                        str(member.id),
                        member.name,
                        points
                    )

                updates.append({
                    "tweet_id": tweet['id'],
                    "points": points
                })

                formatted_points = f"{points:.8f}"
                await channel.send(
                    f"🎉 Points Update:\n"
                    f"Tweet: https://twitter.com/x/status/{tweet['id']}\n"
                    f"Congrats! Each OG member received {formatted_points} points!"
                )
                
        except Exception as e:
            self.error_logger.log_error(e, "_process_tweets")
            raise e

        return updates

    @tasks.loop(hours=24)
    async def check_tweets(self):
        try:
            await self._process_tweets()
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
           
            await self.db.backup_database(backup_path)
            os.chmod(backup_path, 0o600)
       
            cutoff = (datetime.now() - timedelta(days=7))
            for f in os.listdir(backup_dir):
                if f.startswith('points_') and f.endswith('.db'):
                    file_path = os.path.join(backup_dir, f)
                    if datetime.fromtimestamp(os.path.getctime(file_path)) < cutoff:
                        os.remove(file_path)
                   
        except Exception as e:
            self.error_logger.log_error(e, "database backup")

    @check_tweets.before_loop
    @backup_database.before_loop
    async def before_tasks(self):
        await self.wait_until_ready()

    async def close(self):
        if hasattr(self, 'db'):
            self.db.__exit__(None, None, None)
        await super().close()

    async def on_ready(self):
        print(f'Logged in as {self.user} (ID: {self.user.id})')
        print('------')
    
 
