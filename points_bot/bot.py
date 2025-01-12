import discord
import re
import logging
import random
import csv
import asyncio
import os
import shutil
import aiohttp
import time
from discord import app_commands
from discord.ext import tasks
from typing import Optional, List, Dict
from datetime import datetime, timedelta
from .database import DatabaseService
from .twitter_service import TwitterService
from collections import deque

class ErrorLogger:
    def __init__(self, max_logs=30):
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
        database_service: Optional[DatabaseService] = None,
        db_path: Optional[str] = None
    ):
        intents = discord.Intents.default()
        intents.members = True
        super().__init__(intents=intents)
    
        self.tree = app_commands.CommandTree(self)
        self.db = database_service or DatabaseService(db_path or './points.db')
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

    async def start(self, token):
        """
        Asynchronously start the bot

        Args:
            token (str): Discord bot token
        """
        try:
            print("Starting bot with start method...")
            logging.info("Starting bot with start method...")
        
            self.check_tweets.start()
            self.backup_database.start()
        
            await super().start(token)
    
        except Exception as e:
            print(f"Error starting bot: {e}")
            logging.error(f"Error starting bot: {e}")
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

        @self.tree.command(name="confetti", description="Create a confetti ball with points")
        @app_commands.describe(
            total_points="Total points to put in the confetti ball (min: 0.00000001)",
            max_claims="Maximum number of users who can claim points (min: 1, max: 100)",
            message="Optional message to display (max 140 characters)",
            expires_in="Expiration time in seconds (optional, random if not set)"
        )
        async def confetti(
            interaction: discord.Interaction,
            total_points: float,
            max_claims: int,
            message: Optional[str] = None,
            expires_in: Optional[int] = None
        ):
            try:
                # Validate inputs
                if total_points < 0.00000001:
                    await interaction.response.send_message(
                        "Minimum total points is 0.00000001",
                        ephemeral=True
                    )
                    return

                if max_claims < 1 or max_claims > 100:
                    await interaction.response.send_message(
                        "Number of claims must be between 1 and 100",
                        ephemeral=True
                    )
                    return

                if not interaction.guild:
                    await interaction.response.send_message(
                        "This command can only be used in a server",
                        ephemeral=True
                    )
                    return

                total_points = round(total_points, 8)
        
                user_points = await self.db.get_points(str(interaction.user.id))
                if user_points < total_points:
                    await interaction.response.send_message(
                        "You don't have enough points",
                        ephemeral=True
                    )
                    return
                
                # Validate expiration
                expires_at = None
                if expires_in is not None:
                    if expires_in < 1 or expires_in > 24 * 60 * 60:  # 1 second to 24 hours
                        await interaction.response.send_message(
                            "Expiration must be between 1 second and 24 hours",
                            ephemeral=True
                        )
                        return
                    expires_at = datetime.now() + timedelta(seconds=expires_in)

                ball_id = f"ball_{int(time.time())}_{interaction.user.id}"

        
                if not message:
                    message = "I prepared a confetti ball, LET's Loot!!!!"
                elif len(message) > 140:
                    message = message[:140]
                
                await self.db.create_confetti_ball(
                    ball_id=ball_id,
                    creator_id=str(interaction.user.id),
                    total_points=total_points,
                    max_claims=max_claims,
                    message=message,
                    channel_id=str(interaction.channel_id),
                    expires_at=expires_at
                )

                # Get the actual expiration time (in case it was randomly generated)
                ball = await self.db.get_confetti_ball(ball_id)
                expires_at = datetime.fromisoformat(ball['expires_at'])

                # Calculate time remaining
                time_remaining = expires_at - datetime.now()
                time_str = (
                    f"{time_remaining.total_seconds():.0f} seconds" 
                    if time_remaining.total_seconds() < 3600 
                    else f"{time_remaining.total_seconds() / 3600:.1f} hours"
                )

                await self.db.transfer_points(
                    str(interaction.user.id),
                    "confetti_pool",
                    total_points
                )

                view = ConfettiView(self.db, ball_id, max_claims)
                await interaction.response.send_message(
                    f"🎊 Confetti Ball 🎊\n"
                    f"{ interaction.user.mention} says: {message}\n"
                    f"Hurry up! {max_claims} lucky Nads can loot points from this confetti ball!"
                    f"⏰ Expires in: {time_str}",
                    view=view
                )

            except Exception as e:
                error_msg = f"Error in confetti command: {str(e)}\nUser: {interaction.user.id}\nTotal Points: {total_points}\nMax Claims: {max_claims}\nMessage: {message}"
                print(error_msg)  
                logging.error(error_msg) 
                self.error_logger.log_error(e, f"confetti command - Details: {error_msg}")
        
                await interaction.response.send_message(
                    f"An error occurred while creating the confetti ball: {str(e)}\nPlease try again or contact an administrator.",
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

        @self.tree.command(name="createvote", description="Create a vote (Admin only)")
        @app_commands.checks.has_permissions(administrator=True)
        @app_commands.describe(
            user="User who will receive the points",
            description="Vote description/topic",
            option1="Option 1 format: text|points",
            option2="Option 2 format: text|points",
            option3="Option 3 format: text|points (optional)",
            option4="Option 4 format: text|points (optional)",
            option5="Option 5 format: text|points (optional)",
            expires_in="Days until vote expires (default: 7)"
        )
        async def createvote(
            interaction: discord.Interaction,
            user: discord.User,
            description: str,
            option1: str,
            option2: str,
            option3: Optional[str] = None,
            option4: Optional[str] = None,
            option5: Optional[str] = None,
            option6: Optional[str] = None,
            option7: Optional[str] = None,
            option8: Optional[str] = None,
            option9: Optional[str] = None,
            option10: Optional[str] = None,
            expires_in: Optional[int] = 7
        ):
            try:
                if not interaction.guild:
                    await interaction.response.send_message(
                        "This command can only be used in a server",
                        ephemeral=True
                    )
                    return
    
                # Validate expiration
                if expires_in < 1 or expires_in > 30:
                    await interaction.response.send_message(
                        "Vote duration must be between 1 and 30 days",
                        ephemeral=True
                    )
                    return
    
                # Process options
                options = []
                for i, opt in enumerate([option1, option2, option3, option4, option5,
                                       option6, option7, option8, option9, option10]):
                    if opt:
                        try:
                            text, points_str = opt.split('|')
                            points = float(points_str)
                            if points <= 0:
                                await interaction.response.send_message(
                                    f"Points must be positive for option {i+1}",
                                    ephemeral=True
                                )
                                return
                    
                            options.append({
                                'index': i,
                                'option_text': text.strip(),
                                'points': round(points, 8)
                            })
                        except ValueError:
                            await interaction.response.send_message(
                                f"Invalid format for option {i+1}. Use 'text|points'",
                                ephemeral=True
                            )
                            return
        
                if len(options) < 2:
                    await interaction.response.send_message(
                        "You must provide at least 2 options",
                        ephemeral=True
                    )
                    return
    
                # Defer response as the next operations might take some time
                await interaction.response.defer()
    
                # Create vote
                vote_id = f"vote_{int(time.time())}_{interaction.user.id}"
                await self.db.create_vote(
                    vote_id=vote_id,
                    creator_id=str(interaction.user.id),
                    target_user_id=str(user.id),
                    description=description,
                    options=options,
                    expires_in_days=expires_in
                )

                # Create options text
                options_text = "\n".join(
                    f"{i+1}. {opt['option_text']} ({opt['points']:.8f} points)"
                    for i, opt in enumerate(options)
                )

                # Create vote message
                message = (
                    f"🗳️ **New Vote** by {interaction.user.mention}\n\n"
                    f"**For**: {user.mention}\n"
                    f"**Topic**: {description}\n\n"
                    f"**Options**:\n{options_text}\n\n"
                    f"**Expires in**: {expires_in} days\n"
                    f"Current votes: 0\n"
                    f"Total awarded points: 0"
                )

                # Create and setup vote view
                view = VoteView(self.db, vote_id)
                await view.create_buttons(options)

                # Send vote message
                await interaction.followup.send(message, view=view)

            except Exception as e:
                logging.error(f"Create vote error: {str(e)}")
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        "An error occurred while creating the vote. Please try again later.",
                        ephemeral=True
                    )
                else:
                    await interaction.followup.send(
                        "An error occurred while creating the vote. Please try again later.",
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
                members = []
                async for member in interaction.guild.fetch_members():
                    members.append(member)
        
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

        

        @self.tree.command(name="confettitrap", description="Create a confetti trap")
        @app_commands.describe(
            max_claims="Maximum number of users who can fall for the trap (min: 1, max: 100)",
            message="Optional message to display (max 140 characters)",
            expires_in="Expiration time in seconds (optional, random if not set)"
        )
        async def confettitrap(
            interaction: discord.Interaction,
            max_claims: int,
            message: Optional[str] = None,
            expires_in: Optional[int] = None
        ):
            try:
                if max_claims < 1 or max_claims > 100:
                    await interaction.response.send_message(
                        "Number of claims must be between 1 and 100",
                        ephemeral=True
                    )
                    return

                if not interaction.guild:
                    await interaction.response.send_message(
                        "This command can only be used in a server",
                        ephemeral=True
                    )
                    return
                
                # Validate expiration similar to the confetti command
                if expires_in is not None:
                    if expires_in < 1 or expires_in > 24 * 60 * 60:  
                        await interaction.response.send_message(
                            "Expiration must be between 1 second and 24 hours",
                            ephemeral=True
                        )

                # Calculate expiration time
                expires_at = None
                if expires_in is not None:
                    expires_at = datetime.now() + timedelta(seconds=expires_in)

                trap_id = f"trap_{int(time.time())}_{interaction.user.id}"
        
                if not message:
                    message = "I prepared a confetti drop, LET's Loot!!!!"
                elif len(message) > 140:
                    message = message[:140]

                await self.db.create_confetti_trap(
                    trap_id=trap_id,
                    creator_id=str(interaction.user.id),
                    max_claims=max_claims,
                    message=message,
                    channel_id=str(interaction.channel_id),
                    expires_at=expires_at
                )

                # Get the actual expiration time
                trap = await self.db.get_confetti_trap(trap_id)
                expires_at = datetime.fromisoformat(trap['expires_at'])
        
                # Calculate time remaining
                time_remaining = expires_at - datetime.now()
                time_str = (
                    f"{time_remaining.total_seconds():.0f} seconds" 
                    if time_remaining.total_seconds() < 3600 
                    else f"{time_remaining.total_seconds() / 3600:.1f} hours"
                )

                view = ConfettiTrapView(
                    self.db,
                    trap_id,
                    str(interaction.user.id),
                    max_claims
                ) 
                await interaction.response.send_message(
                    f"🎊 Confetti Trap 🎊\n"
                    f"{ interaction.user.mention} says: {message}\n"
                    f"Hurry up! {max_claims} lucky Nads can loot points from this confetti ball!"
                    f"⏰ Expires in: {time_str}",
                    view=view
                )

            except Exception as e:
                logging.error(f"Error in confetti trap command: {str(e)}")
                await interaction.response.send_message(
                    "An error occurred while creating the confetti trap",
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
                members = []
                async for member in interaction.guild.fetch_members():
                    if role in member.roles:
                        members.append(member)
        
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
                self.error_logger.log_error(e, "rolemembers command")
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
        try:
        
            if hasattr(self, 'check_tweets'):
                self.check_tweets.cancel()
            if hasattr(self, 'backup_database'):
                self.backup_database.cancel()

            if hasattr(self, 'db'):
                if hasattr(self.db, 'close'):
                    await self.db.close()
                else:
                    self.db.__exit__(None, None, None)

            await super().close()

        except Exception as e:
            logging.error(f"Error during bot shutdown: {str(e)}")

    async def on_ready(self):
        print(f'Logged in as {self.user} (ID: {self.user.id})')
        print('----------------------------------------------')
    
        try:
            print("Registering commands...")
            logging.info("Registering commands...")
        
            # 注册命令
            await self.register_commands()
     
            # 同步命令树
            await self.tree.sync()
            print("Commands synced successfully")
            logging.info("Commands synced successfully")
    
        except Exception as e:
            print(f"Error registering commands: {e}")
            logging.error(f"Error registering commands: {e}")

















class ConfettiView(discord.ui.View):
    def __init__(self, db: DatabaseService, ball_id: str, max_claims: int):
        super().__init__(timeout=None)
        self.db = db
        self.ball_id = ball_id
        self.max_claims = max_claims

    @discord.ui.button(
        label="🎊 LOOT Points! 🎊", 
        style=discord.ButtonStyle.success,
        custom_id="confetti_claim"
    )
    async def claim_button(
        self, 
        interaction: discord.Interaction, 
        button: discord.ui.Button
    ):
        try:
            logging.info(f"Confetti claim attempt - Ball ID: {self.ball_id}, User: {interaction.user.id}")
        
            ball = await self.db.get_confetti_ball(self.ball_id)
            if not ball:
                logging.warning(f"Confetti ball not found or inactive - Ball ID: {self.ball_id}")
                await interaction.response.send_message(
                    "This confetti ball is no longer active!",
                    ephemeral=True
                )
                button.disabled = True
                await interaction.message.edit(view=self)
                return

            remaining_points = ball['total_points']
            remaining_claims = ball['max_claims'] - ball['claimed_count']
        
            logging.info(f"Confetti claim status - Ball ID: {self.ball_id}, Remaining claims: {remaining_claims}, Remaining points: {remaining_points}")

            if remaining_claims <= 0:
                logging.info(f"Confetti ball max claims reached - Ball ID: {self.ball_id}")
                await interaction.response.send_message(
                    "This confetti ball has been claimed by enough users!",
                    ephemeral=True
                )
                button.disabled = True
                await interaction.message.edit(view=self)
                return

            if remaining_claims == 1:
                points = remaining_points
            else:
                max_claim = min(remaining_points * 0.9, remaining_points / remaining_claims * 2)
                min_claim = max(remaining_points * 0.01, remaining_points / remaining_claims * 0.5)
                points = round(random.uniform(min_claim, max_claim), 8)
        
            logging.info(f"Attempting to claim points - Ball ID: {self.ball_id}, User: {interaction.user.id}, Points: {points}")

            try:
                success = await self.db.claim_confetti_ball(
                    self.ball_id,
                    str(interaction.user.id),
                    points
                )
            except Exception as e:
                error_msg = f"Database error in claim_confetti_ball - Ball ID: {self.ball_id}, User: {interaction.user.id}, Error: {str(e)}"
                logging.error(error_msg)
                await interaction.response.send_message(
                    "An error occurred while claiming points. Please try again.",
                    ephemeral=True
                )
                return

            if success:
                logging.info(f"Claim successful - Ball ID: {self.ball_id}, User: {interaction.user.id}, Points: {points}")
                await interaction.response.send_message(
                    f"🎉 You grabbed {points:.8f} points!",
                    ephemeral=True
                )
            
                try:
                    ball = await self.db.get_confetti_ball(self.ball_id)
                    if not ball or ball['claimed_count'] >= ball['max_claims']:
                        button.disabled = True
                        await interaction.message.edit(view=self)
                    
                        claims = await self.db.get_confetti_claims(self.ball_id)
                        summary = "\n".join(
                            f"{interaction.guild.get_member(int(claim['user_id'])).mention}: {claim['points_claimed']:.8f} points"
                            for claim in claims
                        )
                    
                        creator = interaction.guild.get_member(int(ball['creator_id']))
                        creator_mention = creator.mention if creator else "Unknown User"
                        await interaction.channel.send(
                            f"🎊 Confetti ball from {creator_mention} is complete! Here's who got lucky:\n{summary}"
                        )
                        logging.info(f"Confetti ball completed - Ball ID: {self.ball_id}")
                except Exception as e:
                    error_msg = f"Error updating button state or sending summary - Ball ID: {self.ball_id}, Error: {str(e)}"
                    logging.error(error_msg)

            else:
                logging.info(f"Claim rejected (already claimed) - Ball ID: {self.ball_id}, User: {interaction.user.id}")
                await interaction.response.send_message(
                    "You've already got your pie from this confetti ball!",
                    ephemeral=True
                )

        except Exception as e:
            error_msg = f"Unexpected error in confetti claim - Ball ID: {self.ball_id}, User: {interaction.user.id}, Error: {str(e)}"
            logging.error(error_msg)
            print(error_msg)  
            await interaction.response.send_message(
                "An unexpected error occurred. Please try again or contact an administrator.",
                ephemeral=True
            )



class ConfettiTrapView(discord.ui.View):

    def __init__(self, db: DatabaseService, trap_id: str, creator_id: str, max_claims: int):
        super().__init__(timeout=None)
        self.db = db
        self.trap_id = trap_id
        self.creator_id = creator_id
        self.max_claims = max_claims
 
    @discord.ui.button(
        label="🎊 LOOT Points! 🎊", 
        style=discord.ButtonStyle.success,
        custom_id="confetti_trap_claim"
    )
    async def claim_button(
        self, 
        interaction: discord.Interaction, 
        button: discord.ui.Button
    ):
        try:
            trap = await self.db.get_confetti_trap(self.trap_id)
            if not trap:
                await interaction.response.send_message(
                    "This confetti trap is no longer active!",
                    ephemeral=True
                )
                button.disabled = True
                await interaction.message.edit(view=self)
                return

            success, points_lost = await self.db.claim_confetti_trap(
                self.trap_id,
                str(interaction.user.id),
                self.creator_id
            )

            if success:
                await interaction.response.send_message(
                    f"😈 Oh no! You fell for the trap! Lost {points_lost:.8f} points!",
                    ephemeral=True
                )
                
                trap = await self.db.get_confetti_trap(self.trap_id)
                if not trap or trap['claimed_count'] >= trap['max_claims']:
                    button.disabled = True
                    await interaction.message.edit(view=self)
                    
                    claims = await self.db.get_confetti_trap_claims(self.trap_id)
                    summary = "\n".join(
                        f"{interaction.guild.get_member(int(claim['user_id'])).mention}: {claim['points_lost']:.8f} points"
                        for claim in claims
                    )
                    
                    creator = interaction.guild.get_member(int(self.creator_id))
                    creator_mention = creator.mention if creator else "Unknown User"
                    await interaction.channel.send(
                        f"😈 Confetti trap by {creator_mention} completed! Here's who fell for it:\n{summary}"
                    )

            else:
                await interaction.response.send_message(
                    "You've already been looted!",
                    ephemeral=True
                )

        except Exception as e:
            logging.error(f"Error in confetti trap claim: {str(e)}")
            await interaction.response.send_message(
                "An error occurred while processing your claim.",
                ephemeral=True
            )


class VoteView(discord.ui.View):
    def __init__(self, db: DatabaseService, vote_id: str):
        super().__init__(timeout=None)  
        self.db = db
        self.vote_id = vote_id
        
    async def create_buttons(self, options: List[Dict]):

        for option in options:
            option_id = f"{self.vote_id}_{option['index']}"  
            button = discord.ui.Button(
                label=f"{option['option_text']} ({option['points']} points)",
                custom_id=f"vote_{option_id}",
                style=discord.ButtonStyle.primary,
                row=option['index'] // 5  
            )
            button.callback = self.create_vote_callback(option_id)
            self.add_item(button)
            
    def create_vote_callback(self, option_id: str):

        async def vote_callback(interaction: discord.Interaction):
            try:

                success, points = await self.db.record_vote(
                    self.vote_id,
                    option_id,
                    str(interaction.user.id)
                )
                
                if success:

                    await interaction.response.send_message(
                        f"Vote successfully！{points:.8f} points have been awarded!!",
                        ephemeral=True
                    )
                    

                    results = await self.db.get_vote_results(self.vote_id)
                    if results:
                        current_votes = results['total_votes']
                        total_points = results.get('total_points_awarded', 0)
                        

                        content = interaction.message.content
                        content = re.sub(
                            r'Current vote count: \d+',
                            f'Current vote count: {current_votes}',
                            content
                        )
                        content = re.sub(
                            r'Total awarded points: [\d.]+',
                            f'Total awarded points: {total_points:.8f}',
                            content
                        )
                        
                        await interaction.message.edit(content=content)
                else:
                    await interaction.response.send_message(
                        "You have already cast your vote!",
                        ephemeral=True
                    )
                    
            except Exception as e:
                logging.error(f"Vote callback error: {str(e)}")
                await interaction.response.send_message(
                    "Voting error. Please try again later.",
                    ephemeral=True
                )
                
        return vote_callback