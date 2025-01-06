# Discord Points Bot

A Discord bot that tracks engagement on Twitter/X posts and awards points to users with specific roles.

## Features

- Monitor Twitter/X posts for engagement metrics (likes, retweets, replies)
- Award points to users with specified roles based on post performance
- Track user points in SQLite database with WAL journaling
- Daily automated database backups with 7-day retention
- Health check monitoring via heartbeat URL
- Error logging with recent error history

## Installation

1. Clone the repository
2. Create a virtual environment:
```bash
python -m venv env
source env/bin/activate  # Linux/Mac
```
3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

Create a `.env` file with the following variables:

```
DISCORD_TOKEN=your_discord_bot_token
TWITTER_BEARER_TOKEN=your_twitter_bearer_token
SOCIAL_CHANNEL_ID=channel_id_for_announcements
OG_ROLE_ID=role_id_to_track
HEARTBEAT_URL=optional_monitoring_url
DB_PATH=optional_custom_db_path
```

## Usage

### Start the Bot
```bash
python -m points_bot
```

### Discord Commands

- `/addtweet <url> [like_points] [retweet_points] [reply_points]` - Add tweet to monitor (admin only)
- `/points` - Check your points
- `/activeposts` - View monitored posts (admin only)

## Data Management

- Database is backed up daily
- Backups older than 7 days are automatically removed
- Uses SQLite with WAL journaling for better concurrency
- Automatic rate limiting for Twitter API requests

## Security Features

- Secure file permissions for database and backups
- Input validation for Twitter URLs and point values
- Error logging with context preservation
- Timeout handling for external API calls