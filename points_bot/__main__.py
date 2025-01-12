import os
import sys
import asyncio
import logging
from logging.handlers import RotatingFileHandler
from typing import List
from dotenv import load_dotenv
from .bot import PointsBot
from .database import DatabaseService

def setup_logging():
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    
    file_handler = RotatingFileHandler(
        'bot.log',
        maxBytes=1000*1024,  
        backupCount=1,      
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

def validate_env(required_envs: List[str]) -> None:
    missing = [env for env in required_envs if not os.getenv(env)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    try:
        int(os.getenv('SOCIAL_CHANNEL_ID', '0'))
        int(os.getenv('OG_ROLE_ID', '0'))
    except ValueError:
        raise ValueError("SOCIAL_CHANNEL_ID and OG_ROLE_ID must be valid integers")

async def main():
    try:
        load_dotenv()
        setup_logging()
        
        required_envs = [
            'DISCORD_TOKEN',
            'TWITTER_BEARER_TOKEN',
            'SOCIAL_CHANNEL_ID',
            'OG_ROLE_ID'
        ]
        
        validate_env(required_envs)

        print("Creating database instance...")
        db = DatabaseService(
            db_path=os.getenv('DB_PATH', './points.db')
        )
        
        # Async initialize database
        await db.async_initialize()

        print("Creating bot instance...")
        logging.info("Creating bot instance...")

        bot = PointsBot(
            twitter_token=os.getenv('TWITTER_BEARER_TOKEN'),
            channel_id=os.getenv('SOCIAL_CHANNEL_ID'),
            og_role_id=os.getenv('OG_ROLE_ID'),
            database_service=db
        )
        
        print("Running bot...")
        logging.info("Running bot...")
        await bot.start(os.getenv('DISCORD_TOKEN'))

    except Exception as e:
        logging.critical(f"Fatal error: {str(e)}")
        print(f"Fatal error: {str(e)}")
        sys.exit(1)

def run_bot():
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot stopped by user.")
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    run_bot()