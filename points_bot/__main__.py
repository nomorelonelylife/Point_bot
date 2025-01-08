import os
import sys
import logging
import threading
from typing import List
from dotenv import load_dotenv
from .bot import PointsBot

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('bot.log')
        ]
    )

def validate_env(required_envs: List[str]) -> None:
    missing = [env for env in required_envs if not os.getenv(env)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    try:
        int(os.getenv('SOCIAL_CHANNEL_ID', '0'))
        int(os.getenv('OG_ROLE_ID', '0'))
    except ValueError:
        raise ValueError("SOCIAL_CHANNEL_ID and OG_ROLE_ID must be valid integers")

def main():
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


        bot = PointsBot(
            twitter_token=os.getenv('TWITTER_BEARER_TOKEN'),
            channel_id=os.getenv('SOCIAL_CHANNEL_ID'),
            og_role_id=os.getenv('OG_ROLE_ID'),
            db_path=os.getenv('DB_PATH')
        )
        
        bot.run(os.getenv('DISCORD_TOKEN'), log_handler=None)

    except Exception as e:
        logging.critical(f"Fatal error: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main()
