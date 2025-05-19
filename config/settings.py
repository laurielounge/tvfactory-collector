# config.py
import os

from dotenv import load_dotenv

from core.logger import logger

# Load environment variables from a .env file, if available
load_dotenv(override=False)


class Settings:
    # RabbitMQ settings
    RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
    RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
    RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")
    RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
    RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")

    # Application settings
    TVFACTORY_HOSTNAME = os.getenv("TVFACTORY_HOSTNAME")
    TVFACTORY_PASSWORD = os.getenv("TVFACTORY_PASSWORD")
    TVFACTORY_DATABASE = os.getenv("TVFACTORY_DATABASE")
    TVFACTORY_USERNAME = os.getenv("TVFACTORY_USERNAME")

    MARIADB_HOST = os.getenv("MARIADB_HOST", "10.0.0.113")
    MARIADB_PORT = int(os.getenv("MARIADB_PORT", 3306))
    MARIADB_USER = os.getenv("MARIADB_USER", "iplookup_service")
    MARIADB_PASSWORD = os.getenv("MARIADB_PASSWORD")
    MARIADB_DB = os.getenv("MARIADB_DB", "infinitum_raw_data")
    MARIADB_DSN = f"mysql+aiomysql://{MARIADB_USER}:{MARIADB_PASSWORD}@{MARIADB_HOST}:{MARIADB_PORT}/{MARIADB_DB}"  # Redis Settings

    REDIS_HOST = os.getenv("REDIS_HOST")
    REDIS_PORT = os.getenv("REDIS_PORT")
    REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
    REDIS_TIMEOUT = os.getenv("REDIS_TIMEOUT")

    SQL_ECHO = False

    # Constructing the connection strings
    mysql_pre = "mysql://"

    TVFACTORY_CONNECTION_STRING = (
        f'{mysql_pre}{TVFACTORY_USERNAME}:{TVFACTORY_PASSWORD}'
        f'@{TVFACTORY_HOSTNAME}/{TVFACTORY_DATABASE}'
    )

    SERVER_TIMEZONE = 'Pacific/Auckland'
    LOCK_NAME = 'update_task_lock'
    LOCK_TIMEOUT = 1200  # 20 minutes (you can adjust per task if needed)
    BLOCKED_KEY = 'summarise_blocked'
    BATCH_SIZE = 10000
    RETRY_LIMIT = 3
    RETRY_DELAY = 5
    ONE_DAY = 86400
    ONE_WEEK = 604800

    # Logging the loaded configuration values (without sensitive data)
    logger.info(f"Host name to check we read .env: {RABBITMQ_HOST}")


# Initialize settings instance
settings = Settings()
logger.info(f"rabbitmq host: {settings.RABBITMQ_HOST}")