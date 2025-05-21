# config.py
import os
from ipaddress import ip_network

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
    LOW_VOLUME_THRESHOLD: int = int(os.getenv("LOW_VOLUME_THRESHOLD", 30))
    LOW_VOLUME_PAUSE_SECONDS: int = int(os.getenv("LOW_VOLUME_PAUSE_SECONDS", 10))
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

    BOGON_NETWORKS = [
        ip_network("0.0.0.0/8"),
        ip_network("10.0.0.0/8"),
        ip_network("100.64.0.0/10"),
        ip_network("127.0.0.0/8"),
        ip_network("169.254.0.0/16"),
        ip_network("172.16.0.0/12"),
        ip_network("192.0.0.0/24"),
        ip_network("192.0.2.0/24"),
        ip_network("192.168.0.0/16"),
        ip_network("198.18.0.0/15"),
        ip_network("198.51.100.0/24"),
        ip_network("203.0.113.0/24"),
        ip_network("224.0.0.0/4"),
        ip_network("240.0.0.0/4"),
        ip_network("255.255.255.255/32"),
        ip_network("::/128"),
        ip_network("::1/128"),
        # ip_network("::ffff:0:0/96"),
        ip_network("192.88.99.0/24"),
        ip_network("::/96"),
        ip_network("100::/64"),
        ip_network("2001:10::/28"),
        ip_network("2001:db8::/32"),
        ip_network("fc00::/7"),
        ip_network("fe80::/10"),
        ip_network("fec0::/10"),
        ip_network("ff00::/8"),
        ip_network("2002::/24"),
        ip_network("2002:a00::/24"),
        ip_network("2002:7f00::/24"),
        ip_network("2002:a9fe::/32"),
        ip_network("2002:ac10::/28"),
        ip_network("2002:c000::/40"),
        ip_network("2002:c000:200::/40"),
        ip_network("2002:c0a8::/32"),
        ip_network("2002:c612::/31"),
        ip_network("2002:c633:6400::/40"),
        ip_network("2002:cb00:7100::/40"),
        ip_network("2002:e000::/20"),
        ip_network("2002:f000::/20"),
        ip_network("2002:ffff:ffff::/48"),
        ip_network("2001::/40"),
        ip_network("2001:0:a00::/40"),
        ip_network("2001:0:7f00::/40"),
        ip_network("2001:0:a9fe::/48"),
        ip_network("2001:0:ac10::/44"),
        ip_network("2001:0:c000::/56"),
        ip_network("2001:0:c000:200::/56"),
        ip_network("2001:0:c0a8::/48"),
        ip_network("2001:0:c612::/47"),
        ip_network("2001:0:c633:6400::/56"),
        ip_network("2001:0:cb00:7100::/56"),
        ip_network("2001:0:e000::/36"),
        ip_network("2001:0:f000::/36"),
        ip_network("2001:0:ffff:ffff::/64"),
    ]


# Initialize settings instance
settings = Settings()
logger.info(f"rabbitmq host: {settings.RABBITMQ_HOST}")