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

    TVBVODDB1_HOSTNAME = os.getenv("TVBVODDB1_HOSTNAME")
    TVBVODDB1_PASSWORD = os.getenv("TVBVODDB1_PASSWORD")
    TVBVODDB1_DATABASE = os.getenv("TVBVODDB1_DATABASE")
    TVBVODDB1_USERNAME = os.getenv("TVBVODDB1_USERNAME")

    TVBVODDB2_HOSTNAME = os.getenv("TVBVODDB2_HOSTNAME")
    TVBVODDB2_PASSWORD = os.getenv("TVBVODDB2_PASSWORD")
    TVBVODDB2_DATABASE = os.getenv("TVBVODDB2_DATABASE")
    TVBVODDB2_USERNAME = os.getenv("TVBVODDB2_USERNAME")

    IPABSTRACT_API_KEY = os.getenv("IPABSTRACT_API_KEY")
    ABSTRACT_ENDPOINT = f"https://ipgeolocation.abstractapi.com/v1/?api_key={IPABSTRACT_API_KEY}&ip_address="

    TAGMANAGER_HOSTNAME = os.getenv("TAGMANAGER_HOSTNAME")
    TAGMANAGER_PASSWORD = os.getenv("TAGMANAGER_PASSWORD")
    TAGMANAGER_DATABASE = os.getenv("TAGMANAGER_DATABASE")
    TAGMANAGER_USERNAME = os.getenv("TAGMANAGER_USERNAME")

    # Constructing the connection strings
    mysql_pre = "mysql+mysqlconnector://"

    TVBVODDB1_CONNECTION_STRING = (
        f'{mysql_pre}{TVBVODDB1_USERNAME}:{TVBVODDB1_PASSWORD}'
        f'@{TVBVODDB1_HOSTNAME}/{TVBVODDB1_DATABASE}'
    )

    TVBVODDB2_CONNECTION_STRING = (
        f'{mysql_pre}{TVBVODDB2_USERNAME}:{TVBVODDB2_PASSWORD}'
        f'@{TVBVODDB2_HOSTNAME}/{TVBVODDB2_DATABASE}'
    )

    TVFACTORY_CONNECTION_STRING = (
        f'{mysql_pre}{TVFACTORY_USERNAME}:{TVFACTORY_PASSWORD}'
        f'@{TVFACTORY_HOSTNAME}/{TVFACTORY_DATABASE}'
    )

    TAGMANAGER_CONNECTION_STRING = (
        f'{mysql_pre}{TAGMANAGER_USERNAME}:{TAGMANAGER_PASSWORD}'
        f'@{TAGMANAGER_HOSTNAME}/{TAGMANAGER_DATABASE}'
    )

    SERVER_TIMEZONE = 'Pacific/Auckland'
    LOCK_NAME = 'update_task_lock'
    LOCK_TIMEOUT = 1200  # 20 minutes (you can adjust per task if needed)
    BLOCKED_KEY = 'summarise_blocked'
    BATCH_SIZE = 10000
    RETRY_LIMIT = 3
    RETRY_DELAY = 5
    IP_CONCURRENCY = 30
    SIMULATE_IP_API = os.getenv('SIMULATE_IP_API', 'True').lower() in ('true', '1', 't')


    ML_MODEL_PATH = '/opt/tvfactory/ml_packages/useragent_model.pkl'
    ML_VECTORIZER_PATH = '/opt/tvfactory/ml_packages/vectorizer.pkl'

    # Logging the loaded configuration values (without sensitive data)
    logger.info(f"Host name to check we read .env: {TVFACTORY_HOSTNAME}")


# Initialize settings instance
settings = Settings()
