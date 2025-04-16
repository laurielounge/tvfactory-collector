# infrastructure/database.py
import time
from contextlib import contextmanager
from typing import Optional

import redis
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session

from config.config import settings
from core.logger import logger
from utils.yaml_utils import load_configuration


class DatabaseManager:
    """Manages database connections with aristocratic composure."""

    def __init__(self, config_path: str = "config/databases.yml"):
        self.config = load_configuration(config_path)
        self.engines = {}
        self.session_factories = {}

        # Initialize engines lazily
        for db_key in self.config:
            self._get_engine(db_key)  # This will create and cache the engine

    def _get_engine(self, db_key: str):
        """Retrieves or creates an engine with minimal fuss."""
        if db_key not in self.engines:
            if db_key not in self.config:
                raise ValueError(f"Unknown database key: {db_key}")

            db_config = self.config[db_key]
            connection_string = db_config['connection_string']

            logger.debug(f"Creating engine for {db_key}")
            self.engines[db_key] = create_engine(
                connection_string,
                pool_size=db_config.get('pool_size', 5),
                max_overflow=db_config.get('max_overflow', 10),
                pool_timeout=db_config.get('pool_timeout', 30),
                pool_recycle=db_config.get('pool_recycle', 3600)
            )

            self.session_factories[db_key] = scoped_session(
                sessionmaker(autocommit=False, autoflush=False, bind=self.engines[db_key])
            )

        return self.engines[db_key]

    def get_session_factory(self, db_key: str):
        """Returns a session factory with understated confidence."""
        self._get_engine(db_key)  # Ensure engine exists
        return self.session_factories[db_key]

    @contextmanager
    def session_scope(self, db_key: str):
        """Provides a session with elegantly managed lifecycle."""
        session = self.get_session_factory(db_key)()
        start_time = time.time()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Session error for {db_key}: {e}")
            raise
        finally:
            session.close()

    def try_source_connections(self, priority_list: list) -> Optional[str]:
        """Attempts connections with sophisticated fallback mechanics."""
        for db_key in priority_list:
            try:
                with self.session_scope(db_key) as session:
                    result = session.execute(text('SELECT 1'))
                    logger.info(f"Successfully connected to {db_key} {result}")
                    return db_key
            except Exception as e:
                logger.warning(f"Failed to connect to {db_key}: {e}")

        logger.error(f"Failed to connect to any database in {priority_list}")
        return None


# Singleton pattern with a dash of sophistication
_db_manager = None


def get_db_manager() -> DatabaseManager:
    """Retrieves the database manager with lazy instantiation."""
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager


# Redis client with refined simplicity
def get_redis_client():
    """Returns a Redis client with quiet competence."""
    return redis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        db=settings.REDIS_DB,
        decode_responses=True
    )
