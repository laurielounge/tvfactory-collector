# infrastructure/database.py
import asyncio
import time
from contextlib import asynccontextmanager, contextmanager
from typing import List, Dict, Optional, AsyncGenerator

from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, AsyncEngine
from sqlalchemy.orm import scoped_session, sessionmaker

from config.settings import settings
from core.logger import logger
from utils.yaml_utils import load_configuration


class DatabaseManager:
    """Manages database connections with aristocratic composure."""

    def __init__(self, config_path: str = "config/databases.yml"):
        self.config = load_configuration(config_path)
        logger.info(f"Database configuration: {self.config}")
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


class AsyncDatabaseManager:
    """Provides sophisticated async database connections with connection pooling"""

    def __init__(self):
        self.engines: Dict[str, AsyncEngine] = {}
        self._init_lock = asyncio.Lock()
        self._initialized = False
        self._engine: Optional[AsyncEngine] = None

    async def initialize(self):
        """Initialize database connections if not already done"""
        async with self._init_lock:
            if self._initialized:
                return

            try:
                # Initialize the standard connections we'll use
                await self._create_engine('TVFACTORY',
                                          settings.MARIADB_DSN)

                self._initialized = True
                logger.info("AsyncDatabaseManager initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize AsyncDatabaseManager: {e}")
                raise

    async def _create_engine(self, db_key: str, connection_string: str) -> None:
        """Create an async SQLAlchemy engine with optimal configuration"""
        try:
            engine = create_async_engine(
                connection_string,
                pool_pre_ping=True,
                pool_recycle=3600,
                pool_size=50,
                max_overflow=20,
                future=True,
                echo=settings.SQL_ECHO
            )
            self.engines[db_key] = engine
            logger.debug(f"Created async engine for {db_key}")
        except Exception as e:
            logger.error(f"Failed to create async engine for {db_key}: {e}")
            raise

    @asynccontextmanager
    async def async_session_scope(self, db_key: str) -> AsyncGenerator[AsyncSession, None]:
        """Elegant context manager for async database sessions"""
        if not self._initialized:
            await self.initialize()

        if db_key not in self.engines:
            raise ValueError(f"No engine found for {db_key}")

        engine = self.engines[db_key]
        async_session_factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        session = async_session_factory()

        try:
            yield session
        except Exception as e:
            await session.rollback()
            logger.error(f"Error during database session: {e}")
            raise
        finally:
            await session.close()

    async def try_async_source_connections(self, db_keys: List[str]) -> Optional[str]:
        """Attempt to connect to any of the specified source databases"""
        if not self._initialized:
            await self.initialize()

        for db_key in db_keys:
            if db_key not in self.engines:
                logger.warning(f"No engine configured for {db_key}")
                continue

            try:
                async with self.async_session_scope(db_key) as session:
                    # Simple test query
                    await session.execute(text("SELECT 1"))
                    return db_key
            except Exception as e:
                logger.warning(f"Failed to connect to {db_key}: {e}")

        logger.error("No available source database connections")
        return None

    async def close(self):
        """Close all database connections with understated efficiency"""
        if not self._initialized:
            return

        for db_key, engine in self.engines.items():
            try:
                await engine.dispose()
                logger.debug(f"Closed connection pool for {db_key}")
            except Exception as e:
                logger.error(f"Error closing {db_key} connection: {e}")

        self._initialized = False
        logger.info("AsyncDatabaseManager connections closed")

    async def shutdown(self):
        if self._engine:
            await self._engine.dispose()
            self._engine = None  # Helps GC
