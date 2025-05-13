import asyncio

from infrastructure.circuit_breaker import MariaDBHealthChecker, HealthCheckRegistry
from infrastructure.database import AsyncDatabaseManager


async def main():
    db_manager = AsyncDatabaseManager()
    await db_manager.initialize()

    registry = HealthCheckRegistry(
        MariaDBHealthChecker(db_manager)
    )
    await registry.assert_healthy_or_exit()


if __name__ == "__main__":
    asyncio.run(main())
