import asyncio

from sqlalchemy import select, func, and_, text

from core.logger import logger
from infrastructure.database import AsyncDatabaseManager
from models.impression import FinishedImpression as Impression


async def test_impression_query():
    db = AsyncDatabaseManager()
    await db.initialize()  # Critical: Initializes async engines

    async with db.async_session_scope("TVFACTORY") as session:
        try:
            stmt = (
                select(Impression.id)
                .where(
                    and_(
                        Impression.client_id == 281,
                        Impression.ipaddress == "::ffff:161.29.131.67",
                        Impression.timestmp > func.now() - text("INTERVAL 30 DAY"),
                    )
                )
                .order_by(Impression.timestmp.desc())
                .limit(1)
            )
            result = await session.execute(stmt)
            row = result.scalar_one_or_none()  # This is correct and not awaited

            if not row:
                logger.debug("[WEBHIT] No matching impression found")
                return None

            logger.info(f"[WEBHIT] Found impression_id: {row}")
            return row

        except Exception as e:
            logger.warning(f"[QUERY ERROR] {e}")
            return None


if __name__ == "__main__":
    asyncio.run(test_impression_query())
