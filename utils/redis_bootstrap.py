from sqlalchemy import select, func

from core.logger import logger


async def sync_redis_id_counter(redis, db, redis_key: str, model, id_column, db_key: str = "TVFACTORY"):
    """
    Ensure Redis ID counter reflects the max of (Redis, DB) + 1.

    Args:
        redis: Redis client
        db: DB manager with async_session_scope()
        redis_key: The Redis key to set, e.g. "global:next_impression_id"
        model: SQLAlchemy model (e.g., Impression or WebHit)
        id_column: model.id column
        db_key: DB bind name, default "TVFACTORY"
    """
    try:
        current = await redis.get(redis_key)
        current_val = int(current) if current is not None else 0

        async with db.async_session_scope(db_key) as session:
            result = await session.execute(select(func.max(id_column)))
            db_max = result.scalar() or 0

        new_val = max(current_val, db_max) + 1
        await redis.set(redis_key, new_val)

        logger.info(f"[REDIS BOOTSTRAP] {redis_key} set to {new_val} (max of Redis={current_val}, DB={db_max})")

    except Exception as e:
        logger.error(f"[REDIS BOOTSTRAP] Failed to initialise {redis_key}: {e}")
