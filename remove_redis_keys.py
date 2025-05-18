#!/usr/bin/env python3
import asyncio

import redis.asyncio as redis

from config.settings import settings


async def clean_tracking_keys():
    """Clean out impression and dedupe keys for a fresh start."""
    # Connect to Redis
    r = await redis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        password=settings.REDIS_PASSWORD,
        decode_responses=True
    )

    try:
        # Get all impression tracking keys
        imp_keys = await r.keys("imp:*")
        print(f"Found {len(imp_keys)} impression tracking keys")

        # Get all dedupe keys
        dedupe_keys = await r.keys("dedupe:webhit:*")
        print(f"Found {len(dedupe_keys)} dedupe keys")

        # Count total keys to remove
        total_keys = len(imp_keys) + len(dedupe_keys)
        print(f"Preparing to remove {total_keys} keys")

        if total_keys == 0:
            print("No keys to remove. All clean!")
            return

        # Ask for confirmation
        confirm = input(f"Are you sure you want to delete {total_keys} keys? (y/n): ")
        if confirm.lower() != 'y':
            print("Operation cancelled.")
            return

        # Process in batches for better performance
        deleted_count = 0

        # Delete impression keys
        if imp_keys:
            for i in range(0, len(imp_keys), 1000):
                batch = imp_keys[i:i + 1000]
                await r.delete(*batch)
                deleted_count += len(batch)
                print(f"Deleted {deleted_count}/{total_keys} keys")

        # Delete dedupe keys
        if dedupe_keys:
            for i in range(0, len(dedupe_keys), 1000):
                batch = dedupe_keys[i:i + 1000]
                await r.delete(*batch)
                deleted_count += len(batch)
                print(f"Deleted {deleted_count}/{total_keys} keys")

        print(f"Successfully removed {deleted_count} keys. Your slate is clean!")

    finally:
        await r.close()


if __name__ == "__main__":
    asyncio.run(clean_tracking_keys())
