#!/usr/bin/env python3
import asyncio
import sys
from pathlib import Path
from loguru import logger

# add project root
sys.path.append(str(Path(__file__).resolve().parents[1]))
from data_collection.blockchain.lens_collector import LensCollector

async def main():
    logger.add("logs/test_collector.log", rotation="50 MB", retention="3 days")
    collector = LensCollector(use_api=True)

    logger.info("Test 1: Health check")
    health_q = "query { health maintenance }"
    res = await collector._make_lens_api_request(health_q)
    logger.info(f"health resp: {res}")

    logger.info("Test 2: collect_profiles limit=5")
    try:
        profiles = await collector.collect_profiles(limit=5)
        logger.info(f"profiles collected: {len(profiles)}")
        if profiles:
            logger.info(f"sample: {profiles[0]}")
    except Exception as e:
        logger.error(f"collect_profiles failed: {e}")

    logger.info("Test 3: collect_publications limit=5")
    try:
        pubs = await collector.collect_publications(limit=5)
        logger.info(f"publications collected: {len(pubs)}")
        if pubs:
            logger.info(f"sample: {pubs[0]}")
    except Exception as e:
        logger.error(f"collect_publications failed: {e}")

if __name__ == "__main__":
    asyncio.run(main())
