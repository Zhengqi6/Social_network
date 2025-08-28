#!/usr/bin/env python3
"""
Main data collection script for decentralized social recommendation system
"""
import asyncio
import os
import sys
import time
from datetime import datetime
from loguru import logger

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data_collection.main_collector import MainDataCollector
from config.settings import COLLECTION_CONFIG


async def run_single_collection(collector: MainDataCollector, 
                               max_profiles: int = 50, 
                               max_posts_per_profile: int = 25):
    """Run a single data collection cycle"""
    logger.info("🚀 Starting single data collection cycle")
    logger.info(f"📊 Collection parameters: {max_profiles} profiles, {max_posts_per_profile} posts per profile")
    
    start_time = time.time()
    
    try:
        # Collect data from all platforms
        all_data = await collector.collect_all_platforms(max_profiles, max_posts_per_profile)
        
        # Calculate collection statistics
        total_profiles = 0
        total_posts = 0
        total_interactions = 0
        
        for platform, data in all_data.items():
            if isinstance(data, dict) and "error" not in data:
                if platform == "lens":
                    total_profiles += len(data.get("profiles", []))
                    total_posts += len(data.get("posts", []))
                    total_interactions += len(data.get("engagements", []))
                elif platform == "lens_chain":
                    total_profiles += len(data.get("accounts", []))
                    total_posts += len(data.get("posts", []))
                    total_interactions += len(data.get("interactions", []))
                elif platform == "farcaster":
                    total_profiles += len(data.get("users", []))
                    total_posts += len(data.get("casts", []))
                    total_interactions += len(data.get("reactions", []))
        
        elapsed_time = time.time() - start_time
        
        logger.info("✅ Data collection completed successfully!")
        logger.info(f"📊 Collection Summary:")
        logger.info(f"   Total Profiles: {total_profiles}")
        logger.info(f"   Total Posts: {total_posts}")
        logger.info(f"   Total Interactions: {total_interactions}")
        logger.info(f"   Time Elapsed: {elapsed_time:.2f} seconds")
        logger.info(f"   Collection Rate: {total_profiles/elapsed_time:.2f} profiles/second")
        
        return True, all_data
        
    except Exception as e:
        logger.error(f"❌ Error during data collection: {e}")
        return False, {"error": str(e)}


async def run_continuous_collection(collector: MainDataCollector, 
                                  interval_minutes: int = 60,
                                  max_profiles: int = 25, 
                                  max_posts_per_profile: int = 10):
    """Run continuous data collection"""
    logger.info(f"🔄 Starting continuous data collection every {interval_minutes} minutes")
    logger.info(f"📊 Collection parameters: {max_profiles} profiles, {max_posts_per_profile} posts per profile")
    
    cycle_count = 0
    
    try:
        while True:
            cycle_count += 1
            cycle_start = time.time()
            
            logger.info(f"\n🔄 Collection Cycle #{cycle_count} - {datetime.now()}")
            logger.info("=" * 60)
            
            # Run single collection cycle
            success, data = await run_single_collection(collector, max_profiles, max_posts_per_profile)
            
            if success:
                logger.info(f"✅ Cycle #{cycle_count} completed successfully")
            else:
                logger.error(f"❌ Cycle #{cycle_count} failed")
            
            # Calculate time until next cycle
            cycle_elapsed = time.time() - cycle_start
            sleep_time = max(0, (interval_minutes * 60) - cycle_elapsed)
            
            logger.info(f"⏰ Cycle completed in {cycle_elapsed:.2f} seconds")
            
            if sleep_time > 0:
                logger.info(f"😴 Sleeping for {sleep_time/60:.1f} minutes until next cycle")
                await asyncio.sleep(sleep_time)
            else:
                logger.warning("⚠️  Cycle took longer than interval, starting next cycle immediately")
                
    except KeyboardInterrupt:
        logger.info("🛑 Continuous collection stopped by user")
        logger.info(f"📊 Total cycles completed: {cycle_count}")
    except Exception as e:
        logger.error(f"💥 Error in continuous collection: {e}")
        logger.info(f"📊 Cycles completed before error: {cycle_count}")


async def show_collection_stats(collector: MainDataCollector):
    """Show current collection statistics"""
    logger.info("📊 Getting collection statistics...")
    
    try:
        stats = await collector.get_collection_stats()
        
        logger.info("📈 Collection Statistics:")
        logger.info("=" * 40)
        
        for key, value in stats.items():
            if key == "error":
                logger.error(f"❌ Error: {value}")
            else:
                logger.info(f"   {key}: {value}")
                
    except Exception as e:
        logger.error(f"❌ Error getting statistics: {e}")


async def main():
    """Main function"""
    logger.info("🚀 Decentralized Social Recommendation Data Collector")
    logger.info("=" * 70)
    
    # Get configuration from environment or use defaults
    api_key = os.getenv("SOCIAL_API_KEY")
    max_profiles = int(os.getenv("MAX_PROFILES", "50"))
    max_posts_per_profile = int(os.getenv("MAX_POSTS_PER_PROFILE", "25"))
    interval_minutes = int(os.getenv("COLLECTION_INTERVAL_MINUTES", "60"))
    continuous_mode = os.getenv("CONTINUOUS_MODE", "false").lower() == "true"
    
    logger.info(f"🔑 API Key: {'✅ Set' if api_key else '❌ Not set'}")
    logger.info(f"👥 Max Profiles: {max_profiles}")
    logger.info(f"📝 Max Posts per Profile: {max_posts_per_profile}")
    logger.info(f"⏰ Collection Interval: {interval_minutes} minutes")
    logger.info(f"🔄 Continuous Mode: {'✅ Enabled' if continuous_mode else '❌ Disabled'}")
    
    # Initialize collector
    logger.info("\n🔧 Initializing data collector...")
    collector = MainDataCollector(api_key=api_key)
    
    try:
        if continuous_mode:
            # Run continuous collection
            await run_continuous_collection(
                collector, 
                interval_minutes, 
                max_profiles, 
                max_posts_per_profile
            )
        else:
            # Run single collection
            success, data = await run_single_collection(
                collector, 
                max_profiles, 
                max_posts_per_profile
            )
            
            if success:
                # Show statistics
                await show_collection_stats(collector)
                
                logger.info("\n🎉 Data collection completed successfully!")
                logger.info("📝 Next steps:")
                logger.info("   1. Check MongoDB for collected data")
                logger.info("   2. Check Neo4j for graph relationships")
                logger.info("   3. Check Redis for cached data")
                logger.info("   4. Start building recommendation models")
            else:
                logger.error("💥 Data collection failed")
                return False
        
        return True
        
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}")
        return False
        
    finally:
        # Cleanup
        logger.info("🧹 Cleaning up...")
        collector.close()
        logger.info("✅ Cleanup completed")


if __name__ == "__main__":
    try:
        success = asyncio.run(main())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("🛑 Script interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"💥 Script failed: {e}")
        sys.exit(1)
