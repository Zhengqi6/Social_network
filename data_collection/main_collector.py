"""
Main data collection script for decentralized social recommendation project
"""
import asyncio
import time
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Any
from loguru import logger
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_collection.blockchain.lens_chain_collector import LensChainCollector
from data_collection.blockchain.farcaster_collector import FarcasterCollector
from data_collection.storage.database import DatabaseManager, MongoDBStorage, Neo4jStorage, RedisStorage
from config.settings import COLLECTION_CONFIG


class MainDataCollector:
    """Main data collection orchestrator"""
    
    def __init__(self, api_key: str = None):
        """
        Initialize main data collector
        
        Args:
            api_key: API key for platforms that require it
        """
        self.api_key = api_key
        self.db_manager = None
        self.collectors = {}
        self.storage = {}
        
        # Initialize components
        self._initialize_collectors()
        self._initialize_storage()
        
        logger.info("Main data collector initialized")
    
    def _initialize_collectors(self):
        """Initialize data collectors for different platforms"""
        try:
            # Initialize Lens Chain collector (blockchain-based)
            self.collectors["lens_chain"] = LensChainCollector()
            logger.info("Lens Chain collector initialized")
            
            # Initialize Farcaster collector
            self.collectors["farcaster"] = FarcasterCollector(api_key=self.api_key)
            logger.info("Farcaster collector initialized")
            
        except Exception as e:
            logger.error(f"Error initializing collectors: {e}")
            # Continue with available collectors
    
    def _initialize_storage(self):
        """Initialize storage systems"""
        try:
            self.db_manager = DatabaseManager()
            
            # Initialize different storage types
            self.storage["mongodb"] = MongoDBStorage(self.db_manager)
            
            # Try to initialize Neo4j and Redis, but continue if they fail
            try:
                self.storage["neo4j"] = Neo4jStorage(self.db_manager)
                logger.info("Neo4j storage initialized")
            except Exception as e:
                logger.warning(f"Neo4j storage failed to initialize: {e}")
                self.storage["neo4j"] = None
            
            try:
                self.storage["redis"] = RedisStorage(self.db_manager)
                logger.info("Redis storage initialized")
            except Exception as e:
                logger.warning(f"Redis storage failed to initialize: {e}")
                self.storage["redis"] = None
            
            logger.info("Storage systems initialized (some may be unavailable)")
            
        except Exception as e:
            logger.error(f"Error initializing storage: {e}")
            # Continue with available storage systems
    
    # Lens API collector removed - using Lens Chain instead
    
    async def collect_lens_chain_data(self, max_accounts: int = 100, 
                                     max_posts: int = 100, 
                                     max_interactions: int = 100) -> Dict[str, Any]:
        """
        Collect data from Lens Chain (blockchain-based)
        
        Args:
            max_accounts: Maximum accounts to collect
            max_posts: Maximum posts to collect
            max_interactions: Maximum interactions to collect
        
        Returns:
            Collected data dictionary
        """
        logger.info(f"Starting Lens Chain data collection: {max_accounts} accounts, {max_posts} posts, {max_interactions} interactions")
        
        try:
            # Collect data from Lens Chain
            lens_chain_data = await self.collectors["lens_chain"].collect_all_data(
                max_accounts=max_accounts,
                max_posts=max_posts,
                max_interactions=max_interactions
            )
            
            # Store data in different storage systems
            await self._store_lens_chain_data(lens_chain_data)
            
            logger.info("Lens Chain data collection completed successfully")
            return lens_chain_data
            
        except Exception as e:
            logger.error(f"Error collecting Lens Chain data: {e}")
            return {}
    
    async def collect_farcaster_data(self, max_users: int = 100, 
                                    max_casts: int = 100, 
                                    max_reactions: int = 100) -> Dict[str, Any]:
        """
        Collect data from Farcaster
        
        Args:
            max_users: Maximum users to collect
            max_casts: Maximum casts to collect
            max_reactions: Maximum reactions to collect
        
        Returns:
            Collected data dictionary
        """
        logger.info(f"Starting Farcaster data collection: {max_users} users, {max_casts} casts, {max_reactions} reactions")
        
        try:
            # Collect data from Farcaster
            farcaster_data = await self.collectors["farcaster"].collect_all_data(
                max_users=max_users,
                max_casts=max_casts,
                max_reactions=max_reactions
            )
            
            # Store data in different storage systems
            await self._store_farcaster_data(farcaster_data)
            
            logger.info("Farcaster data collection completed successfully")
            return farcaster_data
            
        except Exception as e:
            logger.error(f"Error collecting Farcaster data: {e}")
            return {}
    
    async def collect_all_platforms(self, max_profiles: int = 100, 
                                   max_posts_per_profile: int = 50) -> Dict[str, Any]:
        """
        Collect data from all available platforms
        
        Args:
            max_profiles: Maximum profiles to collect
            max_posts_per_profile: Maximum posts per profile
        
        Returns:
            Collected data from all platforms
        """
        logger.info("Starting data collection from all platforms")
        
        all_data = {}
        
        # Collect from Lens Chain (blockchain)
        if "lens_chain" in self.collectors and self.collectors["lens_chain"]:
            try:
                lens_chain_data = await self.collect_lens_chain_data(
                    max_accounts=max_profiles,
                    max_posts=max_posts_per_profile * max_profiles,
                    max_interactions=max_posts_per_profile * max_profiles
                )
                all_data["lens_chain"] = lens_chain_data
            except Exception as e:
                logger.error(f"Error collecting from Lens Chain: {e}")
                all_data["lens_chain"] = {"error": str(e)}
        
        # Collect from Lens Chain (blockchain)
        if "lens_chain" in self.collectors and self.collectors["lens_chain"]:
            try:
                lens_chain_data = await self.collect_lens_chain_data(
                    max_accounts=max_profiles,
                    max_posts=max_posts_per_profile * max_profiles,
                    max_interactions=max_posts_per_profile * max_profiles
                )
                all_data["lens_chain"] = lens_chain_data
            except Exception as e:
                logger.error(f"Error collecting from Lens Chain: {e}")
                all_data["lens_chain"] = {"error": str(e)}
        
        # Collect from Farcaster
        if "farcaster" in self.collectors and self.collectors["farcaster"]:
            try:
                farcaster_data = await self.collect_farcaster_data(
                    max_users=max_profiles,
                    max_casts=max_posts_per_profile * max_profiles,
                    max_reactions=max_posts_per_profile * max_profiles
                )
                all_data["farcaster"] = farcaster_data
            except Exception as e:
                logger.error(f"Error collecting from Farcaster: {e}")
                all_data["farcaster"] = {"error": str(e)}
        
        logger.info("Data collection from all platforms completed")
        return all_data
    
    # Lens data storage method removed - using Lens Chain instead
    
    async def collect_all_platforms(self, max_profiles: int = 100, 
                                   max_posts_per_profile: int = 50) -> Dict[str, Dict[str, Any]]:
        """
        Collect data from all platforms
        
        Args:
            max_profiles: Maximum number of profiles per platform
            max_posts_per_profile: Maximum posts per profile
        
        Returns:
            Dictionary containing data from all platforms
        """
        logger.info("Starting data collection from all platforms")
        
        all_platform_data = {}
        
        # Collect from Lens Chain
        try:
            lens_data = await self.collect_lens_chain_data(max_profiles, max_posts_per_profile)
            all_platform_data["lens_chain"] = lens_data
        except Exception as e:
            logger.error(f"Failed to collect Lens Chain data: {e}")
            all_platform_data["lens_chain"] = {}
        
        # Collect from Farcaster
        try:
            farcaster_data = await self.collect_farcaster_data(max_profiles, max_posts_per_profile)
            all_platform_data["farcaster"] = farcaster_data
        except Exception as e:
            logger.error(f"Failed to collect Farcaster data: {e}")
            all_platform_data["farcaster"] = {}
        
        # TODO: Add other platforms
        # try:
        #     farcaster_data = await self.collect_farcaster_data(max_profiles, max_posts_per_profile)
        #     all_platform_data["farcaster"] = farcaster_data
        # except Exception as e:
        #     logger.error(f"Failed to collect Farcaster data: {e}")
        #     all_platform_data["farcaster"] = {}
        
        logger.info("Data collection from all platforms completed")
        return all_platform_data
    
    async def continuous_collection(self, interval_minutes: int = 60, 
                                  max_profiles: int = 50, 
                                  max_posts_per_profile: int = 25):
        """
        Run continuous data collection at specified intervals
        
        Args:
            interval_minutes: Collection interval in minutes
            max_profiles: Maximum profiles per collection cycle
            max_posts_per_profile: Maximum posts per profile per cycle
        """
        logger.info(f"Starting continuous data collection every {interval_minutes} minutes")
        
        try:
            while True:
                start_time = time.time()
                
                logger.info(f"Starting collection cycle at {datetime.now()}")
                
                # Collect data from all platforms
                await self.collect_all_platforms(max_profiles, max_posts_per_profile)
                
                # Calculate time until next collection
                elapsed_time = time.time() - start_time
                sleep_time = max(0, (interval_minutes * 60) - elapsed_time)
                
                logger.info(f"Collection cycle completed in {elapsed_time:.2f} seconds. "
                           f"Next collection in {sleep_time/60:.1f} minutes")
                
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                
        except KeyboardInterrupt:
            logger.info("Continuous collection stopped by user")
        except Exception as e:
            logger.error(f"Error in continuous collection: {e}")
    
    async def get_collection_stats(self) -> Dict[str, Any]:
        """Get statistics about collected data"""
        try:
            stats = {}
            
            # Get MongoDB stats
            profiles = await self.storage["mongodb"].get_profiles(limit=1)
            if profiles:
                # Get total counts from MongoDB
                # This is a simplified approach - in production you'd want proper aggregation queries
                stats["total_profiles"] = len(profiles)  # This is just sample size
                stats["total_posts"] = len(await self.storage["mongodb"].get_posts(limit=1))
            
            # Get Neo4j stats
            try:
                with self.db_manager.neo4j_driver.session() as session:
                    # Count nodes
                    result = session.run("MATCH (n:User) RETURN count(n) as user_count")
                    stats["neo4j_users"] = result.single()["user_count"]
                    
                    result = session.run("MATCH (n:Post) RETURN count(n) as post_count")
                    stats["neo4j_posts"] = result.single()["post_count"]
                    
                    # Count relationships
                    result = session.run("MATCH ()-[r:FOLLOWS]->() RETURN count(r) as follow_count")
                    stats["neo4j_follows"] = result.single()["follow_count"]
                    
            except Exception as e:
                logger.error(f"Error getting Neo4j stats: {e}")
                stats["neo4j_error"] = str(e)
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting collection stats: {e}")
            return {"error": str(e)}
    
    def close(self):
        """Close all connections and cleanup"""
        try:
            # Close collectors
            for collector_name, collector in self.collectors.items():
                if hasattr(collector, 'close'):
                    collector.close()
                logger.info(f"Closed {collector_name} collector")
            
            # Close storage
            if self.db_manager:
                self.db_manager.close()
            
            logger.info("Main data collector closed")
            
        except Exception as e:
            logger.error(f"Error closing main collector: {e}")
    
    async def _store_lens_chain_data(self, lens_chain_data: Dict[str, List]):
        """Store Lens Chain data in all storage systems"""
        try:
            # Store in MongoDB
            if lens_chain_data.get("accounts"):
                await self.storage["mongodb"].store_profiles(lens_chain_data["accounts"])
            
            if lens_chain_data.get("posts"):
                await self.storage["mongodb"].store_posts(lens_chain_data["posts"])
            
            if lens_chain_data.get("interactions"):
                await self.storage["mongodb"].store_engagements(lens_chain_data["interactions"])
            
            # Store in Neo4j (graph database) if available
            if self.storage.get("neo4j"):
                if lens_chain_data.get("accounts"):
                    await self.storage["neo4j"].create_user_nodes(lens_chain_data["accounts"])
                
                if lens_chain_data.get("posts"):
                    await self.storage["neo4j"].create_post_nodes(lens_chain_data["posts"])
                
                if lens_chain_data.get("interactions"):
                    await self.storage["neo4j"].create_interaction_relationships(lens_chain_data["interactions"])
            
            # Store in Redis for caching if available
            if self.storage.get("redis"):
                if lens_chain_data.get("accounts"):
                    await self.storage["redis"].cache_profiles(lens_chain_data["accounts"])
            
            logger.info("Lens Chain data stored successfully")
            
        except Exception as e:
            logger.error(f"Error storing Lens Chain data: {e}")
    
    async def _store_farcaster_data(self, farcaster_data: Dict[str, List]):
        """Store Farcaster data in all storage systems"""
        try:
            # Store in MongoDB
            if farcaster_data.get("users"):
                await self.storage["mongodb"].store_profiles(farcaster_data["users"])
            
            if farcaster_data.get("casts"):
                await self.storage["mongodb"].store_posts(farcaster_data["casts"])
            
            if farcaster_data.get("reactions"):
                await self.storage["mongodb"].store_engagements(farcaster_data["reactions"])
            
            # Store in Neo4j (graph database) if available
            if self.storage.get("neo4j"):
                if farcaster_data.get("users"):
                    await self.storage["neo4j"].create_user_nodes(farcaster_data["users"])
                
                if farcaster_data.get("casts"):
                    await self.storage["neo4j"].create_post_nodes(farcaster_data["casts"])
                
                if farcaster_data.get("reactions"):
                    await self.storage["neo4j"].create_reaction_relationships(farcaster_data["reactions"])
            
            # Store in Redis for caching if available
            if self.storage.get("redis"):
                if farcaster_data.get("users"):
                    await self.storage["redis"].cache_profiles(farcaster_data["users"])
            
            logger.info("Farcaster data stored successfully")
            
        except Exception as e:
            logger.error(f"Error storing Farcaster data: {e}")


async def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Decentralized Social Recommendation Data Collector")
    parser.add_argument("--api-key", help="API key for platforms that require it")
    parser.add_argument("--max-profiles", type=int, default=100, help="Maximum profiles to collect")
    parser.add_argument("--max-posts", type=int, default=50, help="Maximum posts per profile")
    parser.add_argument("--continuous", action="store_true", help="Run continuous collection")
    parser.add_argument("--interval", type=int, default=60, help="Collection interval in minutes (for continuous mode)")
    parser.add_argument("--stats", action="store_true", help="Show collection statistics")
    
    args = parser.parse_args()
    
    # Initialize collector
    collector = MainDataCollector(api_key=args.api_key)
    
    try:
        if args.stats:
            # Show collection statistics
            stats = await collector.get_collection_stats()
            print("\n=== Collection Statistics ===")
            for key, value in stats.items():
                print(f"{key}: {value}")
        
        elif args.continuous:
            # Run continuous collection
            await collector.continuous_collection(
                interval_minutes=args.interval,
                max_profiles=args.max_profiles,
                max_posts_per_profile=args.max_posts
            )
        
        else:
            # Run single collection cycle
            logger.info("Starting single data collection cycle")
            all_data = await collector.collect_all_platforms(
                max_profiles=args.max_profiles,
                max_posts_per_profile=args.max_posts
            )
            
            # Print summary
            print("\n=== Data Collection Summary ===")
            for platform, data in all_data.items():
                if data:
                    print(f"\n{platform.upper()}:")
                    print(f"  Profiles: {len(data.get('profiles', []))}")
                    print(f"  Posts: {len(data.get('posts', []))}")
                    print(f"  Follows: {len(data.get('follows', []))}")
                    print(f"  Engagements: {len(data.get('engagements', []))}")
                else:
                    print(f"\n{platform.upper()}: No data collected")
    
    finally:
        collector.close()


if __name__ == "__main__":
    # Set up logging
    logger.add(
        "logs/data_collection.log",
        rotation="1 day",
        retention="7 days",
        level="INFO",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
    )
    
    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)
    
    # Run main function
    asyncio.run(main())
