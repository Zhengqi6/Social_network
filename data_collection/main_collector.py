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
from pathlib import Path

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_collection.blockchain.lens_collector import LensCollector

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
        self.collectors = {}
        
        # Initialize components
        self._initialize_collectors()
        # JSON-only pipeline, no DB initialization
        
        logger.info("Main data collector initialized")
    
    def _initialize_collectors(self):
        """Initialize data collectors for different platforms"""
        try:
            # Initialize Lens GraphQL collector (recommended path)
            self.collectors["lens_graphql"] = LensCollector(use_api=True)
            logger.info("Lens GraphQL collector initialized")
            

            
        except Exception as e:
            logger.error(f"Error initializing collectors: {e}")
            # Continue with available collectors
    
    def _initialize_storage(self):
        """No-op: DBs removed as per JSON-only design."""
        return
    
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
        logger.info("Starting JSON-only data collection from Lens GraphQL")
        
        all_data = {}
        
        # Collect from Lens via GraphQL (profiles + publications + follows)
        if "lens_graphql" in self.collectors and self.collectors["lens_graphql"]:
            try:
                lens_collector: LensCollector = self.collectors["lens_graphql"]
                results = await lens_collector.collect_all(
                    profile_limit=max_profiles,
                    pub_limit=max_posts_per_profile * max_profiles,
                    follow_per_profile=50,
                )
                all_data["lens_graphql"] = results
            except Exception as e:
                logger.error(f"Error collecting from Lens GraphQL: {e}")
                all_data["lens_graphql"] = {"error": str(e)}
        

        
        logger.info("Data collection from all platforms completed")
        return all_data
    

        
        all_platform_data = {}
        
        # Collect from Lens Chain
        try:
            lens_data = await self.collect_lens_chain_data(max_profiles, max_posts_per_profile)
            all_platform_data["lens_chain"] = lens_data
        except Exception as e:
            logger.error(f"Failed to collect Lens Chain data: {e}")
            all_platform_data["lens_chain"] = {}
        
        # TODO: Add other platforms (Zora, etc.)
        
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
        """JSON-only: return empty stats container."""
        return {}
    
    def close(self):
        """Close all connections and cleanup"""
        try:
            # Close collectors
            for collector_name, collector in self.collectors.items():
                if hasattr(collector, 'close'):
                    collector.close()
                logger.info(f"Closed {collector_name} collector")
            
            
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
                
                # Create follow relationships between users (mock data for now)
                if lens_chain_data.get("accounts") and len(lens_chain_data["accounts"]) > 1:
                    await self.storage["neo4j"].create_follow_relationships_mock(lens_chain_data["accounts"])
            
            # Store in Redis for caching if available
            if self.storage.get("redis"):
                if lens_chain_data.get("accounts"):
                    await self.storage["redis"].cache_profiles(lens_chain_data["accounts"])
            
            logger.info("Lens Chain data stored successfully")
            
        except Exception as e:
            logger.error(f"Error storing Lens Chain data: {e}")
    



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
                    print(f"  Publications: {len(data.get('publications', []))}")
                    print(f"  Follows: {len(data.get('follows', []))}")
                else:
                    print(f"\n{platform.upper()}: No data collected")
    
    finally:
        collector.close()


if __name__ == "__main__":
    # Set up logging
    try:
        base_dir = Path(__file__).resolve().parents[1]
    except Exception:
        base_dir = Path.cwd()
    logs_dir = base_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    logger.add(
        str(logs_dir / "data_collection.log"),
        rotation="1 day",
        retention="7 days",
        level="INFO",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
    )
    
    # Run main function
    asyncio.run(main())
