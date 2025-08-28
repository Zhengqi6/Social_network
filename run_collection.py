#!/usr/bin/env python3
"""
Simple script to test the data collection system
"""
import asyncio
import sys
import os
from loguru import logger

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data_collection.main_collector import MainDataCollector


async def test_collection():
    """Test the data collection system with minimal data"""
    logger.info("Starting test data collection...")
    
    # Initialize collector (without API key for testing)
    collector = MainDataCollector()
    
    try:
        # Test with small amounts of data
        logger.info("Testing Lens data collection...")
        lens_data = await collector.collect_lens_data(
            max_profiles=5,  # Just 5 profiles for testing
            max_posts_per_profile=3  # Just 3 posts per profile
        )
        
        # Print results
        print("\n=== Test Collection Results ===")
        print(f"Lens Profiles: {len(lens_data.get('profiles', []))}")
        print(f"Lens Posts: {len(lens_data.get('posts', []))}")
        print(f"Lens Follows: {len(lens_data.get('follows', []))}")
        print(f"Lens Engagements: {len(lens_data.get('engagements', []))}")
        
        if lens_data.get('profiles'):
            print(f"\nSample Profile:")
            profile = lens_data['profiles'][0]
            print(f"  Handle: {profile.get('handle', 'N/A')}")
            print(f"  Name: {profile.get('name', 'N/A')}")
            print(f"  Followers: {profile.get('total_followers', 'N/A')}")
        
        if lens_data.get('posts'):
            print(f"\nSample Post:")
            post = lens_data['posts'][0]
            print(f"  Type: {post.get('type', 'N/A')}")
            print(f"  Content: {post.get('content', 'N/A')[:100]}...")
            print(f"  Engagement: {post.get('total_mirrors', 0)} mirrors, {post.get('total_comments', 0)} comments")
        
        logger.info("Test collection completed successfully!")
        
    except Exception as e:
        logger.error(f"Test collection failed: {e}")
        print(f"\n❌ Test failed: {e}")
        return False
    
    finally:
        collector.close()
    
    return True


async def test_storage_only():
    """Test only the storage systems without data collection"""
    logger.info("Testing storage systems...")
    
    try:
        from data_collection.storage.database import DatabaseManager
        
        # Initialize database manager
        db_manager = DatabaseManager()
        
        # Test basic operations
        print("\n=== Storage System Test ===")
        
        # Test MongoDB
        try:
            mongo_storage = db_manager.mongodb_db
            print("✅ MongoDB: Connected successfully")
        except Exception as e:
            print(f"❌ MongoDB: Connection failed - {e}")
        
        # Test Neo4j
        try:
            with db_manager.neo4j_driver.session() as session:
                result = session.run("RETURN 1 as test")
                print("✅ Neo4j: Connected successfully")
        except Exception as e:
            print(f"❌ Neo4j: Connection failed - {e}")
        
        # Test Redis
        try:
            db_manager.redis_client.ping()
            print("✅ Redis: Connected successfully")
        except Exception as e:
            print(f"❌ Redis: Connection failed - {e}")
        
        db_manager.close()
        logger.info("Storage system test completed")
        
    except Exception as e:
        logger.error(f"Storage system test failed: {e}")
        print(f"❌ Storage test failed: {e}")


def main():
    """Main function"""
    print("🚀 Decentralized Social Recommendation - System Test")
    print("=" * 60)
    
    # Set up basic logging
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="{message}")
    
    # Check command line arguments
    if len(sys.argv) > 1 and sys.argv[1] == "--storage-only":
        # Test only storage systems
        asyncio.run(test_storage_only())
    else:
        # Test full collection system
        success = asyncio.run(test_collection())
        
        if success:
            print("\n🎉 All tests passed! The system is ready for data collection.")
            print("\nNext steps:")
            print("1. Set up your .env file with proper credentials")
            print("2. Run: python data_collection/main_collector.py --max-profiles 100 --max-posts 50")
            print("3. Check logs/data_collection.log for detailed information")
        else:
            print("\n💥 Some tests failed. Please check the error messages above.")
            print("\nTroubleshooting:")
            print("1. Verify all services are running (MongoDB, Neo4j, Redis)")
            print("2. Check your .env configuration")
            print("3. Ensure you have proper network access")


if __name__ == "__main__":
    main()
