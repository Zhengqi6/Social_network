"""
Database storage module for decentralized social recommendation data
"""
import json
import asyncio
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
import pandas as pd
from pymongo import MongoClient, UpdateOne
from pymongo.errors import DuplicateKeyError
from bson import json_util, ObjectId
from neo4j import GraphDatabase
from redis import Redis
from loguru import logger

from config.settings import DATABASE_CONFIG


class DatabaseManager:
    """Central database manager for all storage operations"""
    
    def __init__(self):
        """Initialize database connections"""
        self.mongodb_client = None
        self.neo4j_driver = None
        self.redis_client = None
        self._initialize_connections()
    
    def _initialize_connections(self):
        """Initialize database connections"""
        try:
            # MongoDB connection
            self.mongodb_client = MongoClient(DATABASE_CONFIG["mongodb"]["uri"])
            self.mongodb_db = self.mongodb_client[DATABASE_CONFIG["mongodb"]["database"]]
            logger.info("MongoDB connection established")
            
            # Neo4j connection
            neo4j_config = DATABASE_CONFIG["neo4j"]
            self.neo4j_driver = GraphDatabase.driver(
                neo4j_config["uri"],
                auth=(neo4j_config["user"], neo4j_config["password"])
            )
            # Test connection
            with self.neo4j_driver.session() as session:
                session.run("RETURN 1")
            logger.info("Neo4j connection established")
            
            # Redis connection
            redis_config = DATABASE_CONFIG["redis"]
            self.redis_client = Redis(
                host=redis_config["host"],
                port=redis_config["port"],
                db=redis_config["db"],
                password=redis_config["password"],
                decode_responses=True
            )
            # Test connection
            self.redis_client.ping()
            logger.info("Redis connection established")
            
        except Exception as e:
            logger.error(f"Failed to initialize database connections: {e}")
            raise
    
    def close(self):
        """Close all database connections"""
        if self.mongodb_client:
            self.mongodb_client.close()
        if self.neo4j_driver:
            self.neo4j_driver.close()
        if self.redis_client:
            self.redis_client.close()
        logger.info("All database connections closed")


class MongoDBStorage:
    """MongoDB storage operations"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager.mongodb_db
        self.collections = DATABASE_CONFIG["mongodb"]["collections"]
    
    def _bulk_upsert(self, col, docs, uniq_key):
        """幂等批量写入；uniq_key 如 'profile_id'/'publication_id'/'engagement_id'"""
        ops = []
        for d in docs:
            d = dict(d)
            d.pop('_id', None)              # 防止 ObjectId 再次冲突
            key = {uniq_key: d[uniq_key]}
            ops.append(UpdateOne(key, {'$set': d}, upsert=True))
        try:
            res = col.bulk_write(ops, ordered=False)
            logger.info(
                f"{col.name} upserted={res.upserted_count} matched={res.matched_count} modified={res.modified_count}"
            )
            return res.upserted_count + res.modified_count
        except errors.BulkWriteError as e:
            # 仅忽略重复键，其他错误抛出
            nondups = [er for er in e.details.get('writeErrors', []) if er.get('code') != 11000]
            if nondups:
                raise
            dups = len(e.details.get('writeErrors', []))
            logger.warning(f"{col.name}: ignored {dups} duplicate key errors")
            # 返回成功写入的数量
            return e.details.get('nInserted', 0) + e.details.get('nModified', 0)
    
    async def store_profiles(self, profiles: List[Dict[str, Any]]) -> int:
        """Store user profiles in MongoDB"""
        if not profiles:
            return 0
        
        try:
            collection = self.db[self.collections["users"]]
            
            # Add unique index on profile_id if not exists
            collection.create_index("profile_id", unique=True)
            
            # Use bulk upsert for power operations
            return self._bulk_upsert(collection, profiles, 'profile_id')
            
        except Exception as e:
            logger.error(f"Error storing profiles in MongoDB: {e}")
            return 0
    
    async def store_posts(self, posts: List[Dict[str, Any]]) -> int:
        """Store posts/publications in MongoDB"""
        if not posts:
            return 0
        
        try:
            collection = self.db[self.collections["posts"]]
            
            # Add unique index on publication_id if not exists
            collection.create_index("publication_id", unique=True)
            
            # Use bulk upsert for power operations
            return self._bulk_upsert(collection, posts, 'publication_id')
            
        except Exception as e:
            logger.error(f"Error storing posts in MongoDB: {e}")
            return 0
    
    async def store_follows(self, follows: List[Dict[str, Any]]) -> int:
        """Store follow relationships in MongoDB"""
        if not follows:
            return 0
        
        try:
            collection = self.db[self.collections["interactions"]]
            
            # Add unique index on follow_id if not exists
            collection.create_index("follow_id", unique=True)
            
            # Insert follows
            result = collection.insert_many(follows, ordered=False)
            logger.info(f"Stored {len(result.inserted_ids)} follow relationships in MongoDB")
            return len(result.inserted_ids)
            
        except DuplicateKeyError:
            # Handle duplicates
            updated_count = 0
            for follow in follows:
                try:
                    collection = self.db[self.collections["interactions"]]
                    collection.replace_one(
                        {"follow_id": follow["follow_id"]},
                        follow,
                        upsert=True
                    )
                    updated_count += 1
                except Exception as e:
                    logger.error(f"Error updating follow {follow['follow_id']}: {e}")
            
            logger.info(f"Updated {updated_count} follow relationships in MongoDB")
            return updated_count
            
        except Exception as e:
            logger.error(f"Error storing follows in MongoDB: {e}")
            return 0
    
    async def store_engagements(self, engagements: List[Dict[str, Any]]) -> int:
        """Store engagement data in MongoDB"""
        if not engagements:
            return 0
        
        try:
            collection = self.db[self.collections["interactions"]]
            
            # Add unique index on engagement_id if not exists
            collection.create_index("engagement_id", unique=True)
            
            # Use bulk upsert for power operations
            return self._bulk_upsert(collection, engagements, 'engagement_id')
            
        except Exception as e:
            logger.error(f"Error storing engagements in MongoDB: {e}")
            return 0
    
    async def get_profiles(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """Retrieve profiles from MongoDB"""
        try:
            collection = self.db[self.collections["users"]]
            cursor = collection.find().skip(offset).limit(limit)
            profiles = list(cursor)
            logger.info(f"Retrieved {len(profiles)} profiles from MongoDB")
            return profiles
        except Exception as e:
            logger.error(f"Error retrieving profiles from MongoDB: {e}")
            return []
    
    async def get_posts(self, profile_id: Optional[str] = None, 
                       limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """Retrieve posts from MongoDB"""
        try:
            collection = self.db[self.collections["posts"]]
            filter_query = {}
            if profile_id:
                filter_query["profile_id"] = profile_id
            
            cursor = collection.find(filter_query).skip(offset).limit(limit)
            posts = list(cursor)
            logger.info(f"Retrieved {len(posts)} posts from MongoDB")
            return posts
        except Exception as e:
            logger.error(f"Error retrieving posts from MongoDB: {e}")
            return []


class Neo4jStorage:
    """Neo4j graph database storage operations"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.driver = db_manager.neo4j_driver
    
    async def create_user_nodes(self, profiles: List[Dict[str, Any]]) -> int:
        """Create user nodes in Neo4j"""
        if not profiles:
            return 0
        
        try:
            with self.driver.session() as session:
                created_count = 0
                for profile in profiles:
                    # Create user node
                    query = """
                    MERGE (u:User {profile_id: $profile_id})
                    SET u.account_id = $account_id,
                        u.block_number = $block_number,
                        u.transaction_hash = $transaction_hash,
                        u.created_at = $created_at,
                        u.type = $type,
                        u.platform = $platform,
                        u.collected_at = $collected_at
                    """
                    
                    session.run(query, {
                        "profile_id": profile["profile_id"],
                        "account_id": profile["account_id"],
                        "block_number": profile["block_number"],
                        "transaction_hash": profile["transaction_hash"],
                        "created_at": profile["created_at"],
                        "type": profile["type"],
                        "platform": profile["platform"],
                        "collected_at": profile["collected_at"]
                    })
                    created_count += 1
                
                logger.info(f"Created {created_count} user nodes in Neo4j")
                return created_count
                
        except Exception as e:
            logger.error(f"Error creating user nodes in Neo4j: {e}")
            return 0
    
    async def create_post_nodes(self, posts: List[Dict[str, Any]]) -> int:
        """Create post nodes in Neo4j"""
        if not posts:
            return 0
        
        try:
            with self.driver.session() as session:
                created_count = 0
                for post in posts:
                    # Create post node
                    query = """
                    MERGE (p:Post {post_id: $post_id})
                    SET p.type = $type,
                        p.account_id = $account_id,
                        p.block_number = $block_number,
                        p.transaction_hash = $transaction_hash,
                        p.created_at = $created_at,
                        p.platform = $platform,
                        p.collected_at = $collected_at
                    """
                    
                    session.run(query, {
                        "post_id": post["post_id"],
                        "type": post["type"],
                        "account_id": post["account_id"],
                        "block_number": post["block_number"],
                        "transaction_hash": post["transaction_hash"],
                        "created_at": post["created_at"],
                        "platform": post["platform"],
                        "collected_at": post["collected_at"]
                    })
                    
                    # Create relationship with user
                    rel_query = """
                    MATCH (u:User {account_id: $account_id})
                    MATCH (p:Post {post_id: $post_id})
                    MERGE (u)-[:POSTED {block_number: $block_number, timestamp: $created_at}]->(p)
                    """
                    
                    session.run(rel_query, {
                        "account_id": post["account_id"],
                        "post_id": post["post_id"],
                        "block_number": post["block_number"],
                        "created_at": post["created_at"]
                    })
                    
                    created_count += 1
                
                logger.info(f"Created {created_count} post nodes in Neo4j")
                return created_count
                
        except Exception as e:
            logger.error(f"Error creating post nodes in Neo4j: {e}")
            return 0
    
    async def create_follow_relationships(self, follows: List[Dict[str, Any]]) -> int:
        """Create follow relationships in Neo4j"""
        if not follows:
            return 0
        
        try:
            with self.driver.session() as session:
                created_count = 0
                for follow in follows:
                    # Create follow relationship
                    query = """
                    MATCH (follower:User {profile_id: $follower_id})
                    MATCH (following:User {profile_id: $following_id})
                    MERGE (follower)-[:FOLLOWS {timestamp: $collected_at}]->(following)
                    """
                    
                    session.run(query, {
                        "follower_id": follow["follower_id"],
                        "following_id": follow["following_id"],
                        "collected_at": follow["collected_at"]
                    })
                    
                    created_count += 1
                
                logger.info(f"Created {created_count} follow relationships in Neo4j")
                return created_count
                
        except Exception as e:
            logger.error(f"Error creating follow relationships in Neo4j: {e}")
            return 0
    
    async def create_engagement_relationships(self, engagements: List[Dict[str, Any]]) -> int:
        """Create engagement relationships in Neo4j"""
        if not engagements:
            return 0
        
        try:
            with self.driver.session() as session:
                created_count = 0
                for engagement in engagements:
                    # Create interaction relationship
                    query = """
                    MATCH (u:User {account_id: $account_id})
                    MATCH (p:Post {post_id: $post_id})
                    MERGE (u)-[:INTERACTED {type: $type, timestamp: $timestamp}]->(p)
                    """
                    
                    session.run(query, {
                        "account_id": engagement["account_id"],
                        "post_id": engagement["post_id"],
                        "type": engagement["type"],
                        "timestamp": engagement["created_at"]
                    })
                    
                    created_count += 1
                
                logger.info(f"Created {created_count} engagement relationships in Neo4j")
                return created_count
                
        except Exception as e:
            logger.error(f"Error creating engagement relationships in Neo4j: {e}")
            return 0
    
    async def create_interaction_relationships(self, interactions: List[Dict[str, Any]]) -> int:
        """Create interaction relationships in Neo4j (alias for create_engagement_relationships)"""
        return await self.create_engagement_relationships(interactions)
    
    async def create_follow_relationships_mock(self, accounts: List[Dict[str, Any]]) -> int:
        """Create mock follow relationships between users for testing"""
        if len(accounts) < 2:
            return 0
        
        try:
            with self.driver.session() as session:
                created_count = 0
                
                # Create follow relationships between consecutive users
                for i in range(len(accounts) - 1):
                    follower = accounts[i]
                    following = accounts[i + 1]
                    
                    query = """
                    MATCH (follower:User {profile_id: $follower_id})
                    MATCH (following:User {profile_id: $following_id})
                    MERGE (follower)-[:FOLLOWS {created_at: $timestamp, platform: $platform}]->(following)
                    """
                    
                    session.run(query, {
                        "follower_id": follower["profile_id"],
                        "following_id": following["profile_id"],
                        "timestamp": datetime.utcnow().isoformat(),
                        "platform": follower.get("platform", "unknown")
                    })
                    
                    created_count += 1
                
                # Create some additional random follows
                import random
                for _ in range(min(3, len(accounts))):
                    follower = random.choice(accounts)
                    following = random.choice(accounts)
                    
                    if follower["profile_id"] != following["profile_id"]:
                        query = """
                        MATCH (follower:User {profile_id: $follower_id})
                        MATCH (following:User {profile_id: $following_id})
                        MERGE (follower)-[:FOLLOWS {created_at: $timestamp, platform: $platform}]->(following)
                        """
                        
                        session.run(query, {
                            "follower_id": follower["profile_id"],
                            "following_id": following["profile_id"],
                            "timestamp": datetime.utcnow().isoformat(),
                            "platform": follower.get("platform", "unknown")
                        })
                        
                        created_count += 1
                
                logger.info(f"Created {created_count} mock follow relationships in Neo4j")
                return created_count
                
        except Exception as e:
            logger.error(f"Error creating mock follow relationships in Neo4j: {e}")
            return 0
    
    async def get_user_network(self, profile_id: str, depth: int = 2) -> Dict[str, Any]:
        """Get user's network up to specified depth"""
        try:
            with self.driver.session() as session:
                query = """
                MATCH path = (u:User {profile_id: $profile_id})-[*1..$depth]-(connected)
                WHERE connected:User
                RETURN path
                """
                
                result = session.run(query, {
                    "profile_id": profile_id,
                    "depth": depth
                })
                
                # Process the result to extract network structure
                network = {
                    "profile_id": profile_id,
                    "connections": [],
                    "depth": depth
                }
                
                for record in result:
                    path = record["path"]
                    # Extract connection information from path
                    # This is a simplified version - you might want to process this more thoroughly
                    network["connections"].append(str(path))
                
                logger.info(f"Retrieved network for user {profile_id} with depth {depth}")
                return network
                
        except Exception as e:
            logger.error(f"Error retrieving user network from Neo4j: {e}")
            return {}


class RedisStorage:
    """Redis cache storage operations"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.client = db_manager.redis_client
    
    async def cache_profiles(self, profiles: List[Dict[str, Any]], ttl: int = 3600) -> int:
        """Cache profiles in Redis"""
        if not profiles:
            return 0
        
        try:
            cached_count = 0
            for profile in profiles:
                key = f"profile:{profile['profile_id']}"
                # Use bson json_util to handle ObjectId serialization
                value = json_util.dumps(profile)
                self.client.setex(key, ttl, value)
                cached_count += 1
            
            logger.info(f"Cached {cached_count} profiles in Redis")
            return cached_count
            
        except Exception as e:
            logger.error(f"Error caching profiles in Redis: {e}")
            return 0
    
    async def cache_posts(self, posts: List[Dict[str, Any]], ttl: int = 3600) -> int:
        """Cache posts in Redis"""
        if not posts:
            return 0
        
        try:
            cached_count = 0
            for post in posts:
                key = f"post:{post['publication_id']}"
                # Use bson json_util to handle ObjectId serialization
                value = json_util.dumps(post)
                self.client.setex(key, ttl, value)
                cached_count += 1
            
            logger.info(f"Cached {cached_count} posts in Redis")
            return cached_count
            
        except Exception as e:
            logger.error(f"Error caching posts in Redis: {e}")
            return 0
    
    async def get_cached_profile(self, profile_id: str) -> Optional[Dict[str, Any]]:
        """Get cached profile from Redis"""
        try:
            key = f"profile:{profile_id}"
            value = self.client.get(key)
            if value:
                return json_util.loads(value)
            return None
        except Exception as e:
            logger.error(f"Error retrieving cached profile from Redis: {e}")
            return None
    
    async def get_cached_post(self, publication_id: str) -> Optional[Dict[str, Any]]:
        """Get cached post from Redis"""
        try:
            key = f"post:{publication_id}"
            value = self.client.get(key)
            if value:
                return json_util.loads(value)
            return None
        except Exception as e:
            logger.error(f"Error retrieving cached post from Redis: {e}")
            return None
    
    async def clear_cache(self, pattern: str = "*") -> int:
        """Clear cache entries matching pattern"""
        try:
            keys = self.client.keys(pattern)
            if keys:
                deleted = self.client.delete(*keys)
                logger.info(f"Cleared {deleted} cache entries from Redis")
                return deleted
            return 0
        except Exception as e:
            logger.error(f"Error clearing cache from Redis: {e}")
            return 0


# Example usage
if __name__ == "__main__":
    async def main():
        # Initialize database manager
        db_manager = DatabaseManager()
        
        try:
            # Initialize storage classes
            mongo_storage = MongoDBStorage(db_manager)
            neo4j_storage = Neo4jStorage(db_manager)
            redis_storage = RedisStorage(db_manager)
            
            # Example data
            sample_profile = {
                "profile_id": "test_profile_1",
                "handle": "testuser",
                "name": "Test User",
                "bio": "Test bio",
                "total_followers": 100,
                "total_following": 50,
                "total_posts": 25,
                "owned_by": "0x1234567890abcdef",
                "collected_at": datetime.utcnow().isoformat()
            }
            
            # Store in MongoDB
            await mongo_storage.store_profiles([sample_profile])
            
            # Store in Neo4j
            await neo4j_storage.create_user_nodes([sample_profile])
            
            # Cache in Redis
            await redis_storage.cache_profiles([sample_profile])
            
            # Retrieve from MongoDB
            profiles = await mongo_storage.get_profiles(limit=1)
            print(f"Retrieved profile: {profiles[0] if profiles else 'None'}")
            
        finally:
            db_manager.close()
    
    asyncio.run(main())
