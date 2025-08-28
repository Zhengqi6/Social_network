"""
Database storage module for decentralized social recommendation data
"""
import json
import asyncio
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
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
    
    async def store_profiles(self, profiles: List[Dict[str, Any]]) -> int:
        """Store user profiles in MongoDB"""
        if not profiles:
            return 0
        
        try:
            collection = self.db[self.collections["users"]]
            
            # Add unique index on profile_id if not exists
            collection.create_index("profile_id", unique=True)
            
            # Insert profiles
            result = collection.insert_many(profiles, ordered=False)
            logger.info(f"Stored {len(result.inserted_ids)} profiles in MongoDB")
            return len(result.inserted_ids)
            
        except DuplicateKeyError:
            # Handle duplicates by updating existing records
            updated_count = 0
            for profile in profiles:
                try:
                    collection = self.db[self.collections["users"]]
                    collection.replace_one(
                        {"profile_id": profile["profile_id"]},
                        profile,
                        upsert=True
                    )
                    updated_count += 1
                except Exception as e:
                    logger.error(f"Error updating profile {profile['profile_id']}: {e}")
            
            logger.info(f"Updated {updated_count} profiles in MongoDB")
            return updated_count
            
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
            
            # Insert posts
            result = collection.insert_many(posts, ordered=False)
            logger.info(f"Stored {len(result.inserted_ids)} posts in MongoDB")
            return len(result.inserted_ids)
            
        except DuplicateKeyError:
            # Handle duplicates
            updated_count = 0
            for post in posts:
                try:
                    collection = self.db[self.collections["posts"]]
                    collection.replace_one(
                        {"publication_id": post["publication_id"]},
                        post,
                        upsert=True
                    )
                    updated_count += 1
                except Exception as e:
                    logger.error(f"Error updating post {post['publication_id']}: {e}")
            
            logger.info(f"Updated {updated_count} posts in MongoDB")
            return updated_count
            
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
            
            # Insert engagements
            result = collection.insert_many(engagements, ordered=False)
            logger.info(f"Stored {len(result.inserted_ids)} engagements in MongoDB")
            return len(result.inserted_ids)
            
        except DuplicateKeyError:
            # Handle duplicates
            updated_count = 0
            for engagement in engagements:
                try:
                    collection = self.db[self.collections["interactions"]]
                    collection.replace_one(
                        {"engagement_id": engagement["engagement_id"]},
                        engagement,
                        upsert=True
                    )
                    updated_count += 1
                except Exception as e:
                    logger.error(f"Error updating engagement {engagement['engagement_id']}: {e}")
            
            logger.info(f"Updated {updated_count} engagements in MongoDB")
            return updated_count
            
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
                    SET u.handle = $handle,
                        u.name = $name,
                        u.bio = $bio,
                        u.total_followers = $total_followers,
                        u.total_following = $total_following,
                        u.total_posts = $total_posts,
                        u.owned_by = $owned_by,
                        u.collected_at = $collected_at
                    """
                    
                    session.run(query, {
                        "profile_id": profile["profile_id"],
                        "handle": profile["handle"],
                        "name": profile["name"],
                        "bio": profile.get("bio", ""),
                        "total_followers": profile.get("total_followers", 0),
                        "total_following": profile.get("total_following", 0),
                        "total_posts": profile.get("total_posts", 0),
                        "owned_by": profile["owned_by"],
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
                    MERGE (p:Post {publication_id: $publication_id})
                    SET p.type = $type,
                        p.content = $content,
                        p.created_at_timestamp = $created_at_timestamp,
                        p.total_mirrors = $total_mirrors,
                        p.total_comments = $total_comments,
                        p.total_collects = $total_collects,
                        p.collected_at = $collected_at
                    """
                    
                    session.run(query, {
                        "publication_id": post["publication_id"],
                        "type": post["type"],
                        "content": post.get("content", ""),
                        "created_at_timestamp": post["created_at_timestamp"],
                        "total_mirrors": post.get("total_mirrors", 0),
                        "total_comments": post.get("total_comments", 0),
                        "total_collects": post.get("total_collects", 0),
                        "collected_at": post["collected_at"]
                    })
                    
                    # Create relationship with user
                    rel_query = """
                    MATCH (u:User {profile_id: $profile_id})
                    MATCH (p:Post {publication_id: $publication_id})
                    MERGE (u)-[:POSTED]->(p)
                    """
                    
                    session.run(rel_query, {
                        "profile_id": post["profile_id"],
                        "publication_id": post["publication_id"]
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
                    if engagement["type"] == "mirror":
                        # Create mirror relationship
                        query = """
                        MATCH (u:User {profile_id: $profile_id})
                        MATCH (p:Post {publication_id: $publication_id})
                        MERGE (u)-[:MIRRORED {timestamp: $timestamp}]->(p)
                        """
                        
                        session.run(query, {
                            "profile_id": engagement["profile_id"],
                            "publication_id": engagement["publication_id"],
                            "timestamp": engagement["timestamp"]
                        })
                        
                    elif engagement["type"] == "comment":
                        # Create comment relationship
                        query = """
                        MATCH (u:User {profile_id: $profile_id})
                        MATCH (p:Post {publication_id: $publication_id})
                        MERGE (u)-[:COMMENTED {timestamp: $timestamp, content: $content}]->(p)
                        """
                        
                        session.run(query, {
                            "profile_id": engagement["profile_id"],
                            "publication_id": engagement["publication_id"],
                            "timestamp": engagement["timestamp"],
                            "content": engagement.get("content", "")
                        })
                    
                    created_count += 1
                
                logger.info(f"Created {created_count} engagement relationships in Neo4j")
                return created_count
                
        except Exception as e:
            logger.error(f"Error creating engagement relationships in Neo4j: {e}")
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
                value = json.dumps(profile)
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
                value = json.dumps(post)
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
                return json.loads(value)
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
                return json.loads(value)
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
