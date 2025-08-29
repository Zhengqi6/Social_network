"""
Data Loader for Social Network Recommendation System
Loads data from MongoDB and Neo4j for processing
"""

import asyncio
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
from loguru import logger

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_collection.storage.database import DatabaseManager


class DataLoader:
    """Loads data from MongoDB and Neo4j databases"""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.mongo_storage = None
        self.neo4j_storage = None
        
    async def initialize(self):
        """Initialize database connections"""
        try:
            self.db_manager._initialize_connections()
            from data_collection.storage.database import MongoDBStorage, Neo4jStorage
            self.mongo_storage = MongoDBStorage(self.db_manager)
            self.neo4j_storage = Neo4jStorage(self.db_manager)
            logger.info("Data loader initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize data loader: {e}")
            raise
    
    async def load_users_data(self) -> pd.DataFrame:
        """Load users data from MongoDB"""
        try:
            users = await self.mongo_storage.get_profiles(limit=1000)
            if not users:
                logger.warning("No users found in MongoDB")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(users)
            logger.info(f"Loaded {len(df)} users from MongoDB")
            return df
            
        except Exception as e:
            logger.error(f"Error loading users data: {e}")
            return pd.DataFrame()
    
    async def load_posts_data(self) -> pd.DataFrame:
        """Load posts data from MongoDB"""
        try:
            posts = await self.mongo_storage.get_posts(limit=1000)
            if not posts:
                logger.warning("No posts found in MongoDB")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(posts)
            logger.info(f"Loaded {len(df)} posts from MongoDB")
            return df
            
        except Exception as e:
            logger.error(f"Error loading posts data: {e}")
            return pd.DataFrame()
    
    async def load_interactions_data(self) -> pd.DataFrame:
        """Load interactions data from MongoDB"""
        try:
            # Get interactions from interactions collection
            collection = self.db_manager.mongodb_db['interactions']
            interactions = list(collection.find().limit(1000))
            
            if not interactions:
                logger.warning("No interactions found in MongoDB")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(interactions)
            logger.info(f"Loaded {len(df)} interactions from MongoDB")
            return df
            
        except Exception as e:
            logger.error(f"Error loading interactions data: {e}")
            return pd.DataFrame()
    
    async def load_graph_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Load graph data from Neo4j"""
        try:
            with self.db_manager.neo4j_driver.session() as session:
                # Load users
                result = session.run("MATCH (u:User) RETURN u")
                users = [dict(record["u"]) for record in result]
                users_df = pd.DataFrame(users)
                
                # Load posts
                result = session.run("MATCH (p:Post) RETURN p")
                posts = [dict(record["p"]) for record in result]
                posts_df = pd.DataFrame(posts)
                
                # Load relationships
                result = session.run("""
                    MATCH (a)-[r]->(b)
                    RETURN type(r) as relationship_type, 
                           properties(a) as source_props, 
                           properties(b) as target_props,
                           properties(r) as relationship_props
                """)
                relationships = []
                for record in result:
                    rel_data = {
                        "relationship_type": record["relationship_type"],
                        "source_props": record["source_props"],
                        "target_props": record["target_props"],
                        "relationship_props": record["relationship_props"]
                    }
                    relationships.append(rel_data)
                relationships_df = pd.DataFrame(relationships)
                
                logger.info(f"Loaded graph data: {len(users_df)} users, {len(posts_df)} posts, {len(relationships_df)} relationships")
                return users_df, posts_df, relationships_df
                
        except Exception as e:
            logger.error(f"Error loading graph data: {e}")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    async def get_data_summary(self) -> Dict[str, Any]:
        """Get summary of available data"""
        try:
            summary = {}
            
            # MongoDB counts
            users_df = await self.load_users_data()
            posts_df = await self.load_posts_data()
            interactions_df = await self.load_interactions_data()
            
            summary["mongodb"] = {
                "users": len(users_df),
                "posts": len(posts_df),
                "interactions": len(interactions_df)
            }
            
            # Neo4j counts
            users_graph, posts_graph, relationships = await self.load_graph_data()
            
            summary["neo4j"] = {
                "users": len(users_graph),
                "posts": len(posts_graph),
                "relationships": len(relationships)
            }
            
            # Data quality info
            summary["data_quality"] = {
                "users_with_posts": len(users_df[users_df.get("total_posts", 0) > 0]) if not users_df.empty else 0,
                "posts_with_interactions": len(posts_df[posts_df.get("total_interactions", 0) > 0]) if not posts_df.empty else 0,
                "avg_posts_per_user": len(posts_df) / len(users_df) if len(users_df) > 0 else 0,
                "avg_interactions_per_post": len(interactions_df) / len(posts_df) if len(posts_df) > 0 else 0
            }
            
            logger.info("Data summary generated successfully")
            return summary
            
        except Exception as e:
            logger.error(f"Error generating data summary: {e}")
            return {}
    
    def close(self):
        """Close database connections"""
        if self.db_manager:
            self.db_manager.close()


async def main():
    """Test data loader"""
    loader = DataLoader()
    try:
        await loader.initialize()
        
        # Load data
        users = await loader.load_users_data()
        posts = await loader.load_posts_data()
        interactions = await loader.load_interactions_data()
        
        print(f"Loaded {len(users)} users, {len(posts)} posts, {len(interactions)} interactions")
        
        # Get summary
        summary = await loader.get_data_summary()
        print("Data Summary:", summary)
        
    finally:
        loader.close()


if __name__ == "__main__":
    asyncio.run(main())
