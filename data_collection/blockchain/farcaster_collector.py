"""
Farcaster data collector using GraphQL API
"""
import asyncio
import time
from typing import Dict, List, Optional, Any, Set
from datetime import datetime, timedelta
import aiohttp
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from loguru import logger

from config.settings import PLATFORM_APIS


class FarcasterCollector:
    """Collector for Farcaster data using GraphQL API"""
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize Farcaster collector
        
        Args:
            api_key: Optional API key for Farcaster
        """
        self.config = PLATFORM_APIS["farcaster"]
        self.api_key = api_key
        self.transport = None
        self.client = None
        self._initialize_client()
        
        # Data storage
        self.collected_users: Set[str] = set()
        self.collected_casts: Set[str] = set()
        self.collected_reactions: Set[str] = set()
    
    def _initialize_client(self):
        """Initialize GraphQL client"""
        try:
            headers = {}
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"
            
            self.transport = AIOHTTPTransport(
                url=self.config["graphql_endpoint"],
                headers=headers
            )
            self.client = Client(transport=self.transport, fetch_schema_from_transport=True)
            logger.info("Farcaster GraphQL client initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing Farcaster client: {e}")
            raise
    
    async def collect_users(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Collect Farcaster users
        
        Args:
            limit: Number of users to collect
            offset: Offset for pagination
        
        Returns:
            List of user data
        """
        logger.info(f"Collecting {limit} Farcaster users")
        
        try:
            # GraphQL query for users
            query = gql("""
                query GetUsers($limit: Int!, $offset: Int!) {
                    users(first: $limit, offset: $offset) {
                        items {
                            fid
                            username
                            displayName
                            pfp
                            followerCount
                            followingCount
                            verifications {
                                address
                            }
                            createdAt
                        }
                        pageInfo {
                            hasNextPage
                            endCursor
                        }
                    }
                }
            """)
            
            variables = {
                "limit": limit,
                "offset": offset
            }
            
            result = await self.client.execute_async(query, variable_values=variables)
            
            users = []
            if result and "users" in result and "items" in result["users"]:
                for user in result["users"]["items"]:
                    if user["fid"] not in self.collected_users:
                        user_data = {
                            "user_id": user["fid"],
                            "username": user.get("username", ""),
                            "display_name": user.get("displayName", ""),
                            "profile_picture": user.get("pfp", ""),
                            "follower_count": user.get("followerCount", 0),
                            "following_count": user.get("followingCount", 0),
                            "verifications": [v["address"] for v in user.get("verifications", [])],
                            "created_at": user.get("createdAt", ""),
                            "platform": "farcaster",
                            "collected_at": datetime.utcnow().isoformat()
                        }
                        users.append(user_data)
                        self.collected_users.add(user["fid"])
            
            logger.info(f"Collected {len(users)} Farcaster users")
            return users
            
        except Exception as e:
            logger.error(f"Error collecting Farcaster users: {e}")
            return []
    
    async def collect_casts(self, limit: int = 100, offset: int = 0, 
                           user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Collect Farcaster casts (posts)
        
        Args:
            limit: Number of casts to collect
            offset: Offset for pagination
            user_id: Optional user ID to filter casts
        
        Returns:
            List of cast data
        """
        logger.info(f"Collecting {limit} Farcaster casts")
        
        try:
            # GraphQL query for casts
            query = gql("""
                query GetCasts($limit: Int!, $offset: Int!, $userId: String) {
                    casts(first: $limit, offset: $offset, userId: $userId) {
                        items {
                            hash
                            author {
                                fid
                                username
                            }
                            text
                            timestamp
                            reactions {
                                count
                                type
                            }
                            replies {
                                count
                            }
                            recasts {
                                count
                            }
                        }
                        pageInfo {
                            hasNextPage
                            endCursor
                        }
                    }
                }
            """)
            
            variables = {
                "limit": limit,
                "offset": offset
            }
            
            if user_id:
                variables["userId"] = user_id
            
            result = await self.client.execute_async(query, variable_values=variables)
            
            casts = []
            if result and "casts" in result and "items" in result["casts"]:
                for cast in result["casts"]["items"]:
                    if cast["hash"] not in self.collected_casts:
                        cast_data = {
                            "cast_id": cast["hash"],
                            "author_id": cast["author"]["fid"],
                            "author_username": cast["author"]["username"],
                            "content": cast.get("text", ""),
                            "timestamp": cast.get("timestamp", ""),
                            "reactions_count": cast.get("reactions", {}).get("count", 0),
                            "replies_count": cast.get("replies", {}).get("count", 0),
                            "recasts_count": cast.get("recasts", {}).get("count", 0),
                            "platform": "farcaster",
                            "collected_at": datetime.utcnow().isoformat()
                        }
                        casts.append(cast_data)
                        self.collected_casts.add(cast["hash"])
            
            logger.info(f"Collected {len(casts)} Farcaster casts")
            return casts
            
        except Exception as e:
            logger.error(f"Error collecting Farcaster casts: {e}")
            return []
    
    async def collect_reactions(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Collect Farcaster reactions
        
        Args:
            limit: Number of reactions to collect
            offset: Offset for pagination
        
        Returns:
            List of reaction data
        """
        logger.info(f"Collecting {limit} Farcaster reactions")
        
        try:
            # GraphQL query for reactions
            query = gql("""
                query GetReactions($limit: Int!, $offset: Int!) {
                    reactions(first: $limit, offset: $offset) {
                        items {
                            hash
                            type
                            user {
                                fid
                                username
                            }
                            cast {
                                hash
                                author {
                                    fid
                                }
                            }
                            timestamp
                        }
                        pageInfo {
                            hasNextPage
                            endCursor
                        }
                    }
                }
            """)
            
            variables = {
                "limit": limit,
                "offset": offset
            }
            
            result = await self.client.execute_async(query, variable_values=variables)
            
            reactions = []
            if result and "reactions" in result and "items" in result["reactions"]:
                for reaction in result["reactions"]["items"]:
                    reaction_id = f"{reaction['hash']}_{reaction['user']['fid']}"
                    if reaction_id not in self.collected_reactions:
                        reaction_data = {
                            "reaction_id": reaction_id,
                            "type": reaction.get("type", ""),
                            "user_id": reaction["user"]["fid"],
                            "user_username": reaction["user"]["username"],
                            "cast_id": reaction["cast"]["hash"],
                            "cast_author_id": reaction["cast"]["author"]["fid"],
                            "timestamp": reaction.get("timestamp", ""),
                            "platform": "farcaster",
                            "collected_at": datetime.utcnow().isoformat()
                        }
                        reactions.append(reaction_data)
                        self.collected_reactions.add(reaction_id)
            
            logger.info(f"Collected {len(reactions)} Farcaster reactions")
            return reactions
            
        except Exception as e:
            logger.error(f"Error collecting Farcaster reactions: {e}")
            return []
    
    async def collect_all_data(self, max_users: int = 100, 
                              max_casts: int = 100, 
                              max_reactions: int = 100) -> Dict[str, List]:
        """
        Collect comprehensive Farcaster data
        
        Args:
            max_users: Maximum users to collect
            max_casts: Maximum casts to collect
            max_reactions: Maximum reactions to collect
        
        Returns:
            Dictionary containing all collected data
        """
        logger.info("Starting comprehensive Farcaster data collection")
        
        all_data = {
            "users": [],
            "casts": [],
            "reactions": []
        }
        
        # Collect users
        users = await self.collect_users(limit=max_users)
        all_data["users"] = users
        
        # Collect casts
        casts = await self.collect_casts(limit=max_casts)
        all_data["casts"] = casts
        
        # Collect reactions
        reactions = await self.collect_reactions(limit=max_reactions)
        all_data["reactions"] = reactions
        
        logger.info(f"Farcaster data collection completed. Collected: {len(users)} users, "
                   f"{len(casts)} casts, {len(reactions)} reactions")
        
        return all_data
    
    def close(self):
        """Close the GraphQL client"""
        try:
            if self.client and hasattr(self.client, 'transport'):
                if hasattr(self.client.transport, 'session'):
                    self.client.transport.session.close()
        except Exception as e:
            logger.warning(f"Error closing Farcaster client: {e}")
        logger.info("Farcaster collector closed")


# Example usage
if __name__ == "__main__":
    async def main():
        collector = FarcasterCollector()
        
        try:
            # Collect sample data
            data = await collector.collect_all_data(
                max_users=10, 
                max_casts=10, 
                max_reactions=10
            )
            
            print(f"Collected {len(data['users'])} users")
            print(f"Collected {len(data['casts'])} casts")
            print(f"Collected {len(data['reactions'])} reactions")
            
        finally:
            collector.close()
    
    asyncio.run(main())
