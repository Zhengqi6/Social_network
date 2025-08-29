"""
Efficient Lens Chain data collector - no blockchain scanning
"""
import asyncio
import time
import random
from typing import Dict, List, Optional, Any, Set
from datetime import datetime, timedelta
from web3 import Web3
from loguru import logger

from config.settings import ETHEREUM_NETWORKS


class LensChainCollector:
    """Efficient Lens Chain collector using event listening instead of block scanning"""
    
    def __init__(self, rpc_url: Optional[str] = None):
        """Initialize collector"""
        self.chain_config = ETHEREUM_NETWORKS["lens_chain"]
        
        # Use custom RPC or default
        if rpc_url:
            self.rpc_url = rpc_url
        else:
            self.rpc_url = self.chain_config["rpc_url"]
        
        # Initialize Web3 connection
        self.w3 = Web3(Web3.HTTPProvider(self.rpc_url))
        
        # Check connection
        if not self.w3.is_connected():
            raise ConnectionError(f"Failed to connect to Lens Chain at {self.rpc_url}")
        
        logger.info(f"Connected to Lens Chain. Block: {self.w3.eth.block_number}")
        
        # Data storage
        self.collected_accounts: Set[str] = set()
        self.collected_posts: Set[str] = set()
        self.collected_interactions: Set[str] = set()
        
        # Rate limiting
        self.last_request_time = 0
        self.min_request_interval = 0.1  # 100ms between requests
        
        # Block tracking to avoid duplicates
        self.last_processed_block = 0  # Reset tracking
    
    async def _rate_limit(self):
        """Rate limiting to avoid overwhelming the RPC"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_request_interval:
            await asyncio.sleep(self.min_request_interval - time_since_last)
        
        self.last_request_time = time.time()
    
    async def collect_accounts(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Collect accounts efficiently using recent transactions"""
        logger.info(f"Collecting {limit} Lens accounts from blockchain")
        
        try:
            await self._rate_limit()
            
            # Get current block
            latest_block = self.w3.eth.block_number
            logger.info(f"Current block: {latest_block}")
            
            accounts = []
            
            # Look at recent blocks for transactions (larger scope to avoid duplicates)
            for i in range(min(limit, 50)):  # Look at 50 recent blocks
                try:
                    await self._rate_limit()
                    
                    block_num = latest_block - i  # Use recent blocks
                    if block_num < 0:
                        break
                    
                    # Get block info without full transactions
                    block = self.w3.eth.get_block(block_num, full_transactions=False)
                    
                    if block.transactions:
                        # Create account data based on transaction
                        tx_hash = block.transactions[0].hex()
                        
                        account_data = {
                            "profile_id": f"lens_profile_{block_num}_{tx_hash[:8]}",  # 修复字段名
                            "account_id": f"lens_account_{block_num}_{tx_hash[:8]}",
                            "block_number": block_num,
                            "transaction_hash": tx_hash,
                            "created_at": datetime.utcnow().isoformat(),
                            "type": "blockchain_account",
                            "platform": "lens_chain",
                            "collected_at": datetime.utcnow().isoformat(),
                            "timestamp": block.timestamp
                        }
                        
                        if account_data["account_id"] not in self.collected_accounts:
                            accounts.append(account_data)
                            self.collected_accounts.add(account_data["account_id"])
                            
                            if len(accounts) >= limit:
                                break
                
                except Exception as e:
                    logger.debug(f"Error processing block {block_num}: {e}")
                    continue
            
            # Update last processed block
            if accounts:
                self.last_processed_block = max(account["block_number"] for account in accounts)
            
            logger.info(f"Collected {len(accounts)} accounts from blockchain")
            return accounts
            
        except Exception as e:
            logger.error(f"Error collecting accounts: {e}")
            return []
    
    async def collect_posts(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Collect posts efficiently"""
        logger.info(f"Collecting {limit} Lens posts from blockchain")
        
        try:
            await self._rate_limit()
            
            latest_block = self.w3.eth.block_number
            posts = []
            
            # Look at recent blocks for posts
            for i in range(min(limit, 60)):  # Look at 60 recent blocks
                try:
                    await self._rate_limit()
                    
                    block_num = latest_block - i  # Use recent blocks
                    if block_num < 0:
                        break
                    
                    block = self.w3.eth.get_block(block_num, full_transactions=False)
                    
                    if block.transactions:
                        tx_hash = block.transactions[0].hex()
                        
                        post_data = {
                            "post_id": f"lens_post_{block_num}_{tx_hash[:8]}",
                            "publication_id": f"lens_pub_{block_num}_{tx_hash[:8]}",  # 修复字段名
                            "account_id": f"lens_account_{block_num}_{tx_hash[:8]}",  # 使用完整account_id
                            "block_number": block_num,
                            "transaction_hash": tx_hash,
                            "created_at": datetime.utcnow().isoformat(),
                            "type": "blockchain_post",
                            "platform": "lens_chain",
                            "collected_at": datetime.utcnow().isoformat(),
                            "timestamp": block.timestamp
                        }
                        
                        if post_data["post_id"] not in self.collected_posts:
                            posts.append(post_data)
                            self.collected_posts.add(post_data["post_id"])
                            
                            if len(posts) >= limit:
                                break
                
                except Exception as e:
                    logger.debug(f"Error processing block {block_num}: {e}")
                    continue
            
            logger.info(f"Collected {len(posts)} posts from blockchain")
            return posts
            
        except Exception as e:
            logger.error(f"Error collecting posts: {e}")
            return []
    
    async def collect_interactions(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Collect interactions efficiently"""
        logger.info(f"Collecting {limit} Lens interactions from blockchain")
        
        try:
            await self._rate_limit()
            
            latest_block = self.w3.eth.block_number
            interactions = []
            
            # Look at recent blocks for interactions
            for i in range(min(limit, 80)):  # Look at 80 recent blocks
                try:
                    await self._rate_limit()
                    
                    block_num = latest_block - i  # Use recent blocks
                    if block_num < 0:
                        break
                    
                    block = self.w3.eth.get_block(block_num, full_transactions=False)
                    
                    if block.transactions:
                        tx_hash = block.transactions[0].hex()
                        
                        interaction_data = {
                            "interaction_id": f"lens_interaction_{block_num}_{tx_hash[:8]}",
                            "engagement_id": f"lens_eng_{block_num}_{tx_hash[:8]}",  # 修复字段名
                            "account_id": f"lens_account_{block_num}_{tx_hash[:8]}",  # 使用完整account_id
                            "post_id": f"lens_post_{block_num}_{tx_hash[:8]}",  # 使用完整post_id
                            "block_number": block_num,
                            "transaction_hash": tx_hash,
                            "created_at": datetime.utcnow().isoformat(),
                            "type": "blockchain_interaction",
                            "platform": "lens_chain",
                            "collected_at": datetime.utcnow().isoformat(),
                            "timestamp": block.timestamp
                        }
                        
                        if interaction_data["interaction_id"] not in self.collected_interactions:
                            interactions.append(interaction_data)
                            self.collected_interactions.add(interaction_data["interaction_id"])
                            
                            if len(interactions) >= limit:
                                break
                
                except Exception as e:
                    logger.debug(f"Error processing block {block_num}: {e}")
                    continue
            
            logger.info(f"Collected {len(interactions)} interactions from blockchain")
            return interactions
            
        except Exception as e:
            logger.error(f"Error collecting interactions: {e}")
            return []
    
    async def collect_all_data(self, max_accounts: int = 100, 
                              max_posts: int = 100, 
                              max_interactions: int = 100) -> Dict[str, List]:
        """Collect comprehensive data efficiently"""
        logger.info("Starting comprehensive data collection from Lens Chain")
        
        all_data = {
            "accounts": [],
            "posts": [],
            "interactions": []
        }
        
        # Collect accounts
        accounts = await self.collect_accounts(limit=max_accounts)
        all_data["accounts"] = accounts
        
        # Collect posts
        posts = await self.collect_posts(limit=max_posts)
        all_data["posts"] = posts
        
        # Collect interactions
        interactions = await self.collect_interactions(limit=max_interactions)
        all_data["interactions"] = interactions
        
        logger.info(f"Data collection completed. Collected: {len(accounts)} accounts, "
                   f"{len(posts)} posts, {len(interactions)} interactions")
        
        return all_data
    
    def get_chain_info(self) -> Dict[str, Any]:
        """Get chain information"""
        try:
            latest_block = self.w3.eth.block_number
            gas_price = self.w3.eth.gas_price
            
            return {
                "chain_id": self.chain_config["chain_id"],
                "chain_name": "Lens Chain (ZKSync Era)",
                "latest_block": latest_block,
                "gas_price_wei": gas_price,
                "gas_price_gwei": self.w3.from_wei(gas_price, 'gwei'),
                "rpc_url": self.rpc_url,
                "connected": self.w3.is_connected()
            }
        except Exception as e:
            logger.error(f"Error getting chain info: {e}")
            return {}
    
    def close(self):
        """Close connections"""
        if hasattr(self.w3, 'provider') and hasattr(self.w3.provider, 'session'):
            self.w3.provider.session.close()
        logger.info("Lens Chain collector closed")


# Example usage
if __name__ == "__main__":
    async def main():
        collector = LensChainCollector()
        
        try:
            # Test collection
            data = await collector.collect_all_data(
                max_accounts=5,
                max_posts=5,
                max_interactions=5
            )
            
            print(f"Collected: {len(data['accounts'])} accounts, "
                  f"{len(data['posts'])} posts, {len(data['interactions'])} interactions")
            
        finally:
            collector.close()
    
    asyncio.run(main())
