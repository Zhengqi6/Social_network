"""
Ethereum blockchain client for interacting with the Ethereum network
"""
import asyncio
import time
from typing import Dict, List, Optional, Any
from web3 import Web3
try:
    from web3.middleware import geth_poa_middleware
except ImportError:
    try:
        from web3.middleware import poa_middleware as geth_poa_middleware
    except ImportError:
        # For newer versions of web3
        from web3.middleware import ExtraDataToPOAMiddleware as geth_poa_middleware
from eth_account import Account
from loguru import logger

from config.settings import ETHEREUM_NETWORKS


class EthereumClient:
    """Client for interacting with Ethereum blockchain"""
    
    def __init__(self, network: str = "mainnet", private_key: Optional[str] = None):
        """
        Initialize Ethereum client
        
        Args:
            network: Network name (mainnet, polygon, etc.)
            private_key: Optional private key for signing transactions
        """
        self.network = network
        self.network_config = ETHEREUM_NETWORKS.get(network)
        
        if not self.network_config:
            raise ValueError(f"Unsupported network: {network}")
        
        # Initialize Web3 connection
        self.w3 = Web3(Web3.HTTPProvider(self.network_config["rpc_url"]))
        
        # Add POA middleware for networks like Polygon
        if network != "mainnet":
            self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        
        # Set up account if private key provided
        self.account = None
        if private_key:
            self.account = Account.from_key(private_key)
            logger.info(f"Account initialized: {self.account.address}")
        
        # Check connection
        if not self.w3.is_connected():
            raise ConnectionError(f"Failed to connect to {network} network")
        
        logger.info(f"Connected to {network} network. Block: {self.w3.eth.block_number}")
    
    def get_latest_block(self) -> int:
        """Get the latest block number"""
        return self.w3.eth.block_number
    
    def get_block_info(self, block_number: int) -> Dict[str, Any]:
        """Get information about a specific block"""
        try:
            block = self.w3.eth.get_block(block_number, full_transactions=True)
            return {
                "number": block.number,
                "hash": block.hash.hex(),
                "timestamp": block.timestamp,
                "transactions_count": len(block.transactions),
                "gas_used": block.gasUsed,
                "gas_limit": block.gasLimit
            }
        except Exception as e:
            logger.error(f"Error getting block {block_number}: {e}")
            return {}
    
    def get_transaction_receipt(self, tx_hash: str) -> Optional[Dict[str, Any]]:
        """Get transaction receipt"""
        try:
            receipt = self.w3.eth.get_transaction_receipt(tx_hash)
            return {
                "transaction_hash": receipt.transactionHash.hex(),
                "block_number": receipt.blockNumber,
                "gas_used": receipt.gasUsed,
                "status": receipt.status,
                "contract_address": receipt.contractAddress.hex() if receipt.contractAddress else None
            }
        except Exception as e:
            logger.error(f"Error getting transaction receipt {tx_hash}: {e}")
            return None
    
    def get_contract_events(self, contract_address: str, abi: List[Dict], 
                           event_name: str, from_block: int = 0, 
                           to_block: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get events from a smart contract
        
        Args:
            contract_address: Contract address
            abi: Contract ABI
            event_name: Name of the event to filter
            from_block: Starting block number
            to_block: Ending block number (None for latest)
        
        Returns:
            List of event data
        """
        try:
            contract = self.w3.eth.contract(address=contract_address, abi=abi)
            
            if to_block is None:
                to_block = self.get_latest_block()
            
            # Get event filter
            event_filter = contract.events[event_name].create_filter(
                fromBlock=from_block,
                toBlock=to_block
            )
            
            events = event_filter.get_all_entries()
            logger.info(f"Found {len(events)} {event_name} events from blocks {from_block}-{to_block}")
            
            return [self._parse_event(event) for event in events]
            
        except Exception as e:
            logger.error(f"Error getting contract events: {e}")
            return []
    
    def _parse_event(self, event) -> Dict[str, Any]:
        """Parse event data into a dictionary"""
        return {
            "event_name": event.event,
            "block_number": event.blockNumber,
            "transaction_hash": event.transactionHash.hex(),
            "log_index": event.logIndex,
            "args": dict(event.args)
        }
    
    def get_balance(self, address: str) -> float:
        """Get ETH balance for an address"""
        try:
            balance_wei = self.w3.eth.get_balance(address)
            balance_eth = self.w3.from_wei(balance_wei, 'ether')
            return float(balance_eth)
        except Exception as e:
            logger.error(f"Error getting balance for {address}: {e}")
            return 0.0
    
    def get_transaction_count(self, address: str) -> int:
        """Get transaction count (nonce) for an address"""
        try:
            return self.w3.eth.get_transaction_count(address)
        except Exception as e:
            logger.error(f"Error getting transaction count for {address}: {e}")
            return 0
    
    def estimate_gas(self, to_address: str, data: str = "", value: int = 0) -> int:
        """Estimate gas for a transaction"""
        try:
            gas_estimate = self.w3.eth.estimate_gas({
                'to': to_address,
                'data': data,
                'value': value
            })
            return gas_estimate
        except Exception as e:
            logger.error(f"Error estimating gas: {e}")
            return 0
    
    async def monitor_blocks(self, callback, interval: int = 1):
        """
        Monitor new blocks and call callback function
        
        Args:
            callback: Function to call with block data
            interval: Polling interval in seconds
        """
        last_block = self.get_latest_block()
        
        while True:
            try:
                current_block = self.get_latest_block()
                
                if current_block > last_block:
                    for block_num in range(last_block + 1, current_block + 1):
                        block_info = self.get_block_info(block_num)
                        await callback(block_info)
                    
                    last_block = current_block
                
                await asyncio.sleep(interval)
                
            except Exception as e:
                logger.error(f"Error in block monitoring: {e}")
                await asyncio.sleep(interval)
    
    def close(self):
        """Close the Web3 connection"""
        if hasattr(self.w3, 'provider') and hasattr(self.w3.provider, 'session'):
            self.w3.provider.session.close()
        logger.info("Ethereum client connection closed")


# Example usage
if __name__ == "__main__":
    # Initialize client
    client = EthereumClient("mainnet")
    
    # Get latest block
    latest_block = client.get_latest_block()
    print(f"Latest block: {latest_block}")
    
    # Get block info
    block_info = client.get_block_info(latest_block)
    print(f"Block info: {block_info}")
    
    # Close connection
    client.close()
