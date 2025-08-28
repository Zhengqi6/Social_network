"""
Configuration settings for decentralized social recommendation project
"""
import os
from typing import Dict, List
from dotenv import load_dotenv

load_dotenv()

# Blockchain Configuration
ETHEREUM_NETWORKS = {
    "mainnet": {
        "rpc_url": os.getenv("ETHEREUM_MAINNET_RPC", "https://mainnet.infura.io/v3/YOUR_PROJECT_ID"),
        "chain_id": 1,
        "explorer": "https://etherscan.io"
    },
    "polygon": {
        "rpc_url": os.getenv("POLYGON_RPC", "https://polygon-rpc.com"),
        "chain_id": 137,
        "explorer": "https://polygonscan.com"
    },
    "lens_chain": {
        "rpc_url": os.getenv("LENS_CHAIN_RPC", "https://mainnet.era.zksync.io"),
        "chain_id": 324,  # ZKSync Era Mainnet
        "explorer": "https://explorer.zksync.io",
        "layer2": True,
        "zk_proofs": True
    }
}

# Social Platform APIs
PLATFORM_APIS = {
    "farcaster": {
        "api_url": "https://api.farcaster.xyz",
        "graphql_endpoint": "https://api.farcaster.xyz/graphql",
        "rate_limit": 100,  # requests per minute
        "api_key_required": False
    },
    "lens_chain": {
        "api_url": "https://api.lens.xyz",
        "graphql_endpoint": "https://api.lens.xyz/graphql",
        "rate_limit": 50,
        "api_key_required": False,
        "chain_type": "layer2",
        "rpc_endpoints": [
            "https://rpc.zksync.io",
            "https://mainnet.era.zksync.io"
        ]
    },
    "zora": {
        "api_url": "https://api.zora.co",
        "graphql_endpoint": "https://api.zora.co/graphql",
        "rate_limit": 200,
        "api_key_required": False
    }
}

# Database Configuration
DATABASE_CONFIG = {
    "neo4j": {
        "uri": os.getenv("NEO4J_URI", "bolt://127.0.0.1:7687"),
        "user": os.getenv("NEO4J_USER", "neo4j"),
        "password": os.getenv("NEO4J_PASSWORD", "neo4jpass123"),
        "database": os.getenv("NEO4J_DATABASE", "neo4j")
    },
    "mongodb": {
        "uri": os.getenv("MONGODB_URI", "mongodb://root:rootpass123@127.0.0.1:27017/social_recommendation?authSource=admin"),
        "database": os.getenv("MONGODB_DATABASE", "social_recommendation"),
        "collections": {
            "users": "users",
            "posts": "posts",
            "interactions": "interactions",
            "graphs": "graphs"
        }
    },
    "redis": {
        "host": os.getenv("REDIS_HOST", "127.0.0.1"),
        "port": int(os.getenv("REDIS_PORT", 6379)),
        "db": int(os.getenv("REDIS_DB", 0)),
        "password": os.getenv("REDIS_PASSWORD", "redispass123")
    }
}

# Data Collection Settings
COLLECTION_CONFIG = {
    "batch_size": 1000,
    "max_retries": 3,
    "retry_delay": 5,  # seconds
    "timeout": 30,  # seconds
    "rate_limit_delay": 1.0,  # seconds between requests
    "max_concurrent_requests": 10
}

# Model Configuration
MODEL_CONFIG = {
    "link_prediction": {
        "embedding_dim": 128,
        "hidden_dim": 256,
        "num_layers": 3,
        "dropout": 0.2,
        "learning_rate": 0.001,
        "batch_size": 64,
        "epochs": 100
    },
    "engagement_prediction": {
        "embedding_dim": 128,
        "hidden_dim": 256,
        "num_layers": 2,
        "dropout": 0.3,
        "learning_rate": 0.001,
        "batch_size": 32,
        "epochs": 150
    },
    "virality_prediction": {
        "embedding_dim": 64,
        "hidden_dim": 128,
        "num_layers": 2,
        "dropout": 0.2,
        "learning_rate": 0.0005,
        "batch_size": 16,
        "epochs": 200
    }
}

# Logging Configuration
LOGGING_CONFIG = {
    "level": os.getenv("LOG_LEVEL", "INFO"),
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "file": "logs/social_recommendation.log"
}

# Feature Engineering
FEATURE_CONFIG = {
    "user_features": [
        "follower_count", "following_count", "post_count",
        "engagement_rate", "account_age", "verification_status"
    ],
    "content_features": [
        "text_length", "hashtag_count", "mention_count",
        "media_count", "post_time", "day_of_week"
    ],
    "interaction_features": [
        "interaction_type", "timestamp", "user_similarity",
        "content_relevance", "network_position"
    ]
}
