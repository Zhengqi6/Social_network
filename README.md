# Decentralized Social Network Recommendation Algorithms

Lens-only JSON pipeline for decentralized social recommendation. We collect from Lens GraphQL (chainId=232) and optionally verify via Lens Chain RPC. Output is local JSON/Parquet; no DB required.

## Project Overview


**Platform:**
- **Hey.xyz / Lens.xyz (Lens Chain 232)**

**Goals:**
1. **Account Recommendation** - Follow/link prediction
2. **Content Recommendation** - Individual engagement prediction
3. **Virality Prediction** - Total share/view prediction
4. **Sensor Node Selection** - Identify viral content early

## Architecture

```
decentralized-social-recommendation/
├── data_collection/           # Data collection modules
│   ├── blockchain/           # Blockchain interaction
│   │   ├── ethereum_client.py        # RPC封装（eth_getLogs）
│   │   └── lens_collector.py         # Lens采集器（Profiles/Posts/Follows/部分Engagements）
│   ├── api/                  # API clients
│   ├── storage/              # （可选/当前未用）
│   └── main_collector.py         # 主编排（JSON-only）
├── data_processing/          # Data curation and analysis
├── models/                   # ML/DL models
│   ├── link_prediction.py    # Account recommendation
│   ├── engagement_prediction.py # Content recommendation
│   └── virality_prediction.py   # Virality prediction
├── evaluation/               # Model evaluation
├── config/                   # Configuration
│   ├── settings.py           # 配置（GraphQL/RPC）
│   └── requirements.txt      # Dependencies
└── logs/                     # Log files
```

## Features

### 🔗 Blockchain Integration
- **Ethereum Client**: Full Web3 integration with mainnet and Polygon support
- **Smart Contract Events**: Monitor and collect on-chain events
- **Multi-chain Support**: Extensible for different blockchain networks

### 📊 Data Collection
- **Lens Protocol**: Comprehensive profile, post, and engagement data
- **Rate Limiting**: Respectful API usage with configurable limits
- **Async Processing**: High-performance concurrent data collection
- **Error Handling**: Robust error handling and retry mechanisms

### 💾 Multi-Storage Architecture
- **MongoDB**: Document storage for profiles, posts, and interactions
- **Neo4j**: Graph database for social network relationships
- **Redis**: High-speed caching layer
- **Data Deduplication**: Smart handling of duplicate data

### 🤖 AI/ML Ready
- **Graph Neural Networks**: PyTorch Geometric integration
- **Feature Engineering**: Comprehensive user and content features
- **Model Pipeline**: End-to-end ML pipeline support

## Installation

### Prerequisites
- Python 3.8+
- MongoDB 4.4+
- Neo4j 4.4+
- Redis 6.0+

### 1. Clone Repository
```bash
git clone <repository-url>
cd decentralized-social-recommendation
```

### 2. Create Virtual Environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies
```bash
pip install -r config/requirements.txt
```

### 4. Environment Configuration
Create a `.env` file in the project root:
```bash
# Ethereum Configuration
ETHEREUM_MAINNET_RPC=https://mainnet.infura.io/v3/YOUR_PROJECT_ID
POLYGON_RPC=https://polygon-rpc.com

# Database Configuration
MONGODB_URI=mongodb://localhost:27017
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=

# Lens Protocol (if required)
LENS_API_KEY=your_lens_api_key

# Logging
LOG_LEVEL=INFO
```

### 5. Start Services
```bash
# Start MongoDB
mongod

# Start Neo4j
neo4j start

# Start Redis
redis-server
```

## Usage

### Basic Data Collection

#### 1. Single Collection Cycle
```bash
python data_collection/main_collector.py --max-profiles 100 --max-posts 50
```

#### 2. Continuous Collection
```bash
python data_collection/main_collector.py --continuous --interval 60 --max-profiles 50 --max-posts 25
```

#### 3. View Statistics
```bash
python data_collection/main_collector.py --stats
```

### Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--api-key` | API key for platforms requiring authentication | None |
| `--max-profiles` | Maximum profiles to collect per platform | 100 |
| `--max-posts` | Maximum posts per profile | 50 |
| `--continuous` | Run continuous collection | False |
| `--interval` | Collection interval in minutes | 60 |
| `--stats` | Show collection statistics | False |

### Programmatic Usage

```python
import asyncio
from data_collection.main_collector import MainDataCollector

async def main():
    # Initialize collector
    collector = MainDataCollector(api_key="your_api_key")
    
    try:
        # Collect data from all platforms
        data = await collector.collect_all_platforms(
            max_profiles=100,
            max_posts_per_profile=50
        )
        
        print(f"Collected {len(data['lens']['profiles'])} Lens profiles")
        
    finally:
        collector.close()

# Run
asyncio.run(main())
```

## Data Collection Process

### 1. Profile Collection
- **User Information**: Handle, name, bio, profile pictures
- **Social Stats**: Follower/following counts, post counts
- **On-chain Identity**: ENS names, Sybil verification, proof of humanity
- **Follow Modules**: Custom follow logic and NFT addresses

### 2. Content Collection
- **Posts**: Text content, media, metadata, engagement stats
- **Comments**: Threaded conversations and interactions
- **Mirrors**: Content sharing and amplification
- **Timestamps**: Creation and interaction timing

### 3. Relationship Mapping
- **Follow Networks**: Who follows whom
- **Engagement Patterns**: Likes, comments, mirrors, collects
- **Content Interactions**: User-content engagement history

### 4. Storage Strategy
- **MongoDB**: Raw data storage with indexing
- **Neo4j**: Graph relationships and network analysis
- **Redis**: High-frequency data caching
- **Data Deduplication**: Smart handling of repeated data

## Data Schema

### User Profiles
```json
{
  "profile_id": "0x1234...",
  "handle": "username",
  "name": "Display Name",
  "bio": "User biography",
  "total_followers": 1000,
  "total_following": 500,
  "total_posts": 150,
  "owned_by": "0xabcd...",
  "proof_of_humanity": true,
  "ens_name": "username.eth",
  "collected_at": "2024-01-01T00:00:00Z"
}
```

### Posts
```json
{
  "publication_id": "0x5678...",
  "profile_id": "0x1234...",
  "type": "post",
  "content": "Post content",
  "media_urls": ["https://..."],
  "total_mirrors": 25,
  "total_comments": 15,
  "total_collects": 8,
  "created_at_timestamp": 1704067200,
  "collected_at": "2024-01-01T00:00:00Z"
}
```

### Follow Relationships
```json
{
  "follow_id": "follower_following",
  "follower_id": "0x1234...",
  "following_id": "0x5678...",
  "timestamp": "2024-01-01T00:00:00Z",
  "collected_at": "2024-01-01T00:00:00Z"
}
```

## Development

### Adding New Platforms

1. **Create Collector Class**
```python
class NewPlatformCollector:
    def __init__(self, api_key=None):
        # Initialize platform-specific client
        
    async def collect_profiles(self, limit=100, offset=0):
        # Implement profile collection
        
    async def collect_posts(self, profile_id=None, limit=100, offset=0):
        # Implement post collection
```

2. **Add to Main Collector**
```python
def _initialize_collectors(self):
    # ... existing code ...
    self.collectors["new_platform"] = NewPlatformCollector(api_key=self.api_key)
```

3. **Update Configuration**
```python
# In config/settings.py
PLATFORM_APIS["new_platform"] = {
    "api_url": "https://api.newplatform.com",
    "graphql_endpoint": "https://api.newplatform.com/graphql",
    "rate_limit": 100,
    "api_key_required": False
}
```

### Adding New Data Types

1. **Extend Storage Classes**
2. **Update Data Processing**
3. **Add to Feature Engineering**
4. **Update Model Inputs**

## Monitoring and Logging

### Log Files
- **Location**: `logs/data_collection.log`
- **Rotation**: Daily
- **Retention**: 7 days
- **Format**: Timestamp | Level | Message

### Key Metrics
- **Collection Rate**: Profiles/posts per minute
- **Success Rate**: Successful vs failed requests
- **Storage Performance**: Database operation timing
- **Error Tracking**: Failed requests and exceptions

## Performance Considerations

### Rate Limiting
- **Lens Protocol**: 50 requests/minute
- **Farcaster**: 100 requests/minute
- **Zora**: 200 requests/minute
- **Configurable delays** between requests

### Batch Processing
- **Profile Collection**: 100 profiles per batch
- **Post Collection**: 100 posts per batch
- **Parallel Processing**: Async operations for multiple platforms

### Storage Optimization
- **Indexing**: Database indexes on key fields
- **Caching**: Redis for frequently accessed data
- **Compression**: Efficient data storage formats

## Troubleshooting

### Common Issues

1. **Connection Timeouts**
   - Check VPN connection (NUS VPN required)
   - Verify server accessibility
   - Check firewall settings

2. **Database Connection Errors**
   - Verify service status (MongoDB, Neo4j, Redis)
   - Check connection strings in `.env`
   - Verify authentication credentials

3. **API Rate Limiting**
   - Reduce collection frequency
   - Increase `rate_limit_delay` in settings
   - Use multiple API keys if available

4. **Memory Issues**
   - Reduce batch sizes
   - Implement data streaming for large datasets
   - Monitor system resources

### Debug Mode
```bash
# Set log level to DEBUG
export LOG_LEVEL=DEBUG

# Run with verbose output
python data_collection/main_collector.py --max-profiles 10 --max-posts 5
```

## Contributing

### Code Style
- **Python**: PEP 8 compliance
- **Documentation**: Docstrings for all functions
- **Type Hints**: Full type annotation
- **Testing**: Unit tests for new features

### Development Workflow
1. Create feature branch
2. Implement changes
3. Add tests
4. Update documentation
5. Submit pull request

## License

This project is part of AIS5281 course work at NUS.

## Contact


