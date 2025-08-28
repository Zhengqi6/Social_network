# 项目总结 - 去中心化社交网络推荐算法

## 🎯 项目概述

本项目实现了基于以太坊的去中心化社交网络推荐算法系统，专注于从Lens Protocol、Farcaster等平台收集数据，并构建推荐模型。

**团队成员：**
- **Wu Yikai** - Farcaster.xyz, Hey.xyz (Lens.xyz)
- **Zhang Zhirui** - 数据收集协调者
- **Wang Zhengqi** - 基于Lens.xyz的其他应用，数据整理/分析协调者，链接预测模型

## 🏗️ 已完成架构

### 1. 核心模块

#### 🔗 区块链交互层
- **`ethereum_client.py`** - 完整的以太坊客户端
  - 支持主网和Polygon网络
  - 智能合约事件监控
  - 异步区块监控
  - 交易收据查询

#### 📊 数据收集层
- **`lens_collector.py`** - Lens协议数据收集器
  - 用户档案收集（Profiles）
  - 内容收集（Posts, Comments, Mirrors）
  - 关系收集（Follows）
  - 互动数据收集（Engagements）
  - 智能速率限制

#### 💾 多存储架构
- **`database.py`** - 统一数据库管理
  - **MongoDB**: 文档存储，支持索引和去重
  - **Neo4j**: 图数据库，社交网络关系建模
  - **Redis**: 高速缓存层，提升性能

#### 🚀 主收集器
- **`main_collector.py`** - 数据收集编排器
  - 多平台数据收集协调
  - 连续收集模式
  - 数据存储管理
  - 统计信息收集

### 2. 配置和部署

#### ⚙️ 配置管理
- **`settings.py`** - 集中配置管理
  - 区块链网络配置
  - 平台API配置
  - 数据库连接配置
  - 模型参数配置

#### 🐳 容器化部署
- **`docker-compose.yml`** - 完整服务编排
  - MongoDB, Neo4j, Redis服务
  - 数据收集服务
  - Jupyter Lab分析环境
  - Grafana监控面板
  - Prometheus指标收集

#### 🚀 快速启动
- **`start.sh`** - 一键部署脚本
  - Docker模式（推荐）
  - 本地模式
  - 系统测试
  - 服务状态检查

## 📊 数据模型

### 用户档案 (Profiles)
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
  "ens_name": "username.eth"
}
```

### 内容 (Posts)
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
  "created_at_timestamp": 1704067200
}
```

### 关系 (Follows)
```json
{
  "follow_id": "follower_following",
  "follower_id": "0x1234...",
  "following_id": "0x5678...",
  "timestamp": "2024-01-01T00:00:00Z"
}
```

## 🚀 使用方法

### 1. 快速启动（推荐）
```bash
# 使用Docker启动所有服务
./start.sh docker

# 或启动本地服务
./start.sh local
```

### 2. 数据收集
```bash
# 单次收集
python data_collection/main_collector.py --max-profiles 100 --max-posts 50

# 连续收集
python data_collection/main_collector.py --continuous --interval 60

# 查看统计
python data_collection/main_collector.py --stats
```

### 3. 系统测试
```bash
# 测试存储系统
python run_collection.py --storage-only

# 测试完整系统
python run_collection.py
```

## 🔧 技术特性

### 异步处理
- 使用`asyncio`实现高并发数据收集
- 智能速率限制，尊重API限制
- 错误处理和重试机制

### 数据去重
- MongoDB唯一索引
- Neo4j图关系去重
- Redis缓存去重

### 可扩展架构
- 模块化设计，易于添加新平台
- 插件式存储后端
- 配置驱动的参数管理

## 📈 性能指标

### 收集性能
- **Lens Protocol**: 50请求/分钟
- **Farcaster**: 100请求/分钟
- **Zora**: 200请求/分钟
- **批处理大小**: 1000条记录/批次

### 存储性能
- **MongoDB**: 支持分片和复制集
- **Neo4j**: 图查询优化
- **Redis**: 毫秒级缓存响应

## 🎯 下一步计划

### 阶段1: 数据收集完善（当前）
- [x] Lens Protocol收集器
- [ ] Farcaster收集器
- [ ] Zora收集器
- [ ] 数据质量验证

### 阶段2: 数据分析和特征工程
- [ ] 图分析算法
- [ ] 用户行为特征提取
- [ ] 内容特征工程
- [ ] 时间序列分析

### 阶段3: 推荐模型开发
- [ ] 链接预测模型（Wang Zhengqi负责）
- [ ] 参与度预测模型
- [ ] 病毒性预测模型
- [ ] 模型评估和优化

### 阶段4: 系统集成和部署
- [ ] 模型服务API
- [ ] 实时推荐引擎
- [ ] 性能监控和告警
- [ ] 生产环境部署

## 🌐 服务访问

### 本地开发环境
- **MongoDB**: `mongodb://localhost:27017`
- **Neo4j Browser**: `http://localhost:7474`
- **Redis**: `localhost:6379`
- **Jupyter Lab**: `http://localhost:8888`

### NUS服务器环境
- **服务器**: `172.28.122.124`
- **用户名**: `zhengqi`
- **密码**: `12345abcde`
- **要求**: 先连接NUS VPN

## 📚 文档和资源

### 项目文档
- **README.md** - 完整使用说明
- **PROJECT_SUMMARY.md** - 项目总结（本文档）
- **代码注释** - 详细的函数和类文档

### 外部资源
- **Lens Protocol**: https://docs.lens.xyz/
- **Farcaster**: https://docs.farcaster.xyz/
- **Ethereum**: https://ethereum.org/developers/
- **GraphQL**: https://graphql.org/

## 🐛 故障排除

### 常见问题
1. **连接超时** - 检查VPN连接和防火墙设置
2. **数据库连接失败** - 验证服务状态和配置
3. **API限制** - 调整收集频率和批次大小
4. **内存不足** - 减少批次大小，启用内存监控

### 调试模式
```bash
# 设置调试日志
export LOG_LEVEL=DEBUG

# 运行测试收集
python data_collection/main_collector.py --max-profiles 10 --max-posts 5
```

## 🤝 贡献指南

### 代码规范
- Python PEP 8标准
- 完整的类型注解
- 详细的文档字符串
- 单元测试覆盖

### 开发流程
1. 创建功能分支
2. 实现功能
3. 添加测试
4. 更新文档
5. 提交Pull Request

## 📞 联系信息

**项目负责人**: Feng Ling  
**沟通平台**: Microsoft Teams  
**服务器**: 172.28.122.124 (NUS VPN)  
**项目**: AIS5281 去中心化社交网络推荐算法

---

**项目状态**: 🟢 数据收集阶段完成，准备进入数据分析和模型开发阶段

**最后更新**: 2024年12月
**版本**: v1.0.0
