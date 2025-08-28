# 🚀 快速启动指南

## 📋 前置要求

- macOS 或 Linux 系统
- 至少 4GB 可用内存
- 至少 10GB 可用磁盘空间

## 🐳 一键搭建环境

### 1. 运行Docker环境搭建脚本

```bash
# 在项目根目录执行
./setup_docker_stack.sh
```

这个脚本会自动：
- 检查并安装Docker（Linux）
- 创建必要的目录结构
- 启动MongoDB、Redis、Neo4j服务
- 执行健康检查
- 创建环境配置文件

### 2. 如果脚本执行失败

#### macOS用户：
```bash
# 手动安装Docker Desktop
# 访问: https://www.docker.com/products/docker-desktop/
# 下载并安装后重新运行脚本
```

#### Linux用户：
```bash
# 手动安装Docker
sudo apt update
sudo apt install -y docker.io docker-compose

# 启动Docker服务
sudo systemctl start docker
sudo systemctl enable docker

# 将用户添加到docker组
sudo usermod -aG docker $USER
newgrp docker

# 重新运行脚本
./setup_docker_stack.sh
```

## 🔍 验证环境

### 1. 检查Docker服务状态

```bash
cd ~/social-stack
docker compose ps
```

应该看到三个服务都在运行：
- mongo (MongoDB)
- redis (Redis)
- neo4j (Neo4j)

### 2. 测试数据库连接

```bash
# 在项目根目录执行
python test_connections.py
```

这个脚本会测试：
- MongoDB连接和基本操作
- Neo4j连接和图操作
- Redis连接和缓存操作
- 存储接口功能

## 🚀 开始数据收集

### 1. 单次数据收集

```bash
python run_data_collection.py --max-profiles 50 --max-posts 25
```

### 2. 连续数据收集

```bash
python run_data_collection.py --continuous --interval 60
```

### 3. 查看收集统计

```bash
python run_data_collection.py --stats
```

## 📊 访问服务界面

### MongoDB
- 端口：27017
- 连接串：`mongodb://root:rootpass123@127.0.0.1:27017/social_recommendation?authSource=admin`

### Redis
- 端口：6379
- 密码：redispass123

### Neo4j Browser
- 端口：7474
- 访问：http://127.0.0.1:7474
- 用户名：neo4j
- 密码：neo4jpass123

## 🛠️ 常用管理命令

### Docker服务管理
```bash
cd ~/social-stack

# 查看服务状态
docker compose ps

# 查看服务日志
docker compose logs

# 重启服务
docker compose restart

# 停止服务
docker compose down

# 启动服务
docker compose up -d
```

### 数据备份
```bash
# MongoDB备份
docker exec mongo mongodump --out /data/backup

# Redis备份
docker exec redis redis-cli -a redispass123 BGSAVE

# Neo4j备份
docker exec neo4j neo4j-admin database backup neo4j
```

## 🔧 故障排除

### 常见问题

#### 1. 端口被占用
```bash
# 检查端口占用
sudo netstat -tlnp | grep -E '27017|6379|7474|7687'

# 停止占用端口的服务
sudo systemctl stop mongod  # 如果本地MongoDB在运行
sudo systemctl stop redis   # 如果本地Redis在运行
```

#### 2. 权限问题
```bash
# 修复目录权限
sudo chown -R $USER:$USER ~/social-stack/*

# 重新启动服务
docker compose down
docker compose up -d
```

#### 3. 内存不足
```bash
# 检查系统内存
free -h

# 如果内存不足，减少Neo4j内存使用
# 编辑 docker-compose.yml 中的 neo4j 服务
# 添加环境变量：
# NEO4J_server_memory_heap_initial__size: "512m"
# NEO4J_server_memory_heap_max__size: "1g"
```

#### 4. 连接失败
```bash
# 检查服务健康状态
docker compose ps

# 查看详细日志
docker compose logs mongo
docker compose logs redis
docker compose logs neo4j

# 重启问题服务
docker compose restart mongo
```

## 📈 性能优化

### 1. 调整MongoDB
```yaml
# 在 docker-compose.yml 中添加
mongo:
  environment:
    MONGO_INITDB_ROOT_USERNAME: root
    MONGO_INITDB_ROOT_PASSWORD: rootpass123
    MONGO_INITDB_DATABASE: social_recommendation
  command: ["mongod", "--wiredTigerCacheSizeGB", "1"]
```

### 2. 调整Redis
```yaml
# 在 docker-compose.yml 中添加
redis:
  command: ["redis-server", "--appendonly", "yes", "--requirepass", "redispass123", "--maxmemory", "512mb", "--maxmemory-policy", "allkeys-lru"]
```

### 3. 调整Neo4j
```yaml
# 在 docker-compose.yml 中添加
neo4j:
  environment:
    NEO4J_server_memory_heap_initial__size: "512m"
    NEO4J_server_memory_heap_max__size: "1g"
    NEO4J_server_memory_pagecache_size: "256m"
```

## 🎯 下一步

环境搭建完成后，你可以：

1. **开始数据收集**：运行数据收集脚本
2. **开发推荐算法**：基于收集的数据构建模型
3. **系统监控**：设置监控和告警
4. **生产部署**：部署到生产环境

## 📞 获取帮助

如果遇到问题：

1. 查看Docker服务日志
2. 检查系统资源使用情况
3. 参考故障排除部分
4. 查看项目文档

---

**快速启动完成时间**：2025-08-28  
**项目状态**：Ready for Data Collection 🚀
