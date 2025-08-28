#!/bin/bash

# 🚀 去中心化社交推荐系统 - Docker环境搭建脚本
# 一键搭建 MongoDB + Redis + Neo4j 环境

set -e  # 遇到错误立即退出

echo "🚀 开始搭建Docker环境..."

# 检查操作系统
if [[ "$OSTYPE" == "darwin"* ]]; then
    OS="macos"
    echo "✅ 检测到 macOS 系统"
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    OS="linux"
    echo "✅ 检测到 Linux 系统"
else
    echo "❌ 不支持的操作系统: $OSTYPE"
    exit 1
fi

# 检查Docker是否已安装
if ! command -v docker &> /dev/null; then
    echo "❌ Docker 未安装，开始安装..."
    
    if [[ "$OS" == "macos" ]]; then
        echo "📥 在 macOS 上安装 Docker Desktop..."
        echo "请访问 https://www.docker.com/products/docker-desktop/ 下载并安装 Docker Desktop"
        echo "安装完成后重新运行此脚本"
        exit 1
    elif [[ "$OS" == "linux" ]]; then
        echo "📥 在 Linux 上安装 Docker..."
        
        # 更新包索引
        sudo apt update
        
        # 安装必要的包
        sudo apt install -y ca-certificates curl gnupg
        
        # 添加Docker官方GPG密钥
        sudo install -m 0755 -d /etc/apt/keyrings
        curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
        
        # 添加Docker仓库
        echo \
          "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
          https://download.docker.com/linux/ubuntu \
          $(. /etc/os-release; echo $UBUNTU_CODENAME) stable" \
        | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
        
        # 更新包索引并安装Docker
        sudo apt update
        sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
        
        # 将当前用户添加到docker组
        sudo usermod -aG docker $USER
        echo "✅ Docker 安装完成！请重新登录或运行 'newgrp docker' 使权限生效"
        newgrp docker
    fi
else
    echo "✅ Docker 已安装: $(docker --version)"
fi

# 检查Docker Compose
if ! command -v docker compose &> /dev/null; then
    echo "❌ Docker Compose 未安装"
    exit 1
else
    echo "✅ Docker Compose 已安装: $(docker compose version)"
fi

# 创建项目目录
PROJECT_DIR="$HOME/social-stack"
echo "📁 创建项目目录: $PROJECT_DIR"

mkdir -p "$PROJECT_DIR"/{mongo-data,redis-data,neo4j-data,neo4j-logs,neo4j-plugins}
cd "$PROJECT_DIR"

# 创建docker-compose.yml文件
echo "📝 创建 docker-compose.yml 文件..."

cat > docker-compose.yml << 'EOF'
version: "3.9"
services:
  mongo:
    image: mongo:7
    container_name: mongo
    restart: unless-stopped
    ports:
      - "127.0.0.1:27017:27017"   # 仅本机访问，更安全
    environment:
      MONGO_INITDB_ROOT_USERNAME: root
      MONGO_INITDB_ROOT_PASSWORD: rootpass123
      MONGO_INITDB_DATABASE: social_recommendation
    volumes:
      - ./mongo-data:/data/db
    healthcheck:
      test: ["CMD", "mongosh", "--quiet", "--eval", "db.runCommand({ ping: 1 })"]
      interval: 10s
      timeout: 5s
      retries: 5

  redis:
    image: redis:7
    container_name: redis
    restart: unless-stopped
    command: ["redis-server", "--appendonly", "yes", "--requirepass", "redispass123"]
    ports:
      - "127.0.0.1:6379:6379"
    volumes:
      - ./redis-data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "-a", "redispass123", "PING"]
      interval: 10s
      timeout: 5s
      retries: 5

  neo4j:
    image: neo4j:5-community
    container_name: neo4j
    restart: unless-stopped
    ports:
      - "127.0.0.1:7474:7474"   # Neo4j Browser (HTTP)
      - "127.0.0.1:7687:7687"   # Bolt
    environment:
      NEO4J_AUTH: neo4j/neo4jpass123
      NEO4J_server_default__listen__address: "0.0.0.0"
      NEO4J_server_http_listen__address: ":7474"
      NEO4J_server_bolt_listen__address: ":7687"
      NEO4J_server_directories_plugins: "/plugins"
    volumes:
      - ./neo4j-data:/data
      - ./neo4j-logs:/logs
      - ./neo4j-plugins:/plugins
    healthcheck:
      test: ["CMD-SHELL", "wget -qO- http://localhost:7474/browser || exit 1"]
      interval: 15s
      timeout: 10s
      retries: 10
EOF

echo "✅ docker-compose.yml 创建完成"

# 启动服务
echo "🚀 启动Docker服务..."
docker compose up -d

# 等待服务启动
echo "⏳ 等待服务启动..."
sleep 10

# 检查服务状态
echo "🔍 检查服务状态..."
docker ps

# 健康检查
echo "🏥 执行健康检查..."

# MongoDB健康检查
echo "📊 检查 MongoDB..."
if docker exec mongo mongosh --quiet --eval "db.runCommand({ping:1})" > /dev/null 2>&1; then
    echo "✅ MongoDB 健康检查通过"
else
    echo "❌ MongoDB 健康检查失败"
fi

# Redis健康检查
echo "📊 检查 Redis..."
if docker exec redis redis-cli -a redispass123 PING | grep -q "PONG"; then
    echo "✅ Redis 健康检查通过"
else
    echo "❌ Redis 健康检查失败"
fi

# Neo4j健康检查
echo "📊 检查 Neo4j..."
if docker exec neo4j cypher-shell -u neo4j -p neo4jpass123 "RETURN 1;" > /dev/null 2>&1; then
    echo "✅ Neo4j 健康检查通过"
else
    echo "❌ Neo4j 健康检查失败"
fi

# 创建环境配置文件
echo "📝 创建环境配置文件..."

cat > .env << 'EOF'
# 数据库连接配置
MONGODB_URI=mongodb://root:rootpass123@127.0.0.1:27017/social_recommendation?authSource=admin
REDIS_URI=redis://:redispass123@127.0.0.1:6379/0
NEO4J_URI=bolt://127.0.0.1:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=neo4jpass123

# 数据收集配置
MAX_PROFILES=100
MAX_POSTS_PER_PROFILE=50
COLLECTION_INTERVAL_MINUTES=60
CONTINUOUS_MODE=false

# 日志配置
LOG_LEVEL=INFO
EOF

echo "✅ 环境配置文件创建完成"

# 显示连接信息
echo ""
echo "🎉 Docker环境搭建完成！"
echo "=================================="
echo "📊 服务状态:"
echo "   MongoDB: 127.0.0.1:27017"
echo "   Redis:   127.0.0.1:6379"
echo "   Neo4j:   127.0.0.1:7474 (Browser), 127.0.0.1:7687 (Bolt)"
echo ""
echo "🔑 连接凭据:"
echo "   MongoDB: root / rootpass123"
echo "   Redis:   (无用户名) / redispass123"
echo "   Neo4j:   neo4j / neo4jpass123"
echo ""
echo "📁 数据目录: $PROJECT_DIR"
echo "   mongo-data/     - MongoDB数据"
echo "   redis-data/     - Redis数据"
echo "   neo4j-data/     - Neo4j数据"
echo "   neo4j-logs/     - Neo4j日志"
echo ""
echo "🚀 下一步操作:"
echo "   1. 测试连接: python test_connections.py"
echo "   2. 运行数据收集: python run_data_collection.py"
echo "   3. 查看服务日志: docker compose logs"
echo ""
echo "🛑 停止服务: docker compose down"
echo "🔄 重启服务: docker compose restart"
echo "📋 查看状态: docker compose ps"
