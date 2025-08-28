#!/usr/bin/env python3
"""
测试数据库连接的脚本
"""
import asyncio
import sys
import os
from loguru import logger

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data_collection.storage.database import DatabaseManager


async def test_mongodb_connection():
    """测试MongoDB连接"""
    print("🔍 测试 MongoDB 连接...")
    
    try:
        db_manager = DatabaseManager()
        
        # 测试MongoDB连接
        if db_manager.mongodb_client:
            # 获取数据库信息
            db_info = db_manager.mongodb_client.admin.command('serverStatus')
            print(f"✅ MongoDB 连接成功!")
            print(f"   版本: {db_info.get('version', 'Unknown')}")
            print(f"   连接数: {db_info.get('connections', {}).get('current', 'Unknown')}")
            
            # 测试数据库操作
            db = db_manager.mongodb_client.social_recommendation
            collection = db.test_connection
            
            # 插入测试数据
            result = collection.insert_one({"test": "connection", "timestamp": "2025-08-28"})
            print(f"   ✅ 写入测试: 插入ID {result.inserted_id}")
            
            # 查询测试数据
            doc = collection.find_one({"test": "connection"})
            print(f"   ✅ 读取测试: 找到文档 {doc}")
            
            # 清理测试数据
            collection.delete_one({"test": "connection"})
            print(f"   ✅ 删除测试: 清理完成")
            
            return True
        else:
            print("❌ MongoDB 客户端未初始化")
            return False
            
    except Exception as e:
        print(f"❌ MongoDB 连接失败: {e}")
        return False


async def test_neo4j_connection():
    """测试Neo4j连接"""
    print("\n🔍 测试 Neo4j 连接...")
    
    try:
        db_manager = DatabaseManager()
        
        # 测试Neo4j连接
        if db_manager.neo4j_driver:
            with db_manager.neo4j_driver.session() as session:
                # 测试基本查询
                result = session.run("RETURN 1 as test")
                record = result.single()
                print(f"✅ Neo4j 连接成功!")
                print(f"   测试查询结果: {record['test']}")
                
                # 测试创建节点
                result = session.run("CREATE (n:TestNode {name: 'connection_test'}) RETURN n")
                node = result.single()
                print(f"   ✅ 创建节点测试: 成功")
                
                # 测试查询节点
                result = session.run("MATCH (n:TestNode {name: 'connection_test'}) RETURN n")
                node = result.single()
                print(f"   ✅ 查询节点测试: 成功")
                
                # 清理测试节点
                session.run("MATCH (n:TestNode {name: 'connection_test'}) DELETE n")
                print(f"   ✅ 删除节点测试: 清理完成")
                
            return True
        else:
            print("❌ Neo4j 驱动未初始化")
            return False
            
    except Exception as e:
        print(f"❌ Neo4j 连接失败: {e}")
        return False


async def test_redis_connection():
    """测试Redis连接"""
    print("\n🔍 测试 Redis 连接...")
    
    try:
        db_manager = DatabaseManager()
        
        # 测试Redis连接
        if db_manager.redis_client:
            # 测试基本操作
            db_manager.redis_client.set("test_key", "connection_test")
            value = db_manager.redis_client.get("test_key")
            print(f"✅ Redis 连接成功!")
            print(f"   写入测试: 设置键 'test_key'")
            print(f"   读取测试: 值 '{value}'")
            
            # 测试删除
            db_manager.redis_client.delete("test_key")
            print(f"   ✅ 删除测试: 清理完成")
            
            return True
        else:
            print("❌ Redis 客户端未初始化")
            return False
            
    except Exception as e:
        print(f"❌ Redis 连接失败: {e}")
        return False


async def test_storage_operations():
    """测试存储操作"""
    print("\n🔍 测试存储操作...")
    
    try:
        db_manager = DatabaseManager()
        
        # 测试MongoDB存储
        if db_manager.mongodb_client:
            from data_collection.storage.database import MongoDBStorage
            mongodb_storage = MongoDBStorage(db_manager)
            
            # 测试存储用户
            test_user = {
                "user_id": "test_user_001",
                "username": "test_user",
                "display_name": "Test User",
                "created_at": "2025-08-28T00:00:00Z",
                "platform": "test"
            }
            
            result = await mongodb_storage.store_profiles([test_user])
            print(f"✅ MongoDB 存储测试: 插入 {result} 个用户")
            
            # 测试查询用户
            users = await mongodb_storage.get_profiles(limit=1)
            print(f"   ✅ 查询测试: 找到 {len(users)} 个用户")
            
            # 清理测试数据
            db_manager.mongodb_client.social_recommendation.profiles.delete_one({"user_id": "test_user_001"})
            print(f"   ✅ 清理测试: 删除测试用户")
            
        return True
        
    except Exception as e:
        print(f"❌ 存储操作测试失败: {e}")
        return False


async def main():
    """主函数"""
    print("🚀 数据库连接测试")
    print("=" * 50)
    
    results = {}
    
    try:
        # 测试各个数据库连接
        results["mongodb"] = await test_mongodb_connection()
        results["neo4j"] = await test_neo4j_connection()
        results["redis"] = await test_redis_connection()
        results["storage"] = await test_storage_operations()
        
        # 显示测试结果
        print("\n" + "=" * 50)
        print("📊 测试结果总结:")
        print("=" * 50)
        
        for service, success in results.items():
            status = "✅ 通过" if success else "❌ 失败"
            print(f"   {service.upper()}: {status}")
        
        # 计算成功率
        success_count = sum(results.values())
        total_count = len(results)
        success_rate = (success_count / total_count) * 100
        
        print(f"\n📈 成功率: {success_count}/{total_count} ({success_rate:.1f}%)")
        
        if success_rate == 100:
            print("\n🎉 所有测试通过！系统准备就绪！")
            print("\n🚀 下一步操作:")
            print("   1. 运行数据收集: python run_data_collection.py")
            print("   2. 开始算法开发")
            print("   3. 部署到生产环境")
        else:
            print(f"\n⚠️  有 {total_count - success_count} 个服务测试失败")
            print("请检查Docker服务状态和配置")
        
        return success_rate == 100
        
    except Exception as e:
        print(f"\n💥 测试过程中发生错误: {e}")
        return False


if __name__ == "__main__":
    try:
        success = asyncio.run(main())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n🛑 测试被用户中断")
        sys.exit(0)
    except Exception as e:
        print(f"\n💥 测试脚本失败: {e}")
        sys.exit(1)
