#!/usr/bin/env python3
"""
数据库浏览器脚本 - 查看和管理收集的数据
"""
import sys
import os
from datetime import datetime
from tabulate import tabulate

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data_collection.storage.database import DatabaseManager


class DatabaseBrowser:
    """数据库浏览器类"""
    
    def __init__(self):
        """初始化数据库连接"""
        try:
            self.db_manager = DatabaseManager()
            print("✅ 数据库连接成功！")
        except Exception as e:
            print(f"❌ 数据库连接失败: {e}")
            sys.exit(1)
    
    def show_mongodb_data(self):
        """显示MongoDB数据"""
        print("\n📊 MongoDB 数据概览")
        print("=" * 50)
        
        try:
            # 获取集合统计
            db = self.db_manager.mongodb_db
            
            collections = ['users', 'posts', 'interactions']
            for collection_name in collections:
                if collection_name in db.list_collection_names():
                    collection = db[collection_name]
                    count = collection.count_documents({})
                    print(f"📁 {collection_name}: {count} 条记录")
                    
                    # 显示前3条记录
                    if count > 0:
                        print(f"   前3条记录:")
                        docs = list(collection.find().limit(3))
                        for i, doc in enumerate(docs, 1):
                            # 清理ObjectId显示
                            if '_id' in doc:
                                doc['_id'] = str(doc['_id'])
                            print(f"   {i}. {doc}")
                        print()
                else:
                    print(f"📁 {collection_name}: 集合不存在")
                    
        except Exception as e:
            print(f"❌ 获取MongoDB数据失败: {e}")
    
    def show_neo4j_data(self):
        """显示Neo4j数据"""
        print("\n🕸️  Neo4j 图数据概览")
        print("=" * 50)
        
        try:
            if self.db_manager.neo4j_driver:
                with self.db_manager.neo4j_driver.session() as session:
                    # 获取节点统计
                    result = session.run("MATCH (n) RETURN labels(n) as labels, count(n) as count")
                    nodes_data = []
                    for record in result:
                        labels = record['labels'] if record['labels'] else ['Unknown']
                        nodes_data.append([', '.join(labels), record['count']])
                    
                    if nodes_data:
                        print("📊 节点统计:")
                        print(tabulate(nodes_data, headers=['标签', '数量'], tablefmt='grid'))
                    else:
                        print("📊 节点统计: 暂无数据")
                    
                    # 获取关系统计
                    result = session.run("MATCH ()-[r]->() RETURN type(r) as type, count(r) as count")
                    relationships_data = []
                    for record in result:
                        relationships_data.append([record['type'], record['count']])
                    
                    if relationships_data:
                        print("\n🔗 关系统计:")
                        print(tabulate(relationships_data, headers=['关系类型', '数量'], tablefmt='grid'))
                    else:
                        print("\n🔗 关系统计: 暂无数据")
                        
            else:
                print("❌ Neo4j驱动未初始化")
                
        except Exception as e:
            print(f"❌ 获取Neo4j数据失败: {e}")
    
    def show_redis_data(self):
        """显示Redis数据"""
        print("\n⚡ Redis 缓存数据概览")
        print("=" * 50)
        
        try:
            if self.db_manager.redis_client:
                # 获取键数量
                key_count = self.db_manager.redis_client.dbsize()
                print(f"🔑 总键数: {key_count}")
                
                if key_count > 0:
                    # 获取所有键
                    keys = self.db_manager.redis_client.keys('*')
                    print(f"📋 键列表:")
                    for i, key in enumerate(keys[:10], 1):  # 只显示前10个
                        value = self.db_manager.redis_client.get(key)
                        print(f"   {i}. {key}: {value}")
                    
                    if len(keys) > 10:
                        print(f"   ... 还有 {len(keys) - 10} 个键")
                else:
                    print("📋 暂无缓存数据")
            else:
                print("❌ Redis客户端未初始化")
                
        except Exception as e:
            print(f"❌ 获取Redis数据失败: {e}")
    
    def search_data(self, collection_name, query_field, query_value):
        """搜索特定数据"""
        print(f"\n🔍 在 {collection_name} 中搜索 {query_field} = {query_value}")
        print("=" * 50)
        
        try:
            db = self.db_manager.mongodb_db
            if collection_name in db.list_collection_names():
                collection = db[collection_name]
                
                # 构建查询
                if query_field == '_id':
                    from bson import ObjectId
                    query = {query_field: ObjectId(query_value)}
                else:
                    query = {query_field: query_value}
                
                results = list(collection.find(query).limit(5))
                
                if results:
                    print(f"✅ 找到 {len(results)} 条记录:")
                    for i, doc in enumerate(results, 1):
                        if '_id' in doc:
                            doc['_id'] = str(doc['_id'])
                        print(f"   {i}. {doc}")
                else:
                    print("❌ 未找到匹配的记录")
            else:
                print(f"❌ 集合 {collection_name} 不存在")
                
        except Exception as e:
            print(f"❌ 搜索失败: {e}")
    
    def show_collection_details(self, collection_name):
        """显示集合详细信息"""
        print(f"\n📋 {collection_name} 集合详细信息")
        print("=" * 50)
        
        try:
            db = self.db_manager.mongodb_db
            if collection_name in db.list_collection_names():
                collection = db[collection_name]
                
                # 总记录数
                total_count = collection.count_documents({})
                print(f"📊 总记录数: {total_count}")
                
                if total_count > 0:
                    # 字段统计
                    sample_doc = collection.find_one()
                    if sample_doc:
                        print(f"📝 字段列表:")
                        for field in sample_doc.keys():
                            print(f"   - {field}")
                    
                    # 平台分布（如果有platform字段）
                    if 'platform' in sample_doc:
                        pipeline = [
                            {"$group": {"_id": "$platform", "count": {"$sum": 1}}},
                            {"$sort": {"count": -1}}
                        ]
                        platform_stats = list(collection.aggregate(pipeline))
                        print(f"\n🌐 平台分布:")
                        for stat in platform_stats:
                            print(f"   {stat['_id']}: {stat['count']} 条")
                    
                    # 时间范围（如果有created_at字段）
                    if 'created_at' in sample_doc:
                        pipeline = [
                            {"$group": {
                                "_id": None,
                                "earliest": {"$min": "$created_at"},
                                "latest": {"$max": "$created_at"}
                            }}
                        ]
                        time_stats = list(collection.aggregate(pipeline))
                        if time_stats:
                            print(f"\n⏰ 时间范围:")
                            print(f"   最早: {time_stats[0]['earliest']}")
                            print(f"   最新: {time_stats[0]['latest']}")
                else:
                    print("📝 集合为空")
            else:
                print(f"❌ 集合 {collection_name} 不存在")
                
        except Exception as e:
            print(f"❌ 获取集合详情失败: {e}")
    
    def interactive_menu(self):
        """交互式菜单"""
        while True:
            print("\n" + "=" * 60)
            print("🗄️  数据库浏览器菜单")
            print("=" * 60)
            print("1. 📊 显示MongoDB数据概览")
            print("2. 🕸️  显示Neo4j图数据概览")
            print("3. ⚡ 显示Redis缓存数据概览")
            print("4. 🔍 搜索特定数据")
            print("5. 📋 显示集合详细信息")
            print("6. 🚪 退出")
            print("=" * 60)
            
            choice = input("请选择操作 (1-6): ").strip()
            
            if choice == '1':
                self.show_mongodb_data()
            elif choice == '2':
                self.show_neo4j_data()
            elif choice == '3':
                self.show_redis_data()
            elif choice == '4':
                collection = input("请输入集合名称 (users/posts/interactions): ").strip()
                field = input("请输入搜索字段: ").strip()
                value = input("请输入搜索值: ").strip()
                self.search_data(collection, field, value)
            elif choice == '5':
                collection = input("请输入集合名称 (users/posts/interactions): ").strip()
                self.show_collection_details(collection)
            elif choice == '6':
                print("👋 再见！")
                break
            else:
                print("❌ 无效选择，请重试")
    
    def close(self):
        """关闭数据库连接"""
        if self.db_manager:
            self.db_manager.close()


def main():
    """主函数"""
    print("🗄️  数据库浏览器启动中...")
    
    try:
        browser = DatabaseBrowser()
        
        # 显示所有数据概览
        browser.show_mongodb_data()
        browser.show_neo4j_data()
        browser.show_redis_data()
        
        # 启动交互式菜单
        browser.interactive_menu()
        
    except KeyboardInterrupt:
        print("\n🛑 程序被用户中断")
    except Exception as e:
        print(f"\n💥 程序运行错误: {e}")
    finally:
        if 'browser' in locals():
            browser.close()


if __name__ == "__main__":
    main()
