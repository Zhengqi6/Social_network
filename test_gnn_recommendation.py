"""
Test GNN Recommendation System
测试基于图神经网络的推荐系统
"""

import asyncio
import pandas as pd
import numpy as np
import torch
from typing import Dict, List, Any
import json
from datetime import datetime
from loguru import logger

# 导入数据处理模块
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data_processing.process_pipeline import DataProcessingPipeline
from models.gnn_recommendation import GNNRecommender


class GNNRecommendationSystem:
    """GNN推荐系统"""
    
    def __init__(self):
        self.data_pipeline = DataProcessingPipeline()
        self.gnn_recommender = None
        self.processed_data = None
        self.graph_data = None
        
    async def initialize(self):
        """初始化系统"""
        try:
            await self.data_pipeline.initialize()
            logger.info("GNN recommendation system initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize GNN system: {e}")
            raise
    
    async def process_data(self):
        """处理数据"""
        try:
            logger.info("🔄 Processing data for GNN recommendation system...")
            
            # 运行数据处理管道
            self.processed_data = await self.data_pipeline.run_full_pipeline()
            
            logger.info("✅ Data processing completed")
            return self.processed_data
            
        except Exception as e:
            logger.error(f"Error processing data: {e}")
            raise
    
    def setup_gnn_model(self):
        """设置GNN模型"""
        try:
            if not self.processed_data:
                raise ValueError("No processed data available")
            
            # 获取用户和帖子特征维度
            users_df = self.processed_data.get("engineered_features", {}).get("users", pd.DataFrame())
            posts_df = self.processed_data.get("engineered_features", {}).get("posts", pd.DataFrame())
            
            if users_df.empty or posts_df.empty:
                raise ValueError("No user or post features available")
            
            # 选择特征列
            user_feature_cols = [col for col in users_df.columns if col.endswith('_normalized') or col in ['platform_lens', 'is_recent_user']]
            if not user_feature_cols:
                user_feature_cols = ['platform_lens', 'is_recent_user']
            
            post_feature_cols = [col for col in posts_df.columns if col.endswith('_normalized') or col in ['platform_lens', 'is_recent_post']]
            if not post_feature_cols:
                post_feature_cols = ['platform_lens', 'is_recent_post']
            
            user_features_dim = len(user_feature_cols)
            post_features_dim = len(post_feature_cols)
            
            logger.info(f"User features: {user_features_dim}, Post features: {post_features_dim}")
            
            # 初始化GNN推荐器
            device = 'cuda' if torch.cuda.is_available() else 'cpu'
            self.gnn_recommender = GNNRecommender(
                user_features=user_features_dim,
                post_features=post_features_dim,
                hidden_dim=32,  # 减小隐藏层维度以适应小数据集
                num_layers=2,
                learning_rate=0.001,
                device=device
            )
            
            logger.info(f"GNN model initialized on {device}")
            
        except Exception as e:
            logger.error(f"Error setting up GNN model: {e}")
            raise
    
    def create_graph_data(self):
        """创建图数据"""
        try:
            if not self.gnn_recommender:
                raise ValueError("GNN model not initialized")
            
            # 获取数据
            users_df = self.processed_data.get("engineered_features", {}).get("users", pd.DataFrame())
            posts_df = self.processed_data.get("engineered_features", {}).get("posts", pd.DataFrame())
            relationships_df = self.processed_data.get("raw_data", {}).get("neo4j", {}).get("relationships", pd.DataFrame())
            
            if users_df.empty or posts_df.empty or relationships_df.empty:
                raise ValueError("Insufficient data for graph creation")
            
            # 创建图数据
            user_features, post_features, adj_matrix = self.gnn_recommender.create_graph_data(
                users_df, posts_df, relationships_df
            )
            
            self.graph_data = {
                "user_features": user_features,
                "post_features": post_features,
                "adj_matrix": adj_matrix,
                "users_df": users_df,
                "posts_df": posts_df,
                "relationships_df": relationships_df
            }
            
            logger.info("Graph data created successfully")
            
        except Exception as e:
            logger.error(f"Error creating graph data: {e}")
            raise
    
    def create_training_data(self):
        """创建训练数据"""
        try:
            if not self.gnn_recommender or not self.graph_data:
                raise ValueError("GNN model or graph data not available")
            
            # 创建训练数据
            user_indices, post_indices, labels = self.gnn_recommender.create_training_data(
                self.graph_data["users_df"],
                self.graph_data["posts_df"],
                self.graph_data["relationships_df"]
            )
            
            self.graph_data["training_data"] = {
                "user_indices": user_indices,
                "post_indices": post_indices,
                "labels": labels
            }
            
            logger.info("Training data created successfully")
            
        except Exception as e:
            logger.error(f"Error creating training data: {e}")
            raise
    
    def train_gnn_model(self, epochs: int = 50):
        """训练GNN模型"""
        try:
            if not self.gnn_recommender or not self.graph_data:
                raise ValueError("GNN model or training data not available")
            
            logger.info("🚀 Starting GNN model training...")
            
            # 训练模型
            training_results = self.gnn_recommender.train(
                user_features=self.graph_data["user_features"],
                post_features=self.graph_data["post_features"],
                adj_matrix=self.graph_data["adj_matrix"],
                user_indices=self.graph_data["training_data"]["user_indices"],
                post_indices=self.graph_data["training_data"]["post_indices"],
                labels=self.graph_data["training_data"]["labels"],
                epochs=epochs,
                batch_size=16  # 减小batch size以适应小数据集
            )
            
            logger.info("✅ GNN model training completed")
            return training_results
            
        except Exception as e:
            logger.error(f"Error training GNN model: {e}")
            raise
    
    def generate_gnn_recommendations(self, user_id: str, n_recommendations: int = 5) -> List[Dict]:
        """使用GNN模型生成推荐"""
        try:
            if not self.gnn_recommender or not self.graph_data:
                raise ValueError("GNN model or graph data not available")
            
            if not self.gnn_recommender.is_trained:
                raise ValueError("GNN model not trained")
            
            # 生成推荐
            recommendations = self.gnn_recommender.recommend_for_user(
                user_id=user_id,
                users_df=self.graph_data["users_df"],
                posts_df=self.graph_data["posts_df"],
                user_features=self.graph_data["user_features"],
                post_features=self.graph_data["post_features"],
                adj_matrix=self.graph_data["adj_matrix"],
                n_recommendations=n_recommendations
            )
            
            logger.info(f"Generated {len(recommendations)} GNN recommendations for user {user_id}")
            return recommendations
            
        except Exception as e:
            logger.error(f"Error generating GNN recommendations: {e}")
            return []
    
    def compare_methods(self, user_id: str) -> Dict[str, Any]:
        """比较不同推荐方法的效果"""
        try:
            # 获取传统方法的推荐结果
            from models.collaborative_filtering import CollaborativeFiltering
            
            # 创建交互矩阵
            interaction_data = []
            relationships_df = self.graph_data["relationships_df"]
            
            for _, rel in relationships_df.iterrows():
                if rel['relationship_type'] in ['POSTED', 'INTERACTED']:
                    source_props = rel['source_props']
                    target_props = rel['target_props']
                    
                    if 'profile_id' in source_props and 'post_id' in target_props:
                        interaction_data.append({
                            'user_id': source_props['profile_id'],
                            'post_id': target_props['post_id'],
                            'interaction': 1
                        })
            
            if not interaction_data:
                logger.warning("No interaction data for traditional methods")
                return {"error": "No interaction data"}
            
            interaction_df = pd.DataFrame(interaction_data)
            
            # 传统方法推荐
            traditional_model = CollaborativeFiltering(method="matrix_factorization")
            traditional_model.fit(interaction_df)
            traditional_recs = traditional_model.recommend_for_user(user_id, n_recommendations=5)
            
            # GNN方法推荐
            gnn_recs = self.generate_gnn_recommendations(user_id, n_recommendations=5)
            
            comparison = {
                "user_id": user_id,
                "traditional_method": {
                    "method": "matrix_factorization",
                    "recommendations": traditional_recs
                },
                "gnn_method": {
                    "method": "SocialGNN",
                    "recommendations": gnn_recs
                },
                "timestamp": datetime.now().isoformat()
            }
            
            logger.info("Method comparison completed")
            return comparison
            
        except Exception as e:
            logger.error(f"Error comparing methods: {e}")
            return {"error": str(e)}
    
    def save_results(self, results: Dict[str, Any], output_dir: str = "./gnn_results"):
        """保存结果"""
        try:
            import os
            os.makedirs(output_dir, exist_ok=True)
            
            # 保存推荐结果
            output_path = f"{output_dir}/gnn_recommendations_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(output_path, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            
            logger.info(f"Results saved to {output_path}")
            
        except Exception as e:
            logger.error(f"Error saving results: {e}")
    
    def close(self):
        """关闭系统"""
        if self.data_pipeline:
            self.data_pipeline.close()


async def main():
    """主程序"""
    try:
        logger.info("🚀 Starting GNN Recommendation System Test...")
        
        # 初始化系统
        gnn_system = GNNRecommendationSystem()
        await gnn_system.initialize()
        
        # 处理数据
        await gnn_system.process_data()
        
        # 设置GNN模型
        gnn_system.setup_gnn_model()
        
        # 创建图数据
        gnn_system.create_graph_data()
        
        # 创建训练数据
        gnn_system.create_training_data()
        
        # 训练GNN模型
        training_results = gnn_system.train_gnn_model(epochs=30)  # 减少训练轮数
        
        print("\n" + "="*50)
        print("📊 GNN TRAINING RESULTS")
        print("="*50)
        print(f"Training completed with {len(training_results['train_losses'])} epochs")
        print(f"Final loss: {training_results['train_losses'][-1]:.4f}")
        print(f"Final AUC: {training_results['train_aucs'][-1]:.4f}")
        
        # 获取模型信息
        model_info = gnn_system.gnn_recommender.get_model_info()
        print(f"Model parameters: {model_info['parameters']:,}")
        print(f"Device: {model_info['device']}")
        
        # 为几个用户生成推荐
        users_df = gnn_system.graph_data["users_df"]
        unique_users = users_df['profile_id'].unique()[:3]
        
        print("\n" + "="*50)
        print("🎯 GNN RECOMMENDATION RESULTS")
        print("="*50)
        
        all_results = {}
        
        for user_id in unique_users:
            print(f"\n👤 User: {user_id}")
            
            # 生成GNN推荐
            gnn_recommendations = gnn_system.generate_gnn_recommendations(user_id, n_recommendations=3)
            
            print(f"   📋 GNN Recommendations:")
            for i, rec in enumerate(gnn_recommendations, 1):
                print(f"      {i}. {rec['post_id']} (Score: {rec['score']:.4f})")
            
            # 比较方法
            comparison = gnn_system.compare_methods(user_id)
            all_results[user_id] = comparison
        
        # 保存结果
        gnn_system.save_results(all_results)
        
        logger.info("🎉 GNN recommendation system test completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ GNN system test failed: {e}")
    finally:
        if 'gnn_system' in locals():
            gnn_system.close()


if __name__ == "__main__":
    asyncio.run(main())
