"""
Recommendation System Main Program
整合数据处理和推荐模型
"""

import asyncio
import pandas as pd
import numpy as np
from typing import Dict, List, Any
import json
from datetime import datetime
from loguru import logger

# 导入数据处理模块
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data_processing.process_pipeline import DataProcessingPipeline
from models.collaborative_filtering import CollaborativeFiltering


class RecommendationSystem:
    """推荐系统主类"""
    
    def __init__(self):
        self.data_pipeline = DataProcessingPipeline()
        self.models = {}
        self.processed_data = None
        
    async def initialize(self):
        """初始化推荐系统"""
        try:
            await self.data_pipeline.initialize()
            logger.info("Recommendation system initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize recommendation system: {e}")
            raise
    
    async def process_data(self):
        """处理数据"""
        try:
            logger.info("🔄 Processing data for recommendation system...")
            
            # 运行数据处理管道
            self.processed_data = await self.data_pipeline.run_full_pipeline()
            
            logger.info("✅ Data processing completed")
            return self.processed_data
            
        except Exception as e:
            logger.error(f"Error processing data: {e}")
            raise
    
    def create_interaction_matrix(self) -> pd.DataFrame:
        """创建用户-帖子交互矩阵"""
        try:
            if not self.processed_data:
                raise ValueError("No processed data available")
            
            # 从Neo4j关系数据创建交互矩阵
            relationships = self.processed_data.get("raw_data", {}).get("neo4j", {}).get("relationships", pd.DataFrame())
            
            if relationships.empty:
                logger.warning("No relationship data available")
                return pd.DataFrame()
            
            # 创建交互数据
            interaction_data = []
            
            # 处理POSTED关系
            posted_rels = relationships[relationships['relationship_type'] == 'POSTED']
            for _, rel in posted_rels.iterrows():
                source_props = rel['source_props']
                target_props = rel['target_props']
                
                if 'profile_id' in source_props and 'post_id' in target_props:
                    interaction_data.append({
                        'user_id': source_props['profile_id'],
                        'post_id': target_props['post_id'],
                        'interaction': 1,  # 发帖关系
                        'type': 'POSTED'
                    })
            
            # 处理INTERACTED关系
            interacted_rels = relationships[relationships['relationship_type'] == 'INTERACTED']
            for _, rel in interacted_rels.iterrows():
                source_props = rel['source_props']
                target_props = rel['target_props']
                
                if 'profile_id' in source_props and 'post_id' in target_props:
                    interaction_data.append({
                        'user_id': source_props['profile_id'],
                        'post_id': target_props['post_id'],
                        'interaction': 1,  # 互动关系
                        'type': 'INTERACTED'
                    })
            
            if not interaction_data:
                logger.warning("No interaction data created")
                return pd.DataFrame()
            
            # 创建DataFrame
            interaction_df = pd.DataFrame(interaction_data)
            
            # 去重并聚合
            interaction_df = interaction_df.groupby(['user_id', 'post_id']).agg({
                'interaction': 'sum',
                'type': lambda x: ','.join(x.unique())
            }).reset_index()
            
            logger.info(f"Created interaction matrix with {len(interaction_df)} interactions")
            return interaction_df
            
        except Exception as e:
            logger.error(f"Error creating interaction matrix: {e}")
            return pd.DataFrame()
    
    def train_models(self, interaction_matrix: pd.DataFrame):
        """训练推荐模型"""
        try:
            if interaction_matrix.empty:
                logger.warning("No interaction data for training models")
                return
            
            logger.info("🚀 Training recommendation models...")
            
            # 训练基于用户的协同过滤模型
            user_based_model = CollaborativeFiltering(method="user_based")
            user_based_model.fit(interaction_matrix)
            self.models["user_based"] = user_based_model
            
            # 训练基于物品的协同过滤模型
            item_based_model = CollaborativeFiltering(method="item_based")
            item_based_model.fit(interaction_matrix)
            self.models["item_based"] = item_based_model
            
            # 训练矩阵分解模型
            matrix_factorization_model = CollaborativeFiltering(method="matrix_factorization")
            matrix_factorization_model.fit(interaction_matrix)
            self.models["matrix_factorization"] = matrix_factorization_model
            
            logger.info(f"✅ Trained {len(self.models)} models successfully")
            
        except Exception as e:
            logger.error(f"Error training models: {e}")
            raise
    
    def get_recommendations(self, user_id: str, method: str = "user_based", n_recommendations: int = 5) -> List[Dict]:
        """获取推荐结果"""
        try:
            if method not in self.models:
                raise ValueError(f"Model {method} not trained")
            
            model = self.models[method]
            recommendations = model.recommend_for_user(user_id, n_recommendations)
            
            logger.info(f"Generated {len(recommendations)} recommendations for user {user_id} using {method}")
            return recommendations
            
        except Exception as e:
            logger.error(f"Error getting recommendations: {e}")
            return []
    
    def evaluate_models(self) -> Dict[str, Any]:
        """评估模型性能"""
        try:
            if not self.models:
                return {"error": "No models trained"}
            
            evaluation_results = {}
            
            for method, model in self.models.items():
                model_info = model.get_model_info()
                evaluation_results[method] = model_info
            
            logger.info("Model evaluation completed")
            return evaluation_results
            
        except Exception as e:
            logger.error(f"Error evaluating models: {e}")
            return {"error": str(e)}
    
    def save_recommendations(self, user_id: str, recommendations: List[Dict], output_dir: str = "./recommendations"):
        """保存推荐结果"""
        try:
            import os
            os.makedirs(output_dir, exist_ok=True)
            
            # 创建推荐结果
            result = {
                "user_id": user_id,
                "timestamp": datetime.now().isoformat(),
                "recommendations": recommendations,
                "total_recommendations": len(recommendations)
            }
            
            # 保存为JSON文件
            output_path = f"{output_dir}/recommendations_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(output_path, 'w') as f:
                json.dump(result, f, indent=2, default=str)
            
            logger.info(f"Saved recommendations to {output_path}")
            
        except Exception as e:
            logger.error(f"Error saving recommendations: {e}")
    
    def close(self):
        """关闭推荐系统"""
        if self.data_pipeline:
            self.data_pipeline.close()


async def main():
    """主程序"""
    try:
        logger.info("🚀 Starting Recommendation System...")
        
        # 初始化推荐系统
        rec_system = RecommendationSystem()
        await rec_system.initialize()
        
        # 处理数据
        await rec_system.process_data()
        
        # 创建交互矩阵
        interaction_matrix = rec_system.create_interaction_matrix()
        
        if interaction_matrix.empty:
            logger.error("No interaction data available for training models")
            return
        
        # 训练模型
        rec_system.train_models(interaction_matrix)
        
        # 评估模型
        evaluation = rec_system.evaluate_models()
        print("\n" + "="*50)
        print("📊 MODEL EVALUATION RESULTS")
        print("="*50)
        for method, info in evaluation.items():
            print(f"🔧 {method.upper()}:")
            for key, value in info.items():
                print(f"   {key}: {value}")
        
        # 为几个用户生成推荐
        unique_users = interaction_matrix['user_id'].unique()[:3]  # 取前3个用户
        
        print("\n" + "="*50)
        print("🎯 RECOMMENDATION RESULTS")
        print("="*50)
        
        for user_id in unique_users:
            print(f"\n👤 User: {user_id}")
            
            # 使用不同方法生成推荐
            for method in ["user_based", "item_based", "matrix_factorization"]:
                recommendations = rec_system.get_recommendations(user_id, method, n_recommendations=3)
                
                print(f"   📋 {method.upper()} Recommendations:")
                for i, rec in enumerate(recommendations, 1):
                    print(f"      {i}. {rec['post_id']} (Score: {rec['score']:.4f})")
                
                # 保存推荐结果
                rec_system.save_recommendations(user_id, recommendations)
        
        logger.info("🎉 Recommendation system completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Recommendation system failed: {e}")
    finally:
        if 'rec_system' in locals():
            rec_system.close()


if __name__ == "__main__":
    asyncio.run(main())
