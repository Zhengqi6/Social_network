"""
Collaborative Filtering Recommendation Model
基于协同过滤的推荐算法
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.decomposition import NMF
import logging

logger = logging.getLogger(__name__)


class CollaborativeFiltering:
    """协同过滤推荐模型"""
    
    def __init__(self, method: str = "user_based"):
        """
        初始化协同过滤模型
        
        Args:
            method: 推荐方法 ("user_based", "item_based", "matrix_factorization")
        """
        self.method = method
        self.user_similarity_matrix = None
        self.item_similarity_matrix = None
        self.user_item_matrix = None
        self.nmf_model = None
        self.is_fitted = False
        
    def fit(self, user_item_matrix: pd.DataFrame) -> 'CollaborativeFiltering':
        """
        训练模型
        
        Args:
            user_item_matrix: 用户-物品交互矩阵，包含列: user_id, post_id, interaction
            
        Returns:
            self: 训练后的模型
        """
        try:
            logger.info(f"Training {self.method} collaborative filtering model...")
            
            # 创建用户-物品交互矩阵
            self.user_item_matrix = self._create_interaction_matrix(user_item_matrix)
            
            if self.method == "user_based":
                self._fit_user_based()
            elif self.method == "item_based":
                self._fit_item_based()
            elif self.method == "matrix_factorization":
                self._fit_matrix_factorization()
            else:
                raise ValueError(f"Unknown method: {self.method}")
            
            self.is_fitted = True
            logger.info(f"Model training completed. Matrix shape: {self.user_item_matrix.shape}")
            return self
            
        except Exception as e:
            logger.error(f"Error training model: {e}")
            raise
    
    def _create_interaction_matrix(self, user_item_matrix: pd.DataFrame) -> pd.DataFrame:
        """创建用户-物品交互矩阵"""
        try:
            # 透视表创建交互矩阵
            matrix = user_item_matrix.pivot_table(
                index='user_id', 
                columns='post_id', 
                values='interaction',
                fill_value=0
            )
            
            logger.info(f"Created interaction matrix: {matrix.shape}")
            return matrix
            
        except Exception as e:
            logger.error(f"Error creating interaction matrix: {e}")
            raise
    
    def _fit_user_based(self):
        """训练基于用户的协同过滤"""
        try:
            # 计算用户相似度矩阵
            self.user_similarity_matrix = cosine_similarity(self.user_item_matrix)
            logger.info("User similarity matrix computed")
            
        except Exception as e:
            logger.error(f"Error in user-based fitting: {e}")
            raise
    
    def _fit_item_based(self):
        """训练基于物品的协同过滤"""
        try:
            # 计算物品相似度矩阵
            self.item_similarity_matrix = cosine_similarity(self.user_item_matrix.T)
            logger.info("Item similarity matrix computed")
            
        except Exception as e:
            logger.error(f"Error in item-based fitting: {e}")
            raise
    
    def _fit_matrix_factorization(self):
        """训练矩阵分解模型"""
        try:
            # 使用NMF进行矩阵分解
            n_components = min(self.user_item_matrix.shape) // 4  # 自动选择组件数
            n_components = max(n_components, 2)  # 至少2个组件
            
            self.nmf_model = NMF(n_components=n_components, random_state=42)
            self.nmf_model.fit(self.user_item_matrix)
            
            logger.info(f"Matrix factorization completed with {n_components} components")
            
        except Exception as e:
            logger.error(f"Error in matrix factorization: {e}")
            raise
    
    def recommend_for_user(self, user_id: str, n_recommendations: int = 5) -> List[Dict]:
        """
        为用户推荐帖子
        
        Args:
            user_id: 用户ID
            n_recommendations: 推荐数量
            
        Returns:
            List[Dict]: 推荐列表，包含post_id和score
        """
        if not self.is_fitted:
            raise ValueError("Model must be fitted before making recommendations")
        
        try:
            if self.method == "user_based":
                return self._user_based_recommend(user_id, n_recommendations)
            elif self.method == "item_based":
                return self._item_based_recommend(user_id, n_recommendations)
            elif self.method == "matrix_factorization":
                return self._matrix_factorization_recommend(user_id, n_recommendations)
            else:
                raise ValueError(f"Unknown method: {self.method}")
                
        except Exception as e:
            logger.error(f"Error making recommendations for user {user_id}: {e}")
            return []
    
    def _user_based_recommend(self, user_id: str, n_recommendations: int) -> List[Dict]:
        """基于用户的推荐"""
        try:
            if user_id not in self.user_item_matrix.index:
                logger.warning(f"User {user_id} not found in training data")
                return []
            
            # 获取用户索引
            user_idx = self.user_item_matrix.index.get_loc(user_id)
            
            # 获取用户相似度
            user_similarities = self.user_similarity_matrix[user_idx]
            
            # 找到最相似的用户
            similar_users = np.argsort(user_similarities)[::-1][1:6]  # 排除自己，取前5个
            
            # 获取相似用户喜欢的帖子
            recommendations = {}
            user_interactions = self.user_item_matrix.iloc[user_idx]
            
            for similar_user_idx in similar_users:
                similar_user_id = self.user_item_matrix.index[similar_user_idx]
                similar_user_interactions = self.user_item_matrix.iloc[similar_user_idx]
                
                # 找到相似用户喜欢但当前用户未交互的帖子
                for post_id in similar_user_interactions.index:
                    if (similar_user_interactions[post_id] > 0 and 
                        user_interactions[post_id] == 0):
                        
                        if post_id not in recommendations:
                            recommendations[post_id] = 0
                        
                        # 加权分数
                        recommendations[post_id] += (
                            user_similarities[similar_user_idx] * 
                            similar_user_interactions[post_id]
                        )
            
            # 排序并返回推荐
            sorted_recommendations = sorted(
                recommendations.items(), 
                key=lambda x: x[1], 
                reverse=True
            )[:n_recommendations]
            
            return [
                {"post_id": post_id, "score": float(score)}
                for post_id, score in sorted_recommendations
            ]
            
        except Exception as e:
            logger.error(f"Error in user-based recommendation: {e}")
            return []
    
    def _item_based_recommend(self, user_id: str, n_recommendations: int) -> List[Dict]:
        """基于物品的推荐"""
        try:
            if user_id not in self.user_item_matrix.index:
                logger.warning(f"User {user_id} not found in training data")
                return []
            
            # 获取用户交互的帖子
            user_interactions = self.user_item_matrix.loc[user_id]
            user_liked_posts = user_interactions[user_interactions > 0].index
            
            if len(user_liked_posts) == 0:
                logger.warning(f"User {user_id} has no interactions")
                return []
            
            # 计算推荐分数
            recommendations = {}
            
            for post_id in self.user_item_matrix.columns:
                if post_id in user_liked_posts:
                    continue  # 跳过用户已经交互的帖子
                
                score = 0
                for liked_post in user_liked_posts:
                    # 获取帖子相似度
                    post_idx = self.user_item_matrix.columns.get_loc(post_id)
                    liked_post_idx = self.user_item_matrix.columns.get_loc(liked_post)
                    
                    similarity = self.item_similarity_matrix[post_idx, liked_post_idx]
                    score += similarity * user_interactions[liked_post]
                
                if score > 0:
                    recommendations[post_id] = score
            
            # 排序并返回推荐
            sorted_recommendations = sorted(
                recommendations.items(), 
                key=lambda x: x[1], 
                reverse=True
            )[:n_recommendations]
            
            return [
                {"post_id": post_id, "score": float(score)}
                for post_id, score in sorted_recommendations
            ]
            
        except Exception as e:
            logger.error(f"Error in item-based recommendation: {e}")
            return []
    
    def _matrix_factorization_recommend(self, user_id: str, n_recommendations: int) -> List[Dict]:
        """基于矩阵分解的推荐"""
        try:
            if user_id not in self.user_item_matrix.index:
                logger.warning(f"User {user_id} not found in training data")
                return []
            
            # 获取用户索引
            user_idx = self.user_item_matrix.index.get_loc(user_id)
            
            # 使用训练好的模型预测用户对所有帖子的评分
            user_vector = self.user_item_matrix.iloc[user_idx:user_idx+1]
            predicted_ratings = self.nmf_model.inverse_transform(
                self.nmf_model.transform(user_vector)
            )[0]
            
            # 获取用户已交互的帖子
            user_interactions = self.user_item_matrix.iloc[user_idx]
            interacted_posts = set(user_interactions[user_interactions > 0].index)
            
            # 创建推荐列表
            recommendations = []
            for post_idx, post_id in enumerate(self.user_item_matrix.columns):
                if post_id not in interacted_posts:
                    recommendations.append({
                        "post_id": post_id,
                        "score": float(predicted_ratings[post_idx])
                    })
            
            # 排序并返回推荐
            recommendations.sort(key=lambda x: x["score"], reverse=True)
            return recommendations[:n_recommendations]
            
        except Exception as e:
            logger.error(f"Error in matrix factorization recommendation: {e}")
            return []
    
    def get_model_info(self) -> Dict:
        """获取模型信息"""
        if not self.is_fitted:
            return {"status": "not_fitted"}
        
        info = {
            "method": self.method,
            "status": "fitted",
            "matrix_shape": self.user_item_matrix.shape if self.user_item_matrix is not None else None,
            "n_users": len(self.user_item_matrix.index) if self.user_item_matrix is not None else 0,
            "n_items": len(self.user_item_matrix.columns) if self.user_item_matrix is not None else 0,
        }
        
        if self.method == "matrix_factorization" and self.nmf_model:
            info["n_components"] = self.nmf_model.n_components_
        
        return info


def main():
    """测试协同过滤模型"""
    print("Collaborative filtering model ready for use")


if __name__ == "__main__":
    main()
