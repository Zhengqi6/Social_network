"""
Graph Neural Network Recommendation Model
基于图神经网络的社交网络推荐算法
"""

import torch
import torch.nn as nn
import torch.nn.functional as F
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional, Any
import logging
from sklearn.metrics import roc_auc_score, ndcg_score
import networkx as nx

logger = logging.getLogger(__name__)


class GraphConvolution(nn.Module):
    """图卷积层"""
    
    def __init__(self, in_features: int, out_features: int, bias: bool = True):
        super(GraphConvolution, self).__init__()
        self.in_features = in_features
        self.out_features = out_features
        
        # 权重矩阵
        self.weight = nn.Parameter(torch.FloatTensor(in_features, out_features))
        
        # 偏置项
        if bias:
            self.bias = nn.Parameter(torch.FloatTensor(out_features))
        else:
            self.register_parameter('bias', None)
        
        self.reset_parameters()
    
    def reset_parameters(self):
        """初始化参数"""
        nn.init.kaiming_uniform_(self.weight)
        if self.bias is not None:
            nn.init.zeros_(self.bias)
    
    def forward(self, input_features: torch.Tensor, adj_matrix: torch.Tensor) -> torch.Tensor:
        """
        前向传播
        
        Args:
            input_features: 输入特征 [N, in_features]
            adj_matrix: 邻接矩阵 [N, N]
            
        Returns:
            输出特征 [N, out_features]
        """
        # 图卷积: H = σ(D^(-1/2) * A * D^(-1/2) * X * W)
        support = torch.mm(input_features, self.weight)
        output = torch.spmm(adj_matrix, support)
        
        if self.bias is not None:
            output += self.bias
        
        return output


class SocialGNN(nn.Module):
    """社交网络图神经网络"""
    
    def __init__(self, 
                 user_features: int,
                 post_features: int,
                 hidden_dim: int = 64,
                 num_layers: int = 2,
                 dropout: float = 0.3):
        super(SocialGNN, self).__init__()
        
        self.user_features = user_features
        self.post_features = post_features
        self.hidden_dim = hidden_dim
        self.num_layers = num_layers
        self.dropout = dropout
        
        # 用户特征投影层
        self.user_projection = nn.Linear(user_features, hidden_dim)
        
        # 帖子特征投影层
        self.post_projection = nn.Linear(post_features, hidden_dim)
        
        # 图卷积层
        self.gnn_layers = nn.ModuleList()
        for i in range(num_layers):
            if i == 0:
                self.gnn_layers.append(GraphConvolution(hidden_dim, hidden_dim))
            else:
                self.gnn_layers.append(GraphConvolution(hidden_dim, hidden_dim))
        
        # 推荐预测层
        self.recommendation_head = nn.Sequential(
            nn.Linear(hidden_dim * 2, hidden_dim),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim, hidden_dim // 2),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim // 2, 1),
            nn.Sigmoid()
        )
        
        # 初始化参数
        self.reset_parameters()
    
    def reset_parameters(self):
        """初始化模型参数"""
        for layer in self.gnn_layers:
            layer.reset_parameters()
        
        for layer in self.recommendation_head:
            if hasattr(layer, 'weight'):
                nn.init.xavier_uniform_(layer.weight)
                if hasattr(layer, 'bias') and layer.bias is not None:
                    nn.init.zeros_(layer.bias)
    
    def forward(self, 
                user_features: torch.Tensor,
                post_features: torch.Tensor,
                adj_matrix: torch.Tensor,
                user_indices: torch.Tensor,
                post_indices: torch.Tensor) -> torch.Tensor:
        """
        前向传播
        
        Args:
            user_features: 用户特征 [num_users, user_features]
            post_features: 帖子特征 [num_posts, post_features]
            adj_matrix: 邻接矩阵 [num_nodes, num_nodes]
            user_indices: 用户索引 [batch_size]
            post_indices: 帖子索引 [batch_size]
            
        Returns:
            推荐分数 [batch_size]
        """
        # 特征投影
        user_embeddings = self.user_projection(user_features)
        post_embeddings = self.post_projection(post_features)
        
        # 合并所有节点特征
        all_features = torch.cat([user_embeddings, post_embeddings], dim=0)
        
        # 图卷积传播
        hidden_features = all_features
        for gnn_layer in self.gnn_layers:
            hidden_features = F.relu(gnn_layer(hidden_features, adj_matrix))
            hidden_features = F.dropout(hidden_features, self.dropout, training=self.training)
        
        # 分离用户和帖子嵌入
        user_embeddings_final = hidden_features[:user_features.size(0)]
        post_embeddings_final = hidden_features[user_features.size(0):]
        
        # 获取batch中的用户和帖子嵌入
        batch_user_embeddings = user_embeddings_final[user_indices]
        batch_post_embeddings = post_embeddings_final[post_indices]
        
        # 拼接用户和帖子特征
        combined_features = torch.cat([batch_user_embeddings, batch_post_embeddings], dim=1)
        
        # 预测推荐分数
        recommendation_scores = self.recommendation_head(combined_features).squeeze()
        
        return recommendation_scores


class GNNRecommender:
    """GNN推荐器"""
    
    def __init__(self, 
                 user_features: int,
                 post_features: int,
                 hidden_dim: int = 64,
                 num_layers: int = 2,
                 learning_rate: float = 0.001,
                 device: str = 'cpu'):
        self.device = device
        self.model = SocialGNN(
            user_features=user_features,
            post_features=post_features,
            hidden_dim=hidden_dim,
            num_layers=num_layers
        ).to(device)
        
        self.optimizer = torch.optim.Adam(self.model.parameters(), lr=learning_rate)
        self.criterion = nn.BCELoss()
        self.is_trained = False
        
        logger.info(f"GNN Recommender initialized with {hidden_dim} hidden dim, {num_layers} layers")
    
    def create_graph_data(self, 
                         users_df: pd.DataFrame,
                         posts_df: pd.DataFrame,
                         relationships_df: pd.DataFrame) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        """
        创建图数据
        
        Returns:
            user_features: 用户特征
            post_features: 帖子特征  
            adj_matrix: 邻接矩阵
        """
        try:
            # 创建用户特征矩阵
            user_feature_cols = [col for col in users_df.columns if col.endswith('_normalized') or col in ['platform_lens', 'is_recent_user']]
            if not user_feature_cols:
                user_feature_cols = ['platform_lens', 'is_recent_user']
            
            user_features = users_df[user_feature_cols].fillna(0).values
            user_features = torch.FloatTensor(user_features).to(self.device)
            
            # 创建帖子特征矩阵
            post_feature_cols = [col for col in posts_df.columns if col.endswith('_normalized') or col in ['platform_lens', 'is_recent_post']]
            if not post_feature_cols:
                post_feature_cols = ['platform_lens', 'is_recent_post']
            
            post_features = posts_df[post_feature_cols].fillna(0).values
            post_features = torch.FloatTensor(post_features).to(self.device)
            
            # 创建邻接矩阵
            num_users = len(users_df)
            num_posts = len(posts_df)
            num_nodes = num_users + num_posts
            
            adj_matrix = torch.zeros(num_nodes, num_nodes).to(self.device)
            
            # 添加用户-用户关系 (FOLLOWS)
            follows_rels = relationships_df[relationships_df['relationship_type'] == 'FOLLOWS']
            for _, rel in follows_rels.iterrows():
                source_props = rel['source_props']
                target_props = rel['target_props']
                
                if 'profile_id' in source_props and 'profile_id' in target_props:
                    source_idx = users_df[users_df['profile_id'] == source_props['profile_id']].index[0]
                    target_idx = users_df[users_df['profile_id'] == target_props['profile_id']].index[0]
                    
                    adj_matrix[source_idx, target_idx] = 1
                    adj_matrix[target_idx, source_idx] = 1  # 无向图
            
            # 添加用户-帖子关系 (POSTED, INTERACTED)
            for _, rel in relationships_df.iterrows():
                if rel['relationship_type'] in ['POSTED', 'INTERACTED']:
                    source_props = rel['source_props']
                    target_props = rel['target_props']
                    
                    if 'profile_id' in source_props and 'post_id' in target_props:
                        source_idx = users_df[users_df['profile_id'] == source_props['profile_id']].index[0]
                        target_idx = posts_df[posts_df['post_id'] == target_props['post_id']].index[0] + num_users
                        
                        adj_matrix[source_idx, target_idx] = 1
                        adj_matrix[target_idx, source_idx] = 1
            
            # 添加自环
            adj_matrix += torch.eye(num_nodes).to(self.device)
            
            # 对称归一化
            degree = adj_matrix.sum(dim=1)
            degree_matrix = torch.diag(torch.pow(degree, -0.5))
            adj_matrix = torch.mm(torch.mm(degree_matrix, adj_matrix), degree_matrix)
            
            logger.info(f"Created graph with {num_users} users, {num_posts} posts, {num_nodes} total nodes")
            return user_features, post_features, adj_matrix
            
        except Exception as e:
            logger.error(f"Error creating graph data: {e}")
            raise
    
    def create_training_data(self, 
                            users_df: pd.DataFrame,
                            posts_df: pd.DataFrame,
                            relationships_df: pd.DataFrame) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        """
        创建训练数据
        
        Returns:
            user_indices: 用户索引
            post_indices: 帖子索引
            labels: 标签 (1表示有交互，0表示无交互)
        """
        try:
            num_users = len(users_df)
            num_posts = len(posts_df)
            
            # 创建正样本 (有交互的用户-帖子对)
            positive_pairs = []
            for _, rel in relationships_df.iterrows():
                if rel['relationship_type'] in ['POSTED', 'INTERACTED']:
                    source_props = rel['source_props']
                    target_props = rel['target_props']
                    
                    if 'profile_id' in source_props and 'post_id' in target_props:
                        user_idx = users_df[users_df['profile_id'] == source_props['profile_id']].index[0]
                        post_idx = posts_df[posts_df['post_id'] == target_props['post_id']].index[0]
                        positive_pairs.append((user_idx, post_idx))
            
            # 创建负样本 (随机采样无交互的用户-帖子对)
            negative_pairs = []
            num_negative = len(positive_pairs) * 3  # 负样本数量是正样本的3倍
            
            for _ in range(num_negative):
                user_idx = np.random.randint(0, num_users)
                post_idx = np.random.randint(0, num_posts)
                
                # 确保不是正样本
                if (user_idx, post_idx) not in positive_pairs:
                    negative_pairs.append((user_idx, post_idx))
            
            # 合并正负样本
            all_pairs = positive_pairs + negative_pairs
            labels = [1] * len(positive_pairs) + [0] * len(negative_pairs)
            
            # 转换为tensor
            user_indices = torch.LongTensor([pair[0] for pair in all_pairs]).to(self.device)
            post_indices = torch.LongTensor([pair[1] for pair in all_pairs]).to(self.device)
            labels = torch.FloatTensor(labels).to(self.device)
            
            logger.info(f"Created training data: {len(positive_pairs)} positive, {len(negative_pairs)} negative samples")
            return user_indices, post_indices, labels
            
        except Exception as e:
            logger.error(f"Error creating training data: {e}")
            raise
    
    def train(self, 
              user_features: torch.Tensor,
              post_features: torch.Tensor,
              adj_matrix: torch.Tensor,
              user_indices: torch.Tensor,
              post_indices: torch.Tensor,
              labels: torch.Tensor,
              epochs: int = 100,
              batch_size: int = 32) -> Dict[str, List[float]]:
        """
        训练模型
        """
        try:
            logger.info("Starting GNN training...")
            
            num_samples = len(labels)
            num_batches = (num_samples + batch_size - 1) // batch_size
            
            train_losses = []
            train_aucs = []
            
            self.model.train()
            
            for epoch in range(epochs):
                epoch_loss = 0.0
                epoch_predictions = []
                epoch_labels = []
                
                # 随机打乱数据
                indices = torch.randperm(num_samples)
                
                for batch_idx in range(num_batches):
                    start_idx = batch_idx * batch_size
                    end_idx = min(start_idx + batch_size, num_samples)
                    
                    batch_indices = indices[start_idx:end_idx]
                    
                    batch_user_indices = user_indices[batch_indices]
                    batch_post_indices = post_indices[batch_indices]
                    batch_labels = labels[batch_indices]
                    
                    # 前向传播
                    predictions = self.model(
                        user_features, post_features, adj_matrix,
                        batch_user_indices, batch_post_indices
                    )
                    
                    # 计算损失
                    loss = self.criterion(predictions, batch_labels)
                    
                    # 反向传播
                    self.optimizer.zero_grad()
                    loss.backward()
                    self.optimizer.step()
                    
                    epoch_loss += loss.item()
                    epoch_predictions.extend(predictions.detach().cpu().numpy())
                    epoch_labels.extend(batch_labels.cpu().numpy())
                
                # 计算epoch指标
                avg_loss = epoch_loss / num_batches
                auc = roc_auc_score(epoch_labels, epoch_predictions)
                
                train_losses.append(avg_loss)
                train_aucs.append(auc)
                
                if (epoch + 1) % 20 == 0:
                    logger.info(f"Epoch {epoch+1}/{epochs}, Loss: {avg_loss:.4f}, AUC: {auc:.4f}")
            
            self.is_trained = True
            logger.info("GNN training completed!")
            
            return {
                "train_losses": train_losses,
                "train_aucs": train_aucs
            }
            
        except Exception as e:
            logger.error(f"Error during training: {e}")
            raise
    
    def recommend_for_user(self, 
                          user_id: str,
                          users_df: pd.DataFrame,
                          posts_df: pd.DataFrame,
                          user_features: torch.Tensor,
                          post_features: torch.Tensor,
                          adj_matrix: torch.Tensor,
                          n_recommendations: int = 5) -> List[Dict]:
        """
        为用户生成推荐
        """
        try:
            if not self.is_trained:
                raise ValueError("Model must be trained before making recommendations")
            
            # 找到用户索引
            user_mask = users_df['profile_id'] == user_id
            if not user_mask.any():
                logger.warning(f"User {user_id} not found in training data")
                return []
            
            user_idx = users_df[user_mask].index[0]
            
            # 找到用户已经交互过的帖子
            user_interacted_posts = set()
            # 这里需要根据实际数据找到用户交互过的帖子
            
            # 为所有帖子计算推荐分数
            self.model.eval()
            with torch.no_grad():
                user_indices = torch.full((len(posts_df),), user_idx).to(self.device)
                post_indices = torch.arange(len(posts_df)).to(self.device)
                
                scores = self.model(
                    user_features, post_features, adj_matrix,
                    user_indices, post_indices
                )
                
                # 排除已交互的帖子
                for post_idx in user_interacted_posts:
                    scores[post_idx] = -1.0
                
                # 获取top-N推荐
                top_indices = torch.topk(scores, n_recommendations).indices
                
                recommendations = []
                for idx in top_indices:
                    idx_int = int(idx.item())  # 转换为Python整数
                    post_id = posts_df.iloc[idx_int]['post_id']
                    score = float(scores[idx])
                    recommendations.append({
                        "post_id": post_id,
                        "score": score
                    })
            
            logger.info(f"Generated {len(recommendations)} recommendations for user {user_id}")
            return recommendations
            
        except Exception as e:
            logger.error(f"Error generating recommendations: {e}")
            return []
    
    def get_model_info(self) -> Dict[str, Any]:
        """获取模型信息"""
        info = {
            "model_type": "SocialGNN",
            "is_trained": self.is_trained,
            "device": self.device,
            "parameters": sum(p.numel() for p in self.model.parameters())
        }
        
        if self.is_trained:
            info["status"] = "trained"
        else:
            info["status"] = "not_trained"
        
        return info


def main():
    """测试GNN推荐模型"""
    print("GNN recommendation model ready for use")


if __name__ == "__main__":
    main()
