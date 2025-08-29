"""
Feature Engineering for Social Network Recommendation System
Generates features for users, posts, and interactions
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Tuple
from datetime import datetime
import re
from loguru import logger


class FeatureEngineer:
    """Feature engineering for social network data"""
    
    def __init__(self):
        self.feature_columns = []
        
    def engineer_user_features(self, users_df: pd.DataFrame, posts_df: pd.DataFrame, 
                             interactions_df: pd.DataFrame) -> pd.DataFrame:
        """Engineer features for users"""
        try:
            if users_df.empty:
                logger.warning("No users data for feature engineering")
                return pd.DataFrame()
            
            # Start with user data
            user_features = users_df.copy()
            
            # Basic user features
            user_features['user_id'] = user_features['profile_id']
            
            # Fix time handling
            try:
                user_features['created_at'] = pd.to_datetime(user_features['created_at'])
                user_features['account_age_days'] = (pd.Timestamp.now() - user_features['created_at']).dt.days
            except Exception as e:
                logger.warning(f"Time conversion failed: {e}")
                user_features['account_age_days'] = 0
            
            # Activity features
            user_features['total_posts'] = user_features.get('total_posts', 0)
            user_features['total_interactions'] = user_features.get('total_interactions', 0)
            user_features['activity_score'] = user_features['total_posts'] + user_features['total_interactions']
            
            # Engagement features
            if not posts_df.empty:
                post_counts = posts_df.groupby('account_id').size().reset_index(name='actual_posts')
                user_features = user_features.merge(post_counts, left_on='account_id', right_on='account_id', how='left')
                user_features['actual_posts'] = user_features['actual_posts'].fillna(0)
            else:
                user_features['actual_posts'] = 0
            
            # Interaction features
            if not interactions_df.empty:
                interaction_counts = interactions_df.groupby('account_id').size().reset_index(name='actual_interactions')
                user_features = user_features.merge(interaction_counts, left_on='account_id', right_on='account_id', how='left')
                user_features['actual_interactions'] = user_features['actual_interactions'].fillna(0)
            else:
                user_features['actual_interactions'] = 0
            
            # Engagement rate
            user_features['engagement_rate'] = np.where(
                user_features['actual_posts'] > 0,
                user_features['actual_interactions'] / user_features['actual_posts'],
                0
            )
            
            # Platform features
            user_features['platform_lens'] = (user_features['platform'] == 'lens_chain').astype(int)
            
            # Blockchain features
            user_features['block_number'] = pd.to_numeric(user_features['block_number'], errors='coerce')
            user_features['is_recent_user'] = (user_features['block_number'] > user_features['block_number'].quantile(0.8)).astype(int)
            
            # Normalize numerical features
            numerical_cols = ['total_posts', 'total_interactions', 'actual_posts', 'actual_interactions', 'activity_score']
            for col in numerical_cols:
                if col in user_features.columns:
                    user_features[f'{col}_normalized'] = (user_features[col] - user_features[col].mean()) / (user_features[col].std() + 1e-8)
            
            logger.info(f"Engineered features for {len(user_features)} users")
            return user_features
            
        except Exception as e:
            logger.error(f"Error engineering user features: {e}")
            return pd.DataFrame()
    
    def engineer_post_features(self, posts_df: pd.DataFrame, interactions_df: pd.DataFrame) -> pd.DataFrame:
        """Engineer features for posts"""
        try:
            if posts_df.empty:
                logger.warning("No posts data for feature engineering")
                return pd.DataFrame()
            
            # Start with post data
            post_features = posts_df.copy()
            
            # Basic post features
            post_features['post_id'] = post_features['post_id']
            post_features['author_id'] = post_features['account_id']
            
            # Time features
            try:
                post_features['created_at'] = pd.to_datetime(post_features['created_at'])
                post_features['post_age_hours'] = (pd.Timestamp.now() - post_features['created_at']).dt.total_seconds() / 3600
                post_features['post_age_days'] = post_features['post_age_hours'] / 24
            except Exception as e:
                logger.warning(f"Time conversion failed: {e}")
                post_features['post_age_hours'] = 0
                post_features['post_age_days'] = 0
            
            # Blockchain features
            post_features['block_number'] = pd.to_numeric(post_features['block_number'], errors='coerce')
            post_features['is_recent_post'] = (post_features['block_number'] > post_features['block_number'].quantile(0.8)).astype(int)
            
            # Interaction features
            if not interactions_df.empty:
                interaction_counts = interactions_df.groupby('post_id').size().reset_index(name='interaction_count')
                post_features = post_features.merge(interaction_counts, left_on='post_id', right_on='post_id', how='left')
                post_features['interaction_count'] = post_features['interaction_count'].fillna(0)
            else:
                post_features['interaction_count'] = 0
            
            # Engagement features
            post_features['engagement_score'] = post_features['interaction_count']
            post_features['engagement_rate'] = post_features['interaction_count'] / (post_features['post_age_hours'] + 1)
            
            # Content features (if available)
            if 'content' in post_features.columns:
                post_features['content_length'] = post_features['content'].str.len()
                post_features['has_hashtags'] = post_features['content'].str.contains(r'#\w+').astype(int)
                post_features['has_mentions'] = post_features['content'].str.contains(r'@\w+').astype(int)
            else:
                post_features['content_length'] = 0
                post_features['has_hashtags'] = 0
                post_features['has_mentions'] = 0
            
            # Platform features
            post_features['platform_lens'] = (post_features['platform'] == 'lens_chain').astype(int)
            
            # Normalize numerical features
            numerical_cols = ['interaction_count', 'engagement_score', 'engagement_rate', 'content_length']
            for col in numerical_cols:
                if col in post_features.columns:
                    post_features[f'{col}_normalized'] = (post_features[col] - post_features[col].mean()) / (post_features[col].std() + 1e-8)
            
            logger.info(f"Engineered features for {len(post_features)} posts")
            return post_features
            
        except Exception as e:
            logger.error(f"Error engineering post features: {e}")
            return pd.DataFrame()
    
    def engineer_interaction_features(self, interactions_df: pd.DataFrame, users_df: pd.DataFrame, 
                                   posts_df: pd.DataFrame) -> pd.DataFrame:
        """Engineer features for interactions"""
        try:
            if interactions_df.empty:
                logger.warning("No interactions data for feature engineering")
                return pd.DataFrame()
            
            # Start with interaction data
            interaction_features = interactions_df.copy()
            
            # Basic interaction features
            interaction_features['interaction_id'] = interaction_features['interaction_id']
            interaction_features['user_id'] = interaction_features['account_id']
            interaction_features['post_id'] = interaction_features['post_id']
            
            # Time features
            try:
                interaction_features['created_at'] = pd.to_datetime(interaction_features['created_at'])
                interaction_features['interaction_age_hours'] = (pd.Timestamp.now() - interaction_features['created_at']).dt.total_seconds() / 3600
            except Exception as e:
                logger.warning(f"Time conversion failed: {e}")
                interaction_features['interaction_age_hours'] = 0
            
            # Type features
            if 'type' in interaction_features.columns:
                interaction_features['interaction_type'] = interaction_features['type']
                # One-hot encode interaction types
                type_dummies = pd.get_dummies(interaction_features['interaction_type'], prefix='interaction_type')
                interaction_features = pd.concat([interaction_features, type_dummies], axis=1)
            else:
                interaction_features['interaction_type'] = 'unknown'
            
            # User features (merge with user data)
            if not users_df.empty:
                # Check which columns exist in users_df
                available_cols = []
                for col in ['total_posts', 'total_interactions', 'account_id']:
                    if col in users_df.columns:
                        available_cols.append(col)
                
                if available_cols:
                    user_subset = users_df[available_cols].copy()
                    interaction_features = interaction_features.merge(user_subset, left_on='user_id', right_on='account_id', how='left', suffixes=('', '_user'))
                    
                    # Calculate user activity level based on available columns
                    activity_cols = [col for col in ['total_posts', 'total_interactions'] if col in interaction_features.columns]
                    if activity_cols:
                        interaction_features['user_activity_level'] = interaction_features[activity_cols].sum(axis=1)
                    else:
                        interaction_features['user_activity_level'] = 0
                else:
                    interaction_features['user_activity_level'] = 0
            else:
                interaction_features['user_activity_level'] = 0
            
            # Post features (merge with post data)
            if not posts_df.empty:
                post_subset = posts_df[['post_id', 'interaction_count']].copy()
                interaction_features = interaction_features.merge(post_subset, left_on='post_id', right_on='post_id', how='left', suffixes=('', '_post'))
                interaction_features['post_popularity'] = interaction_features['interaction_count']
            else:
                interaction_features['post_popularity'] = 0
            
            # Engagement features
            interaction_features['engagement_intensity'] = interaction_features['user_activity_level'] * interaction_features['post_popularity']
            
            # Platform features
            interaction_features['platform_lens'] = (interaction_features['platform'] == 'lens_chain').astype(int)
            
            # Normalize numerical features
            numerical_cols = ['user_activity_level', 'post_popularity', 'engagement_intensity']
            for col in numerical_cols:
                if col in interaction_features.columns:
                    interaction_features[f'{col}_normalized'] = (interaction_features[col] - interaction_features[col].mean()) / (interaction_features[col].std() + 1e-8)
            
            logger.info(f"Engineered features for {len(interaction_features)} interactions")
            return interaction_features
            
        except Exception as e:
            logger.error(f"Error engineering interaction features: {e}")
            return pd.DataFrame()
    
    def create_user_post_matrix(self, users_df: pd.DataFrame, posts_df: pd.DataFrame, 
                               interactions_df: pd.DataFrame) -> pd.DataFrame:
        """Create user-post interaction matrix"""
        try:
            if users_df.empty or posts_df.empty:
                logger.warning("Insufficient data for user-post matrix")
                return pd.DataFrame()
            
            # Create user-post matrix
            user_ids = users_df['profile_id'].unique()
            post_ids = posts_df['post_id'].unique()
            
            # Initialize matrix
            matrix_data = []
            for user_id in user_ids:
                for post_id in post_ids:
                    # Check if interaction exists
                    interaction = interactions_df[
                        (interactions_df['account_id'] == users_df[users_df['profile_id'] == user_id]['account_id'].iloc[0]) &
                        (interactions_df['post_id'] == post_id)
                    ]
                    
                    interaction_value = 1 if not interaction.empty else 0
                    
                    matrix_data.append({
                        'user_id': user_id,
                        'post_id': post_id,
                        'interaction': interaction_value
                    })
            
            matrix_df = pd.DataFrame(matrix_data)
            logger.info(f"Created user-post matrix: {len(user_ids)} users x {len(post_ids)} posts")
            return matrix_df
            
        except Exception as e:
            logger.error(f"Error creating user-post matrix: {e}")
            return pd.DataFrame()
    
    def get_feature_summary(self, user_features: pd.DataFrame, post_features: pd.DataFrame, 
                           interaction_features: pd.DataFrame) -> Dict[str, Any]:
        """Get summary of engineered features"""
        try:
            summary = {
                "user_features": {
                    "total_users": len(user_features),
                    "feature_columns": list(user_features.columns) if not user_features.empty else [],
                    "numerical_features": len([col for col in user_features.columns if user_features[col].dtype in ['int64', 'float64']]) if not user_features.empty else 0
                },
                "post_features": {
                    "total_posts": len(post_features),
                    "feature_columns": list(post_features.columns) if not post_features.empty else [],
                    "numerical_features": len([col for col in post_features.columns if post_features[col].dtype in ['int64', 'float64']]) if not post_features.empty else 0
                },
                "interaction_features": {
                    "total_interactions": len(interaction_features),
                    "feature_columns": list(interaction_features.columns) if not interaction_features.empty else [],
                    "numerical_features": len([col for col in interaction_features.columns if interaction_features[col].dtype in ['int64', 'float64']]) if not interaction_features.empty else 0
                }
            }
            
            logger.info("Feature summary generated successfully")
            return summary
            
        except Exception as e:
            logger.error(f"Error generating feature summary: {e}")
            return {}


def main():
    """Test feature engineering"""
    # This would be used with actual data
    print("Feature engineering module ready for use")


if __name__ == "__main__":
    main()
