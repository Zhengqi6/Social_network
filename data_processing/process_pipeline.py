"""
Data Processing Pipeline for Social Network Recommendation System
Main pipeline that orchestrates data loading and feature engineering
"""

import asyncio
import pandas as pd
from typing import Dict, List, Any, Tuple
import json
from datetime import datetime
from loguru import logger

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data_loader import DataLoader
from feature_engineering import FeatureEngineer


class DataProcessingPipeline:
    """Main data processing pipeline"""
    
    def __init__(self):
        self.data_loader = DataLoader()
        self.feature_engineer = FeatureEngineer()
        self.processed_data = {}
        
    async def initialize(self):
        """Initialize the pipeline"""
        try:
            await self.data_loader.initialize()
            logger.info("Data processing pipeline initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize pipeline: {e}")
            raise
    
    async def run_full_pipeline(self) -> Dict[str, Any]:
        """Run the complete data processing pipeline"""
        try:
            logger.info("🚀 Starting data processing pipeline...")
            
            # Step 1: Load raw data
            logger.info("📥 Step 1: Loading raw data...")
            raw_data = await self._load_raw_data()
            
            # Step 2: Engineer features
            logger.info("🔧 Step 2: Engineering features...")
            engineered_features = await self._engineer_features(raw_data)
            
            # Step 3: Create interaction matrix
            logger.info("📊 Step 3: Creating interaction matrix...")
            interaction_matrix = await self._create_interaction_matrix(engineered_features)
            
            # Step 4: Generate summary and statistics
            logger.info("📈 Step 4: Generating summary and statistics...")
            summary = await self._generate_summary(raw_data, engineered_features, interaction_matrix)
            
            # Store processed data
            self.processed_data = {
                "raw_data": raw_data,
                "engineered_features": engineered_features,
                "interaction_matrix": interaction_matrix,
                "summary": summary,
                "timestamp": datetime.now().isoformat()
            }
            
            logger.info("✅ Data processing pipeline completed successfully!")
            return self.processed_data
            
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {e}")
            raise
    
    async def _load_raw_data(self) -> Dict[str, pd.DataFrame]:
        """Load raw data from databases"""
        try:
            # Load data from MongoDB
            users = await self.data_loader.load_users_data()
            posts = await self.data_loader.load_posts_data()
            interactions = await self.data_loader.load_interactions_data()
            
            # Load data from Neo4j
            users_graph, posts_graph, relationships = await self.data_loader.load_graph_data()
            
            raw_data = {
                "mongodb": {
                    "users": users,
                    "posts": posts,
                    "interactions": interactions
                },
                "neo4j": {
                    "users": users_graph,
                    "posts": posts_graph,
                    "relationships": relationships
                }
            }
            
            logger.info(f"Loaded raw data: {len(users)} users, {len(posts)} posts, {len(interactions)} interactions")
            return raw_data
            
        except Exception as e:
            logger.error(f"Error loading raw data: {e}")
            return {}
    
    async def _engineer_features(self, raw_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Engineer features from raw data"""
        try:
            mongodb_data = raw_data.get("mongodb", {})
            
            users_df = mongodb_data.get("users", pd.DataFrame())
            posts_df = mongodb_data.get("posts", pd.DataFrame())
            interactions_df = mongodb_data.get("interactions", pd.DataFrame())
            
            # Engineer features
            user_features = self.feature_engineer.engineer_user_features(users_df, posts_df, interactions_df)
            post_features = self.feature_engineer.engineer_post_features(posts_df, interactions_df)
            interaction_features = self.feature_engineer.engineer_interaction_features(interactions_df, users_df, posts_df)
            
            engineered_features = {
                "users": user_features,
                "posts": post_features,
                "interactions": interaction_features
            }
            
            logger.info("Feature engineering completed successfully")
            return engineered_features
            
        except Exception as e:
            logger.error(f"Error engineering features: {e}")
            return {}
    
    async def _create_interaction_matrix(self, engineered_features: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Create user-post interaction matrix"""
        try:
            users_df = engineered_features.get("users", pd.DataFrame())
            posts_df = engineered_features.get("posts", pd.DataFrame())
            interactions_df = engineered_features.get("interactions", pd.DataFrame())
            
            if users_df.empty or posts_df.empty:
                logger.warning("Insufficient data for interaction matrix")
                return pd.DataFrame()
            
            # Create interaction matrix
            interaction_matrix = self.feature_engineer.create_user_post_matrix(users_df, posts_df, interactions_df)
            
            logger.info("Interaction matrix created successfully")
            return interaction_matrix
            
        except Exception as e:
            logger.error(f"Error creating interaction matrix: {e}")
            return pd.DataFrame()
    
    async def _generate_summary(self, raw_data: Dict[str, Any], engineered_features: Dict[str, Any], 
                               interaction_matrix: pd.DataFrame) -> Dict[str, Any]:
        """Generate comprehensive summary"""
        try:
            # Data summary
            data_summary = await self.data_loader.get_data_summary()
            
            # Feature summary
            feature_summary = self.feature_engineer.get_feature_summary(
                engineered_features.get("users", pd.DataFrame()),
                engineered_features.get("posts", pd.DataFrame()),
                engineered_features.get("interactions", pd.DataFrame())
            )
            
            # Pipeline summary
            pipeline_summary = {
                "pipeline_status": "completed",
                "timestamp": datetime.now().isoformat(),
                "raw_data_size": {
                    "users": len(raw_data.get("mongodb", {}).get("users", pd.DataFrame())),
                    "posts": len(raw_data.get("mongodb", {}).get("posts", pd.DataFrame())),
                    "interactions": len(raw_data.get("mongodb", {}).get("interactions", pd.DataFrame()))
                },
                "engineered_features_size": {
                    "users": len(engineered_features.get("users", pd.DataFrame())),
                    "posts": len(engineered_features.get("posts", pd.DataFrame())),
                    "interactions": len(engineered_features.get("interactions", pd.DataFrame()))
                },
                "interaction_matrix_size": {
                    "rows": len(interaction_matrix),
                    "columns": len(interaction_matrix.columns) if not interaction_matrix.empty else 0
                }
            }
            
            summary = {
                "data_summary": data_summary,
                "feature_summary": feature_summary,
                "pipeline_summary": pipeline_summary
            }
            
            logger.info("Summary generated successfully")
            return summary
            
        except Exception as e:
            logger.error(f"Error generating summary: {e}")
            return {}
    
    def save_processed_data(self, output_dir: str = "./processed_data"):
        """Save processed data to files"""
        try:
            import os
            os.makedirs(output_dir, exist_ok=True)
            
            # Save engineered features
            for data_type, df in self.processed_data.get("engineered_features", {}).items():
                if not df.empty:
                    output_path = f"{output_dir}/{data_type}_features.csv"
                    df.to_csv(output_path, index=False)
                    logger.info(f"Saved {data_type} features to {output_path}")
            
            # Save interaction matrix
            interaction_matrix = self.processed_data.get("interaction_matrix", pd.DataFrame())
            if not interaction_matrix.empty:
                output_path = f"{output_dir}/interaction_matrix.csv"
                interaction_matrix.to_csv(output_path, index=False)
                logger.info(f"Saved interaction matrix to {output_path}")
            
            # Save summary
            summary = self.processed_data.get("summary", {})
            if summary:
                output_path = f"{output_dir}/pipeline_summary.json"
                with open(output_path, 'w') as f:
                    json.dump(summary, f, indent=2, default=str)
                logger.info(f"Saved summary to {output_path}")
            
            logger.info(f"All processed data saved to {output_dir}")
            
        except Exception as e:
            logger.error(f"Error saving processed data: {e}")
    
    def get_processed_data(self) -> Dict[str, Any]:
        """Get processed data"""
        return self.processed_data
    
    def close(self):
        """Close the pipeline"""
        if self.data_loader:
            self.data_loader.close()


async def main():
    """Test the data processing pipeline"""
    pipeline = DataProcessingPipeline()
    try:
        await pipeline.initialize()
        
        # Run the pipeline
        result = await pipeline.run_full_pipeline()
        
        # Print summary
        print("\n" + "="*50)
        print("📊 PIPELINE RESULTS SUMMARY")
        print("="*50)
        
        summary = result.get("summary", {})
        pipeline_summary = summary.get("pipeline_summary", {})
        
        print(f"✅ Pipeline Status: {pipeline_summary.get('pipeline_status', 'unknown')}")
        print(f"📅 Timestamp: {pipeline_summary.get('timestamp', 'unknown')}")
        
        raw_data_size = pipeline_summary.get("raw_data_size", {})
        print(f"👥 Raw Users: {raw_data_size.get('users', 0)}")
        print(f"📝 Raw Posts: {raw_data_size.get('posts', 0)}")
        print(f"🔄 Raw Interactions: {raw_data_size.get('interactions', 0)}")
        
        engineered_size = pipeline_summary.get("engineered_features_size", {})
        print(f"🔧 Engineered Users: {engineered_size.get('users', 0)}")
        print(f"🔧 Engineered Posts: {engineered_size.get('posts', 0)}")
        print(f"🔧 Engineered Interactions: {engineered_size.get('interactions', 0)}")
        
        matrix_size = pipeline_summary.get("interaction_matrix_size", {})
        print(f"📊 Interaction Matrix: {matrix_size.get('rows', 0)} rows x {matrix_size.get('columns', 0)} columns")
        
        # Save processed data
        pipeline.save_processed_data()
        
    except Exception as e:
        logger.error(f"Pipeline test failed: {e}")
    finally:
        pipeline.close()


if __name__ == "__main__":
    asyncio.run(main())
