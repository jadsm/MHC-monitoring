import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
from typing import List, Dict, Optional
from pathlib import Path
import logging
import time
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FirestoreDataExtractor:
    """Efficiently extracts health data from Firestore with rate limiting and batching."""
    
    def __init__(self, creds_path: str, output_dir: str = 'temp', 
                 batch_size: int = 500, rate_limit_delay: float = 0.1):
        """
        Initialize Firebase and setup output directory.
        
        Args:
            creds_path: Path to Firebase credentials
            output_dir: Directory for output files
            batch_size: Number of documents to fetch per batch
            rate_limit_delay: Delay in seconds between batches
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.batch_size = batch_size
        self.rate_limit_delay = rate_limit_delay
        
        cred = credentials.Certificate(creds_path)
        if not firebase_admin._apps:
            firebase_admin.initialize_app(cred)
        self.db = firestore.client()
        
        # Track query counts
        self.query_count = 0
    
    def extract_user_info(self, doc) -> Dict:
        """Extract user information from document."""
        user_data = doc.to_dict()
        user_data['user_id'] = doc.id
        return user_data
    
    def extract_health_observation(self, metric_doc, subcollection_name: str, user_id: str) -> Optional[Dict]:
        """Extract health observation data from a metric document."""
        data = metric_doc.to_dict()
        
        # Skip if no effectivePeriod
        if 'effectivePeriod' not in data or data['effectivePeriod'] is None:
            return None
        
        # Build result dictionary
        result = data['effectivePeriod'].copy()
        result.update({
            'value_str': data.get('valueString'),
            'metric': subcollection_name,
            'user_id': user_id
        })
        
        # Add quantity data if available
        if 'valueQuantity' in data:
            quantity = data['valueQuantity']
            result.update({
                'value': quantity.get('value'),
                'unit': quantity.get('unit')
            })
        
        return result
    
    def process_user_subcollections_with_pagination(self, user_id: str) -> List[Dict]:
        """
        Process HealthObservations with pagination and rate limiting.
        Uses limit() and startAfter() for controlled batching.
        """
        observations = []
        doc_ref = self.db.collection("users").document(user_id)
        
        try:
            # Single query to get subcollections
            subcollections = list(doc_ref.collections())
            self.query_count += 1
        except Exception as e:
            logger.error(f"Error getting subcollections for user {user_id}: {e}")
            return observations
        
        # Process only HealthObservations subcollections
        health_collections = [
            sub_col for sub_col in subcollections 
            if sub_col.id.startswith("HealthObservations")
        ]
        
        for subcollection in health_collections:
            try:
                observations.extend(
                    self._process_collection_batched(subcollection, user_id)
                )
            except Exception as e:
                logger.error(f"Error processing {subcollection.id} for user {user_id}: {e}")
        
        return observations
    
    def _process_collection_batched(self, collection_ref, user_id: str) -> List[Dict]:
        """Process a collection in batches with rate limiting."""
        observations = []
        last_doc = None
        batch_num = 0
        
        while True:
            # Build query with pagination
            query = collection_ref.limit(self.batch_size)
            if last_doc:
                query = query.start_after(last_doc)
            
            # Execute batch query
            batch = list(query.stream())
            self.query_count += 1
            
            if not batch:
                break
            
            batch_num += 1
            logger.debug(f"Processing batch {batch_num} ({len(batch)} docs) for {collection_ref.id}")
            
            # Process documents in batch
            for metric_doc in batch:
                observation = self.extract_health_observation(
                    metric_doc, 
                    collection_ref.id, 
                    user_id
                )
                if observation:
                    observations.append(observation)
            
            # Update pagination cursor
            last_doc = batch[-1]
            
            # Rate limiting
            if len(batch) == self.batch_size:
                time.sleep(self.rate_limit_delay)
            else:
                break  # Last batch (incomplete)
        
        return observations
    
    def process_users_in_batches(self, user_batch_size: int = 10) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Process users in batches to avoid memory issues.
        
        Args:
            user_batch_size: Number of users to process before saving checkpoint
            
        Returns:
            Tuple of (user_df, observations_df)
        """
        users_data = []
        all_observations = []
        
        # Query all users with pagination
        logger.info("Fetching users in batches...")
        users_query = self.db.collection("users")
        self.query_count += 1
        
        last_user_doc = None
        user_count = 0
        
        while True:
            tic = time.time()
            # Fetch batch of users
            query = users_query.limit(user_batch_size)
            if last_user_doc:
                query = query.start_after(last_user_doc)
            
            user_batch = list(query.stream())
            if not user_batch:
                break
            
            user_count += len(user_batch)
            logger.info(f"Processing users {user_count - len(user_batch) + 1} to {user_count}")
            
            # Process each user in batch
            for user_doc in user_batch:
                # Extract user info
                user_info = self.extract_user_info(user_doc)
                users_data.append(user_info)
                
                # Process user's health observations
                observations = self.process_user_subcollections_with_pagination(user_doc.id)
                all_observations.extend(observations)
            
            # Save checkpoint
            self._save_checkpoint(users_data, all_observations)
            
            # Update pagination
            last_user_doc = user_batch[-1]
            
            # Rate limiting between user batches
            if len(user_batch) == user_batch_size:
                time.sleep(self.rate_limit_delay * 2)
            else:
                break
            tac = time.time()
            logger.info(f"Finished batch in {tac - tic:.2f} seconds")
        
        # Create final dataframes
        user_df = pd.DataFrame(users_data)
        observations_df = pd.DataFrame(all_observations)
        
        logger.info(f"Extracted {len(users_data)} users and {len(all_observations)} observations")
        logger.info(f"Total Firestore queries: {self.query_count}")
        
        return user_df, observations_df
    
    def _save_checkpoint(self, users_data: List[Dict], observations: List[Dict]):
        """Save checkpoint data during processing."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if users_data:
            pd.DataFrame(users_data).to_csv(
                self.output_dir / f'checkpoint_users_{timestamp}.csv', 
                index=False
            )
        
        if observations:
            pd.DataFrame(observations).to_csv(
                self.output_dir / f'checkpoint_observations_{timestamp}.csv', 
                index=False
            )
    
    def extract_all_data(self, user_batch_size: int = 10) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Extract all users and their health observations with batching.
        
        Args:
            user_batch_size: Number of users to process per batch
            
        Returns:
            Tuple of (user_df, observations_df)
        """
        return self.process_users_in_batches(user_batch_size)
    
    def save_final_results(self, user_df: pd.DataFrame, observations_df: pd.DataFrame):
        """Save final consolidated results."""
        user_df.to_csv(self.output_dir / 'users_final.csv', index=False)
        observations_df.to_csv(self.output_dir / 'observations_final.csv', index=False)
        logger.info("Final results saved")


def main():
    """Main execution function."""
    # Initialize extractor with batching and rate limiting
    extractor = FirestoreDataExtractor(
        creds_path="/home/juan/Desktop/Juan/code/.creds/creds-myheart-counts-development.json",
        output_dir='temp',
        batch_size=500,  # Fetch 500 docs per query
        rate_limit_delay=0.5  # 500ms delay between batches
    )
    
    # Extract all data with user batching
    user_df, observations_df = extractor.extract_all_data(user_batch_size=10)
    
    # Save final results
    extractor.save_final_results(user_df, observations_df)
    
    # Print summary
    print(f"\nSummary:")
    print(f"Total users: {len(user_df)}")
    print(f"Total observations: {len(observations_df)}")
    print(f"Total Firestore queries: {extractor.query_count}")
    if not observations_df.empty:
        print(f"Unique metrics: {observations_df['metric'].nunique()}")


if __name__ == "__main__":
    main()