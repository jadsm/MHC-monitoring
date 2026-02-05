import re
import firebase_admin
from firebase_admin import credentials, firestore
from typing import Generator, Dict, Optional
from datetime import datetime
import time
import os
import pandas_gbq as pdg

prefixes = [
    "HealthObservations_HKQuantityTypeIdentifier",
    "HealthObservations_HKCategoryTypeIdentifier",
    "HealthObservations_com.apple.SensorKit.",
    "HealthObservations_HKDataTypeIdentifier",
    "HealthObservations_HKWorkoutTypeIdentifier",
    "HealthObservations_MHC"
]
pattern = re.compile(f"^({'|'.join(map(re.escape, prefixes))})")

def clean_metric(metric):
    clean = pattern.sub('', metric)
    return clean if clean else 'Workout'

class FirestoreStreamer:
    """Focuses solely on streaming data out of Firestore efficiently."""
    
    def __init__(self, logger):
        self.logger = logger
        self.initialize_firebase()
        self.db = firestore.client()
        query = f"SELECT user_id FROM `myheart_counts_development.users3`"
        self.users_in_BQ = tuple(pdg.read_gbq(query, project_id="myheart-counts-development", dialect="standard")['user_id'])
        
    
    def initialize_firebase(self):
        if not firebase_admin._apps:
            cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            
            if cred_path and os.path.exists(cred_path):
                # LOCAL: Use the JSON file provided in the env var
                self.logger.info(f"Initializing with local credentials: {cred_path}")
                cred = credentials.Certificate(cred_path)
            else:
                # CLOUD RUN: Use the built-in Service Account identity
                self.logger.info("Initializing with Application Default Credentials (ADC)")
                cred = credentials.ApplicationDefault()
                
            firebase_admin.initialize_app(cred)

    def get_last_sync_time(self) -> Optional[datetime]:
        """Retrieve the last sync timestamp from a tracking document."""
        try:
            sync_doc = self.db.collection("_sync_metadata").document("last_sync").get()
            if sync_doc.exists:
                timestamp = sync_doc.to_dict().get('timestamp')
                self.logger.info(f"Last sync was at: {timestamp}")
                return timestamp
            else:
                self.logger.info("No previous sync found")
                return None
        except Exception as e:
            self.logger.warning(f"Could not retrieve last sync time: {e}")
            return None
    
    def update_last_sync_time(self, sync_time: datetime):
        """Update the last sync timestamp."""
        try:
            self.db.collection("_sync_metadata").document("last_sync").set({
                'timestamp': sync_time,
                'updated_at': firestore.SERVER_TIMESTAMP
            })
            self.logger.info(f"Updated last sync time to: {sync_time}")
        except Exception as e:
            self.logger.error(f"Failed to update sync time: {e}")
    
    def stream_collection(self, collection_ref, batch_size: int = 500) -> Generator[Dict, None, None]:
        """Generic generator to handle pagination logic once."""
        last_doc = None
        while True:
            query = collection_ref.limit(batch_size)
            if last_doc:
                query = query.start_after(last_doc)
            
            docs = list(query.stream())
            if not docs:
                break
                
            for doc in docs:
                yield doc
            
            last_doc = docs[-1]
            time.sleep(0.1)
    
    def get_user_observations(self, user_id: str, last_sync_time: Optional[datetime] = None) -> Generator[Dict, None, None]:
        """Streams health observations for a single user, filtering by issued timestamp."""
        user_ref = self.db.collection("users").document(user_id)
        
        for sub in user_ref.collections():
            if not sub.id.startswith("HealthObservations"):
                continue
            
            # Build query with 'issued' filter for new documents
            query = sub
            if last_sync_time:
                query = query.where('issued', '>', last_sync_time)
            
            # Paginate through filtered results
            last_doc = None
            while True:
                paginated_query = query.limit(500)
                if last_doc:
                    paginated_query = paginated_query.start_after(last_doc)
                
                docs = list(paginated_query.stream())
                if not docs:
                    break
                
                for doc in docs:
                    data = doc.to_dict()
                    period = data.get('effectivePeriod')
                    issued = data.get('issued')
                    
                    if period and isinstance(period, dict):
                        yield {
                            **period,
                            'user_id': user_id,
                            'metric': clean_metric(sub.id),
                            'value': data.get('valueQuantity', {}).get('value'),
                            'unit': data.get('valueQuantity', {}).get('unit'),
                            'value_str': data.get('valueString'),
                            'issued': issued  # Include 'issued' timestamp for tracking
                        }
                
                last_doc = docs[-1]
                time.sleep(0.05)
    
    def count_updated_observations(self, user_id: str, last_sync_time: Optional[datetime] = None) -> int:
        """Quick count of how many new observations were issued for a user."""
        user_ref = self.db.collection("users").document(user_id)
        total = 0
        
        for sub in user_ref.collections():
            if not sub.id.startswith("HealthObservations"):
                continue
            
            query = sub
            if last_sync_time:
                query = query.where('issued', '>', last_sync_time)
            
            # Count without fetching all data
            count_docs = list(query.select([]).limit(1000).stream())
            total += len(count_docs)
        
        return total
