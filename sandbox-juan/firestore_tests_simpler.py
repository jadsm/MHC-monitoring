import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
from typing import Generator, Dict
import pandas_gbq as pdg
import logging
import time

# Simplified Logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

class FirestoreStreamer:
    """Focuses solely on streaming data out of Firestore efficiently."""
    
    def __init__(self, creds_path: str):
        if not firebase_admin._apps:
            cred = credentials.Certificate(creds_path)
            firebase_admin.initialize_app(cred)
        self.db = firestore.client()

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
            time.sleep(0.1) # Soft rate limit

    def get_user_observations(self, user_id: str) -> Generator[Dict, None, None]:
        """Streams health observations for a single user."""
        user_ref = self.db.collection("users").document(user_id)
        
        # Only fetch subcollections that match the pattern
        for sub in user_ref.collections():
            if not sub.id.startswith("HealthObservations"):
                continue
                
            for doc in self.stream_collection(sub):
                data = doc.to_dict()
                period = data.get('effectivePeriod')
                
                if period and isinstance(period, dict):
                    yield {
                        **period,
                        'user_id': user_id,
                        'metric': sub.id,
                        'value': data.get('valueQuantity', {}).get('value'),
                        'unit': data.get('valueQuantity', {}).get('unit'),
                        'value_str': data.get('valueString')
                    }

def main():
    creds = "/home/juan/Desktop/Juan/code/.creds/creds-myheart-counts-development.json"
    streamer = FirestoreStreamer(creds)
    
    users_accumulator = []
    obs_accumulator = []
    
    # Process users as a stream
    user_col = streamer.db.collection("users")
    for user_doc in streamer.stream_collection(user_col, batch_size=10):
        tic = time.time()
        user_id = user_doc.id
        logger.info(f"Processing user: {user_id}")
        
        # Add User Data
        users_accumulator.append({"user_id": user_id, **user_doc.to_dict()})
        
        # Add Observations
        for obs in streamer.get_user_observations(user_id):
            obs_accumulator.append(obs)
            
        # Periodic Checkpointing (e.g., every 50 users)
        if len(users_accumulator) % 5 == 0:
            pd.DataFrame(obs_accumulator).to_csv(f"temp/obs_checkpoint.csv", index=False)
            pd.DataFrame(users_accumulator).to_csv(f"temp/users_checkpoint.csv", index=False)
        tac = time.time()
        logger.info(f"Finished user: {user_id} in {tac - tic:.2f} seconds")


    # Final Save
    pdg.to_gbq(pd.DataFrame(users_accumulator), "myheart_counts_development.users",
                project_id="myheart-counts-development",
                if_exists="append")
    pdg.to_gbq(pd.DataFrame(obs_accumulator), "myheart_counts_development.observations",
                project_id="myheart-counts-development",
                if_exists="append")
    pd.DataFrame(users_accumulator).to_csv("temp/users_final2.csv", index=False)
    pd.DataFrame(obs_accumulator).to_csv("temp/obs_final2.csv", index=False)

if __name__ == "__main__":
    main()