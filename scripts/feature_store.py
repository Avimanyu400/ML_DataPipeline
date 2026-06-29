import pandas as pd
import json
import os
import datetime
from typing import List, Dict, Optional
from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres:postgres@localhost/dmml')

class FeatureRegistry:
    """
    A lightweight Feature Store for managing versioned recommendation features.
    Tracks lineage, transformations, and data snapshots.
    """
    def __init__(self, storage_dir: str = "feature_store"):
        self.storage_dir = storage_dir
        self.registry_file = os.path.join(storage_dir, "registry.json")
        
        # Ensure storage directory exists
        if not os.path.exists(self.storage_dir):
            os.makedirs(self.storage_dir)
            
        self.metadata = self._load_metadata()

    def _load_metadata(self) -> Dict:
        if os.path.exists(self.registry_file):
            with open(self.registry_file, 'r') as f:
                return json.load(f)
        return {"versions": {}, "active_version": None}

    def _save_metadata(self):
        with open(self.registry_file, 'w') as f:
            json.dump(self.metadata, f, indent=4)

    def register(self, 
                 df: pd.DataFrame, 
                 version: str, 
                 description: str, 
                 sources: List[str], 
                 transformations: List[str]):
        """
        Saves a dataframe as a versioned snapshot and logs its metadata.
        """
        filename = f"features_v{version}.csv"
        file_path = os.path.join(self.storage_dir, filename)
        
        # Save the actual data
        df.to_csv(file_path, index=False)
        
        # Log the metadata
        self.metadata["versions"][version] = {
            "timestamp": datetime.datetime.now().isoformat(),
            "description": description,
            "feature_count": len(df.columns),
            "row_count": len(df),
            "file_path": file_path,
            "sources": sources,
            "transformations": transformations,
            "columns": list(df.columns)
        }
        self.metadata["active_version"] = version
        self._save_metadata()
        print(f"✅ Successfully registered version {version} at {file_path}")

    def get_version(self, version: str) -> pd.DataFrame:
        """Retrieves a specific version of features for training or inference."""
        if version not in self.metadata["versions"]:
            raise KeyError(f"Version {version} not found in the registry.")
            
        path = self.metadata["versions"][version]["file_path"]
        print(f"📦 Retrieving features from version {version}...")
        return pd.read_csv(path)

    def list_versions(self):
        """Prints all registered versions and their descriptions."""
        print("\n--- Feature Registry Versions ---")
        for v, meta in self.metadata["versions"].items():
            active_marker = "*" if v == self.metadata["active_version"] else " "
            print(f"{active_marker} v{v}: {meta['description']} ({meta['timestamp']})")

# --- EXAMPLE USAGE ---
if __name__ == "__main__":
    # 1. Initialize Registry
    store = FeatureRegistry()

    # 2. Simulate Engineered Data 
    click_query = """
    SELECT user_id, product_id, category_encoded, brand_encoded, action_encoded, price_normalized, timestamp_normalized, interaction_score
    FROM dmmlschema.recommendation_features 
    """
    df_new_features = pd.read_sql(click_query, engine)

    # 3. Register a New Version
    store.register(
        df=df_new_features,
        version="1.0.0",
        description="Initial baseline with user activity and item affinity",
        sources=["user_interaction.csv"],
        transformations=[
            "Median imputation for scores",
            "Min-Max scaling for prices",
            "Cosine similarity affinity scores"
        ]
    )

    # 4. Audit the store
    store.list_versions()

    # 5. Fetch for Training
    training_data = store.get_version("1.0.0")