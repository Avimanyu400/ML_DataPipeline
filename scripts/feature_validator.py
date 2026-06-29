import pandas as pd
import json
import os
import datetime
from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres:postgres@localhost/dmml')
# 1. THE FEATURE REGISTRY
class FeatureRegistry:
    def __init__(self, storage_dir: str = "feature_store"):
        self.storage_dir = storage_dir
        self.registry_file = os.path.join(storage_dir, "registry.json")
        if not os.path.exists(self.storage_dir):
            os.makedirs(self.storage_dir)
        self.metadata = self._load_metadata()

    def _load_metadata(self):
        if os.path.exists(self.registry_file):
            with open(self.registry_file, 'r') as f:
                return json.load(f)
        return {"versions": {}}

    def register(self, df, version, description, columns):
        file_path = os.path.join(self.storage_dir, f"features_v{version}.csv")
        df.to_csv(file_path, index=False)
        self.metadata["versions"][version] = {
            "columns": columns,
            "description": description,
            "file_path": file_path
        }
        with open(self.registry_file, 'w') as f:
            json.dump(self.metadata, f, indent=4)
        print(f"✅ Registered version {version}")

# 2. THE VALIDATOR
class FeatureValidator:
    def __init__(self, registry_instance):
        self.registry = registry_instance

    def validate(self, df_new, ref_version="1.0.0"):
        if ref_version not in self.registry.metadata["versions"]:
            return True # Nothing to compare against yet
        
        ref_cols = set(self.registry.metadata["versions"][ref_version]["columns"])
        new_cols = set(df_new.columns)
        
        if not ref_cols.issubset(new_cols):
            missing = ref_cols - new_cols
            print(f"❌ Validation Failed: Missing columns {missing}")
            return False
            
        print("✅ Schema Validation Passed.")
        return True

# 3. INITIALIZATION & EXECUTION (Fixes the NameError)
# First, create the store instance
store = FeatureRegistry()

# Load your data
#df_features = pd.read_csv('recommendation_features.csv')
click_query = """
    SELECT user_id, product_id, category_encoded, brand_encoded, action_encoded, price_normalized, timestamp_normalized, interaction_score
    FROM dmmlschema.recommendation_features 
    """
df_features = pd.read_sql(click_query, engine)

# Create the validator instance using the 'store' we just made
validator = FeatureValidator(store)

# Validate and then Register
if validator.validate(df_features, ref_version="1.0.0"):
    store.register(
        df=df_features, 
        version="1.0.0", 
        description="Baseline recommendation features",
        columns=list(df_features.columns)
    )