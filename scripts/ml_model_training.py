import pandas as pd
import numpy as np
import os
import json
import datetime
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.metrics.pairwise import cosine_similarity
from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres:postgres@localhost/dmml')

# --- 1. FEATURE REGISTRY SYSTEM ---
class FeatureRegistry:
    def __init__(self, storage_dir="feature_store"):
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

    def register(self, df, version, description):
        file_path = os.path.join(self.storage_dir, f"features_v{version}.csv")
        df.to_csv(file_path, index=False)
        self.metadata["versions"][version] = {
            "columns": list(df.columns),
            "description": description,
            "file_path": file_path,
            "timestamp": datetime.datetime.now().isoformat()
        }
        with open(self.registry_file, 'w') as f:
            json.dump(self.metadata, f, indent=4)
        print(f"✅ Version {version} registered.")

# --- 2. PIPELINE EXECUTION ---

# A. Load Initial Prepared Data
#df = pd.read_csv('prepared_user_interaction.csv')
engine = create_engine('postgresql://postgres:postgres@localhost/dmml')
load_query = """
    SELECT user_id, product_id, category_encoded, brand_encoded, action_encoded, price_normalized, timestamp_normalized, interaction_score
    FROM dmmlschema.prepared_user_interaction 
    """
df = pd.read_sql(load_query, engine)


# B. Feature Engineering (Ensuring all columns exist)
print("🛠️ Engineering features...")
user_stats = df.groupby('user_id').agg(
    user_activity_count=('user_id', 'count'),
    user_avg_score=('interaction_score', 'mean')
).reset_index()

item_stats = df.groupby('product_id').agg(
    item_activity_count=('product_id', 'count'),
    item_avg_score=('interaction_score', 'mean')
).reset_index()

# User preference per category
cat_pref = df.groupby(['user_id', 'category_encoded']).size().reset_index(name='user_category_count')

# Similarity Scores
pivot = df.pivot_table(index='user_id', columns='product_id', values='interaction_score').fillna(0)
item_sim = pd.DataFrame(cosine_similarity(pivot.T), index=pivot.columns, columns=pivot.columns)

def get_affinity(uid, pid):
    history = pivot.loc[uid]
    items = history[history > 0].index.tolist()
    return item_sim.loc[pid, items].mean() if items else 0

# Merge everything
df_final = df.merge(user_stats, on='user_id') \
             .merge(item_stats, on='product_id') \
             .merge(cat_pref, on=['user_id', 'category_encoded'])

df_final['user_item_similarity_score'] = df_final.apply(lambda x: get_affinity(x['user_id'], x['product_id']), axis=1)

# C. Registry & Versioning
store = FeatureRegistry()
store.register(df_final, version="1.0.0", description="Full feature set for recommendation model")

# D. Model Training
print("🚀 Training model...")
features = [
    'user_activity_count', 'item_activity_count', 
    'user_avg_score', 'item_avg_score', 
    'user_category_count', 'user_item_similarity_score',
    'price_normalized', 'timestamp_normalized'
]

X = df_final[features]
y = df_final['interaction_score']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
model = RandomForestRegressor(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# E. Results
y_pred = model.predict(X_test)
print(f"Final Model MSE: {mean_squared_error(y_test, y_pred):.4f}")
print(f"Final Model R2: {r2_score(y_test, y_pred):.4f}")