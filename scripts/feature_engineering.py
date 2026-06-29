import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
from pathlib import Path
from sklearn.metrics.pairwise import cosine_similarity

engine = create_engine('postgresql://postgres:postgres@localhost/dmml')


def get_data():
    click_query = """
        SELECT user_id, product_id, category_encoded, brand_encoded, action_encoded, price_normalized, timestamp_normalized, interaction_score
        FROM dmmlschema.prepared_user_interaction 
        """
    df = pd.read_sql(click_query, engine)

    print(df.head(5))
    return df

def aggregate(df):
    # --- 1. Activity & Rating Aggregations ---
    user_stats = df.groupby('user_id').agg(
        user_activity_count=('user_id', 'count'),
        user_avg_score=('interaction_score', 'mean')
    ).reset_index()

    item_stats = df.groupby('product_id').agg(
        item_activity_count=('product_id', 'count'),
        item_avg_score=('interaction_score', 'mean')
    ).reset_index()

    # --- 2. Similarity Calculation (Cosine) ---
    # Create User-Item matrix for similarity profiling
    pivot = df.pivot_table(index='user_id', columns='product_id', values='interaction_score').fillna(0)
    item_sim = pd.DataFrame(cosine_similarity(pivot.T), index=pivot.columns, columns=pivot.columns)
    # --- 3. Final Feature Integration ---
    df_warehouse = df.merge(user_stats, on='user_id').merge(item_stats, on='product_id')
    df_warehouse['user_item_similarity_score'] = df_warehouse.apply(
        lambda x: get_affinity(x['user_id'], x['product_id'], pivot, item_sim), axis=1
    )
    return df_warehouse

def get_affinity(user_id, item_id, pivot, item_sim):
    history = pivot.loc[user_id]
    history_items = history[history > 0].index.tolist()
    return item_sim.loc[item_id, history_items].mean() if history_items else 0



def load_to_postgres(df, table_name, rmschema, engine):
    """Appends data to Postgres. Creates table if it doesn't exist."""
    df.to_sql(table_name, engine, schema=rmschema, if_exists='append', index=False)
    print(f"Recommendation features successfully loaded {len(df)} rows to postgres table '{table_name}'")

# MAIN 
if __name__ == "__main__":

    df = get_data()
    #aggregate(df)
    df_warehouse = aggregate(df)
    #df_warehouse = feature_integration(df)
    load_to_postgres(df_warehouse,"recommendation_features","dmmlschema", engine)