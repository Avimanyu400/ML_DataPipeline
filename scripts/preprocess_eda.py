import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import os
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import LabelEncoder, MinMaxScaler

# 1. Setup PostgreSQL Connection
# Format: 'postgresql://username:password@host:port/database'
DATABASE_URL = 'postgresql://postgres:postgres@localhost/dmml'
engine = create_engine(DATABASE_URL)

# 2. Get current directory 
script_dir = os.getcwd() 
base_path = os.path.join(script_dir, "processed")

# 1. LOAD DATA
def load_data():
    # 2. Pull Clickstream (Behavioral Data)
    # We select only the last 30 days to keep the model focused on fresh trends
    click_query = """
    SELECT user_id, product_id, action, timestamp, category, brand, price, interaction_score
    FROM dmmlschema.userinteraction 
    """
    df_interaction = pd.read_sql(click_query, engine)
    
    print(f"Dataset loaded: {df_interaction.shape[0]} rows, {df_interaction.shape[1]} columns")
    
    return df_interaction

# 2. DATA CLEANING & PREPROCESSING
def clean_and_preprocess(df):
    print("Starting data cleaning and preprocessing...")
    
    # Handle missing values: Fill interaction_score with median
    df['interaction_score'] = df['interaction_score'].fillna(df['interaction_score'].median())
    
    # Standardize categorical data: Map 'wishlist' to 'add_to_cart'
    df['action'] = df['action'].replace({'wishlist': 'add_to_cart'})
    
    # Initialize Preprocessing Tools
    le = LabelEncoder()
    scaler = MinMaxScaler()
    df_prepared = df.copy()
    
    # Encode Categorical Attributes
    df_prepared['category_encoded'] = le.fit_transform(df['category'])
    df_prepared['brand_encoded'] = le.fit_transform(df['brand'])
    df_prepared['action_encoded'] = le.fit_transform(df['action'])
    
    # Normalize Numerical Variables
    # Price
    df_prepared['price_normalized'] = scaler.fit_transform(df[['price']])
    
    # Timestamp (Convert to Unix seconds then normalize)
    df_prepared['timestamp_dt'] = pd.to_datetime(df['timestamp'])
    df_prepared['timestamp_seconds'] = df_prepared['timestamp_dt'].astype(np.int64) // 10**9
    df_prepared['timestamp_normalized'] = scaler.fit_transform(df_prepared[['timestamp_seconds']])
    
    # Select final columns for the prepared dataset
    final_cols = [
        'user_id', 'product_id', 'category_encoded', 'brand_encoded', 
        'action_encoded', 'price_normalized', 'timestamp_normalized', 'interaction_score'
    ]
    df_final = df_prepared[final_cols]
    
    return df, df_final

# 3. EXPLORATORY DATA ANALYSIS (EDA)
def generate_eda_plots(df):
    print("Generating EDA visualizations...")
    sns.set_theme(style="whitegrid")
    
    # Plot 1: Interaction Score Distribution
    plt.figure(figsize=(8, 5))
    # Assigning x to hue and setting legend=False to avoid redundancy
    sns.countplot(x='interaction_score', hue='interaction_score', data=df, palette='viridis', legend=False)
    plt.title('Distribution of Interaction Scores')
    plt.savefig('interaction_score_dist.png')
    
    # Plot 2: Action Frequency
    plt.figure(figsize=(10, 5))
    action_counts = df['action'].value_counts()
    sns.barplot(x=action_counts.index, y=action_counts.values, hue=action_counts.index, palette='magma', legend=False)
    plt.title('Frequency of User Actions')
    plt.xticks(rotation=45)
    plt.savefig('action_frequency.png')
    
    # Plot 3: Item Popularity (Top 10)
    plt.figure(figsize=(10, 5))
    top_items = df['product_id'].value_counts().head(10)
    sns.barplot(x=top_items.index, y=top_items.values, hue=top_items.index, palette='coolwarm', legend=False)
    plt.title('Top 10 Most Popular Products')
    plt.xticks(rotation=45)
    plt.savefig('item_popularity.png')
    
    # Plot 4: Sparsity Heatmap
    user_item_matrix = df.pivot_table(index='user_id', columns='product_id', values='interaction_score')
    plt.figure(figsize=(12, 8))
    sns.heatmap(user_item_matrix.notnull(), cbar=False, cmap='Blues')
    plt.title('User-Item Interaction Sparsity Pattern')
    plt.savefig('sparsity_heatmap.png')
    
    # Calculate Sparsity Metric
    sparsity = (1 - (len(df) / (df['user_id'].nunique() * df['product_id'].nunique()))) * 100
    print(f"Matrix Sparsity: {sparsity:.2f}%")

    print("All plots saved successfully.")

def load_to_postgres(df, table_name, rmschema, engine):
    """Appends data to Postgres. Creates table if it doesn't exist."""
    df.to_sql(table_name, engine, schema=rmschema, if_exists='replace', index=False)
    print(f"prepared user interaction data successfully loaded {len(df)} rows to postgres table '{table_name}'")
    

# MAIN 
if __name__ == "__main__":
    raw_df = load_data()
    cleaned_df, prepared_df = clean_and_preprocess(raw_df)
    
    # Save the processed data
    file_path = os.path.join(base_path, "prepared_user_interaction_data.csv")

    # 4. save product_catalog data
    # Database Connection
    engine = create_engine('postgresql://postgres:postgres@localhost/dmml')
    load_to_postgres(prepared_df,"prepared_user_interaction","dmmlschema", engine)
        
    # Run EDA
    generate_eda_plots(cleaned_df)
    

    
    
    
    
    