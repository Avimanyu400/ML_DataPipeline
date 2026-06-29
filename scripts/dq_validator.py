import pandas as pd
from sqlalchemy import create_engine
import great_expectations as gx

# 1. Setup PostgreSQL Connection
# Format: 'postgresql://username:password@host:port/database'
DATABASE_URL = 'postgresql://postgres:postgres@localhost/dmml'
engine = create_engine(DATABASE_URL)

def get_interaction_file():
    # 2. Pull Clickstream (Behavioral Data)
    # We select only the last 30 days to keep the model focused on fresh trends
    click_query = """
    SELECT user_id, product_id, action, timestamp 
    FROM dmmlschema.clickstream_logs 
    WHERE timestamp > NOW() - INTERVAL '30 days'
    """
    df_clicks = pd.read_sql(click_query, engine)

    # 3. Pull Product Metadata (Content Data)
    prod_query = "SELECT product_id, category, brand, price FROM dmmlschema.product_catalog"
    df_prods = pd.read_sql(prod_query, engine)

    # 4. Perform the Left Join
    # This attaches product details to every user action
    interaction_df = pd.merge(df_clicks, df_prods, on='product_id', how='left')

    # 5. Engineering: Implicit Ratings
    # Recommenders work best with numerical scores rather than text labels
    mapping = {'view': 1, 'add_to_cart': 3, 'purchase': 5, 'click': 1, 'wishlist': 2}
    interaction_df['interaction_score'] = interaction_df['action'].map(mapping)

    # 6. Clean Up
    # Fill missing categories so the model doesn't crash
    interaction_df['category'] = interaction_df['category'].fillna('Unknown')
    
    return interaction_df

# Generate the file
#df_interactions = get_interaction_file()
# =========================================================================
def cleanse_interaction_data(interaction_df):

    #-------------------------------
    # IMPORTANT: Reset index to ensure GX indices match Pandas rows perfectly
    df_raw = interaction_df.reset_index(drop=True)
    #print("Unique categories in data:", df_raw['category'].unique())
    
    # 2. Initialize GX Context
    context = gx.get_context()
    datasource = context.data_sources.add_pandas(name="retail_source")
    data_asset = datasource.add_dataframe_asset(name="interactions")
    batch = data_asset.add_batch_definition_whole_dataframe("raw_batch").get_batch(batch_parameters={"dataframe": df_raw})

    # 3. Define the Expectation Suite (The Rules)
    suite_name = "retail_cleansing_suite"
    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))

    # Rule 1: Ensure Product IDs exist
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="product_id"))

    # Rule 2: Valid interaction types only
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column="action", 
        value_set=["view", "cart", "purchase", "add_to_cart", "click"]
    ))

    # 3. Logic Check: Price must be positive
    # Prevents items with $0.00 or negative prices from skewing revenue-based models
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="price", min_value=1.0, max_value=45000.00
    ))

    # 4. Chronology Check: Timestamps must be in the past
    # Catches system clock errors (e.g., year 1970 or year 2099)
    import datetime
    current_time = datetime.datetime.now().isoformat()
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="timestamp", max_value=current_time
    ))

    # 5. Integrity Check: Mandatory IDs
    # A row is useless for a recommender if it doesn't have BOTH a user and a product
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="user_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="product_id"))

    # 6. Distribution Check: Bot/Scraper Detection
    # If a single user interacts with more than 500 unique items in a short batch, 
    
    suite.add_expectation(gx.expectations.ExpectColumnUniqueValueCountToBeBetween(
        column="product_id", max_value=500
    ))

    # 7. Categorical Check: Brand/Category consistency
    # Ensures all brands are strings and not just numbers or "NaN"
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(
        column="brand", type_="str"
    ))

    # 8. Define the rule: Category must NOT be in the "bad" list
    # This rule fails if the category matches 'unknown' in any case (Unknown, UNKNOWN, uNkNoWn)
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotMatchRegex(
        column="category",
        regex=r"(?i)^unknown$",  # (?i) makes it case-insensitive
        meta={"description": "Catch all variations of Unknown"}
    ))

    # Also check for empty strings or whitespace-only strings
    # Force a failure if even ONE 'Unknown' exists
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeInSet(
        column="category",
        value_set=["Unknown"],
        mostly=0.5  # 1.0 means 100% of rows must NOT be 'Unknown'
    ))

    # Run validation with 'COMPLETE' format to see every bad row
    results = batch.validate(suite, result_format="COMPLETE")

# ==================================================================================
    # Print a summary of the results
    print(f"Validation Success: {results['success']}")
    print(f"Stats: {results['statistics']}")

    # 4. View specific failures
    for item in results['results']:
        if not item['success']:
        # Access the expectation type and column safely
            config = item['expectation_config']
        
            # In newer versions, expectation_type is a direct attribute
            # In others, it might be nested in the dictionary
            exp_type = getattr(config, "expectation_type", "Unknown Expectation")
            column = config.kwargs.get('column', 'Table-Level Rule')
        
            print(f"\n Column '{column}' failed!")
            print(f"Issue: {exp_type}")
            print(f"Unexpected count: {item['result'].get('unexpected_count', 0)}")
        
            # Optional: Print samples of the bad data
            bad_values = item['result'].get('partial_unexpected_list', [])
            if bad_values:
                print(f"Sample invalid data: {bad_values}")
#==================================================================================
    # Extract Invalid Records (The Debugging Step)
    invalid_indices = set()
    invalid_report = []

    for result in results.results:
        if not result.success:
            # Get metadata about the failure
            col = result.expectation_config.kwargs.get("column", "Table-Level")
            bad_values = result.result.get("unexpected_list", [])
            indices = result.result.get("unexpected_index_list", [])
        
            # Track indices for filtering
            invalid_indices.update(indices)
        
            # Log details for the report
            for val in bad_values:
                invalid_report.append({"column": col, "invalid_value": val})

    # Create the DataFrames
    df_invalid = df_raw.loc[list(invalid_indices)]
    df_clean = df_raw.drop(list(invalid_indices))

    # 6. Final Summary
    print(f" Success: {len(df_clean)} rows")
    print(f" Failed: {len(df_invalid)} rows")

    if not df_invalid.empty:
        print("\nSample of Invalid Records:")
        print(df_invalid.head())
    # ----------------------------------------
   
    return df_clean

def load_to_postgres(df, table_name, rmschema, engine):

    """Appends data to Postgres. Creates table if it doesn't exist."""
    df.to_sql(table_name, engine, schema=rmschema, if_exists='replace', index=False)
    print("**********************************************************")
    print(f"Successfully loaded {len(df)} rows to postgres table '{table_name}'")
    

if __name__ == "__main__":
    # Generate synthetic data and save as csv files
    df_interactions = get_interaction_file()
    
    #  Execute Cleansing
    clean_interactions_df = cleanse_interaction_data(df_interactions)
    print(f"Rows before: {len(df_interactions)} , Rows after cleansing: {len(clean_interactions_df)}")

    # Load cleansed data tp postgres
 
    load_to_postgres(clean_interactions_df,"userinteraction","dmmlschema", engine)
    