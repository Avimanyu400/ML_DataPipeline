from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact # New import for UI reports
import subprocess
import sys

# Each task below wraps an external Python script to maintain modularity 

@task(retries=2, retry_delay_seconds=30)
def run_ingestion():
    """Ingest data from CSV and JSON Streaming Files """
    logger = get_run_logger()
    logger.info("Starting Data Ingestion Stage...")
    result = subprocess.run([sys.executable, "scripts/ingestion.py"], capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"Ingestion Failed: {result.stderr}")
        raise Exception("Ingestion script failed.")
    logger.info("Ingestion Success.")

@task(log_prints=True)
def run_validation():
    """Task 4: Data Profiling and Validation with GX Output Logging"""
    logger = get_run_logger()
    logger.info("Starting Data Validation Stage...")
    
    # Run the script and capture output
    result = subprocess.run(
        [sys.executable, "scripts/dq_validator.py"], 
        capture_output=True, 
        text=True
    )
    
    # 1. Print the stdout to the Prefect logs
    if result.stdout:
        print("--- Great Expectations Standard Output ---")
        print(result.stdout)
    
    # 2. Handle failures and print errors
    if result.returncode != 0:
        logger.error("Great Expectations Validation Failed!")
        if result.stderr:
            print("--- GX Error Log ---")
            print(result.stderr)
            
        # 3. Create a Markdown Artifact to highlight the failure in the UI
        create_markdown_artifact(
            key="gx-validation-failure",
            markdown=f"## ❌ Validation Failed\nCheck the task logs for full trace.\n\n**Error Summary:**\n`{result.stderr[:500]}`",
            description="High-level failure notice for Data Quality"
        )
        raise Exception("Data validation failed.")
    
    logger.info("Validation Success.")
    
    # 4. Create a Success Artifact
    create_markdown_artifact(
        key="gx-validation-success",
        markdown="## ✅ Validation Passed\nGreat Expectations found no critical data quality issues.",
        description="Data Quality Check Status"
    )

@task(log_prints=True)
def run_preparation():
    """Task 5: Data Preparation and Cleaning  """
    logger = get_run_logger()
    logger.info("Starting Data Preparation Stage...")
    subprocess.run([sys.executable, "scripts/preprocess_eda.py"], check=True)
    logger.info("Preparation Success.")

@task(log_prints=True)
def run_feature_engg():
    """Task 6 & 7: Feature Engineering and Feature Store update  """
    logger = get_run_logger()
    logger.info("Updating Feature Engineering...")
    subprocess.run([sys.executable, "scripts/feature_engineering.py"], check=True)
    logger.info("Feature Engineering updated successfully.")
    
@task(log_prints=True)
def run_feature_store():
    """Task 6 & 7:   Feature Store update  """
    logger = get_run_logger()
    logger.info("Updating Feature Store...")
    subprocess.run([sys.executable, "scripts/feature_store.py"], check=True)
    logger.info("Feature Store updated successfully.")

@task(log_prints=True)
def run_schema_validator():
    """Task 6 & 7:   Schema validation and versioning  """
    logger = get_run_logger()
    logger.info("Validating schema & versioning...")
    subprocess.run([sys.executable, "scripts/feature_validator.py"], check=True)
    logger.info("Feature Store updated successfully.")
    
@task(log_prints=True)
def run_model_training():
    """Task 9: Model Training and MLflow tracking  """
    logger = get_run_logger()
    logger.info("Starting Model Training Stage...")
    subprocess.run([sys.executable, "scripts/ml_model_training.py"], check=True)
    logger.info("Model Training and Experiment Tracking Complete.")

@flow(name="RecoMart_End_to_End_Orchestration", log_prints=True)
def recomart_full_pipeline():
    """Main flow to orchestrate Task """
    # Define execution order
    ingest = run_ingestion()
    validate = run_validation(wait_for=[ingest])
    prep = run_preparation(wait_for=[validate])
    feateng = run_feature_engg(wait_for=[prep])
    feat = run_feature_store(wait_for=[feateng])
    ver = run_schema_validator(wait_for=[feat])
    run_model_training(wait_for=[ver])

if __name__ == "__main__":
    recomart_full_pipeline()