from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add project root to path for imports
sys.path.append("/app")
sys.path.append("/app/src")
sys.path.append("/app/src/etl")

from master_pipeline import run_pipeline
from scraper.config import logger
import pandas as pd
from pathlib import Path

default_args = {
    'owner': 'antigravity',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0, # No retries for test
}

with DAG(
    'adil_test_run',
    default_args=default_args,
    description='Quick Test Run (5 Codes) for ADIL Scraper',
    schedule_interval=None, # Manual trigger only
    catchup=False,
    tags=['test', 'scraping'],
) as dag:

    def test_run_wrapper(**kwargs):
        logger.info("🚀 Launching QUICK TEST RUN (Limit: 5 codes)...")
        # Hardcoded limit for the test DAG
        run_pipeline(limit=5)

    def preview_data(**kwargs):
        logger.info("👀 Previewing Scraped Data Output...")
        csv_path = Path("/app/src/etl/output_csv/hs_products_v3.csv")
        
        if csv_path.exists():
            # Use sep=';' because the industrial pipeline exports with semicolons
            df = pd.read_csv(csv_path, sep=';')
            # Show last 5 rows as a pretty table in logs
            # Columns in v3 CSV are hs10, designation, section_label
            preview = df.tail(5)[['hs10', 'designation', 'section_label']]
            logger.info("\n" + preview.to_string(index=False))
            logger.info(f"✅ Total records in CSV: {len(df)}")
        else:
            logger.error(f"❌ Output CSV not found at {csv_path}")

    run_test = PythonOperator(
        task_id='run_adil_test',
        python_callable=test_run_wrapper,
        execution_timeout=timedelta(minutes=15),
    )

    show_preview = PythonOperator(
        task_id='preview_scraped_data',
        python_callable=preview_data,
    )

    run_test >> show_preview
