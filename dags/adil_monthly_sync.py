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

default_args = {
    'owner': 'antigravity',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'adil_monthly_sync',
    default_args=default_args,
    description='Monthly Sync for ADIL HS Codes',
    schedule_interval='@monthly',
    catchup=False,
    tags=['scraping', 'etl', 'adil'],
) as dag:

    def scrape_task_wrapper(**kwargs):
        logger.info("Starting automated monthly scrap via Airflow...")
        # Airflow can pass parameters via kwargs if needed
        limit = kwargs.get('dag_run').conf.get('limit', None)
        run_pipeline(limit=limit)

    run_scraper = PythonOperator(
        task_id='run_adil_scraper',
        python_callable=scrape_task_wrapper,
        execution_timeout=timedelta(hours=10), # 13k codes take ~7h
    )

    run_scraper
