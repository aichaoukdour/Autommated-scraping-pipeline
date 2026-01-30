from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    'aaa_heartbeat_test',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['debug'],
) as dag:
    EmptyOperator(task_id='heartbeat')
