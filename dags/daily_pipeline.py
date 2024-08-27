import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import main

default_args = {
    "owner": "your_name",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "daily_data_pipeline",
    default_args=default_args,
    description="Run main.py daily at 9 AM UTC",
    schedule_interval="0 9 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

run_main_task = PythonOperator(
    task_id="run_main_task",
    python_callable=main,
    dag=dag,
)
