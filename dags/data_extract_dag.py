# big help from https://www.youtube.com/watch?v=i25ttd32-eo
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from batch_process import extract_csv_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 17),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'data_extract_dag',
    default_args=default_args,
    description='Simple DAG for Data Engineer Test',
    schedule_interval=timedelta(days=1), # daily extract for nonstatic data
)

run_etl = PythonOperator(
    task_id='csv_data_extract',
    python_callable=extract_csv_data,
    dag=dag,
)

run_etl