from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 24),
    'retries': 10,
    'retry_delay': timedelta(minutes=1),
}


def run_script(order_book_name):
    subprocess.run(['python', '/opt/airflow/src/order_book_scrapping/order_book_spread_scrapping.py', order_book_name])


dag = DAG(
    'process_order_book',
    default_args=default_args,
    description='A DAG to continuously process order book',
    schedule_interval=None,  # Run manually
)

order_book_name = 'btc_mxn'  # Replace with the desired order book name

process_order_book_task = PythonOperator(
    task_id='process_order_book_task',
    python_callable=run_script,
    op_args=[order_book_name],
    dag=dag
)
