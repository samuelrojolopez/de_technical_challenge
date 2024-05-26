from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import logging
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 24),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}


def run_script(arg_order_book_name):
    # TODO: Modularize this python method to reduce responsability and reusability.
    try:
        # Path to the script
        script_path = '/opt/airflow/src/order_book_scrapping/order_book_spread_scrapping.py'

        # Check if the script file exists
        if not os.path.exists(script_path):
            logging.error(f"Script file does not exist: {script_path}")
            return

        # Run the subprocess
        result = subprocess.run(
            ['python', script_path, arg_order_book_name],
            check=True,
            capture_output=True,
            text=True
        )
        logging.info(f"Script output: {result.stdout}")
        logging.error(f"Script error (if any): {result.stderr}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error occurred while running script: {e}")
        logging.error(f"Return code: {e.returncode}")
        logging.error(f"Output: {e.output}")
        raise


dag = DAG(
    'usd_mxn_order_book_spread_pipeline',
    default_args=default_args,
    description='USD_MXN Spread scrapping and Storage Pipeline',
    schedule_interval=None,  # Run manually
)

order_book_name = 'usd_mxn'  # Replace with the desired order book name

process_order_book_task = PythonOperator(
    task_id='process_order_book_task',
    python_callable=run_script,
    op_args=[order_book_name],
    dag=dag
)
