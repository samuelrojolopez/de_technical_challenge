import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import subprocess

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 5, 24),
    'schedule_interval': '@daily',
}


def run_script():
    script_path = '/opt/airflow/src/warehouse/warehouse_movements_model.py'

    subprocess.run(
        ['python', script_path],
        check=True,
        capture_output=True,
        text=True
    )


dag = DAG(
    'warehouse_movements_model',
    default_args=default_args,
    description='Warehouse Movements Table Model Pipeline',
    schedule_interval=timedelta(days=1),
    catchup=False
)

movements_model_task = PythonOperator(
    task_id='warehouse_movements_model',
    python_callable=run_script,
    dag=dag,
)

