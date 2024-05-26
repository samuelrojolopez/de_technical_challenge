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
    script_path = '/opt/airflow/src/warehouse/warehouse_user_login_events_model.py'
    subprocess.run(['python3', script_path])    sys.path.insert(0, '/path/to/your/script')


dag = DAG(
    'warehouse_user_login_events_model',
    default_args=default_args,
    description='Warehouse User Login Events Table Model Pipeline',
    schedule_interval=timedelta(days=1),
)

user_login_events_model_task = PythonOperator(
    task_id='warehouse_user_login_events_model',
    python_callable=run_script,
    dag=dag,
)

