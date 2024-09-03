from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 1),
}

with DAG('dollar_checker_dag', default_args=default_args, schedule_interval='@daily') as dag:
    run_script = BashOperator(
        task_id='run_script',
        bash_command='python /app/entrega1-variacion-dolar.py',
    )
