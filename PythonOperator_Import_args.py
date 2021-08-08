from airflow.models import DAG, Variable
from airflow.operators.bash import BashOperator
from datetime import date, datetime, timedelta
from collections import defaultdict
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from pprint import pprint

# In the UI variable, define path, and filename. As for my_settings, fill val in {"path": "/opt/local/airflow", "filename": "test.json"}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 6),
    'email': ['xxx@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=0.5)
}

def _process_one(path, filename):
    print(f"{path}/{filename}")

def _process_two(path, filename):
    print(f"{path}/{filename}")

with DAG('my_python_dag', default_args=default_args, schedule_interval= '@daily', catchup=False) as dag:
    # (1) Method 1 to import args
    t1 = PythonOperator(
    task_id='task_a',
    python_callable=_process_one,
    op_kwargs={
        'path':'{{var.value.path}}',
        'filename':'{{var.value.filename}}'
        }
    )
    # (2) Method 2 to import args, better than Method 1 because you just need to define one Variable instead of two
    t2 = PythonOperator(
    task_id='task_b',
    python_callable=_process_two,
    op_kwargs=Variable.get("my_settings",deserialize_json=True)
    )

    t1>>t2
