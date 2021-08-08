from airflow.models import DAG, Variable
from airflow.operators.bash import BashOperator
from datetime import date, datetime, timedelta
from collections import defaultdict
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from pprint import pprint
# import task decorator then you don't need to have PythonOperator
from airflow.decorators import task
# Below is for import ti when you use task decorator
from airflow.operators.python import get_current_context

# In the UI variable, define path, and filename. As for my_settings, fill val in {"path": "/opt/local/airflow", "filename": "test.json"}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 6),
    'email': ['ximengzhao1220@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=0.5)
}

def _process_one(path, filename):
    print(f"{path}/{filename}")

def _process_two(path, filename):
    print(f"{path}/{filename}")

@task(task_id="task_c")
def _process_three(my_settings):
    context = get_current_context()
    print(f"{my_settings['path']}/{my_settings['filename']}-{context['ds']}")

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

    store = BashOperator(
        task_id="store",
        bash_command="echo 'store'"
    )

    t1 >> t2 >> _process_three(Variable.get("my_settings",deserialize_json=True)) >> store
