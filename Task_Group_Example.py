from airflow.models import DAG, Variable
from airflow.operators.bash import BashOperator
from datetime import date, datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from task_group import training_groups

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 16),
    'email': ['xxx@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=0.5)
}

with DAG('parent_dag', default_args=default_args, schedule_interval= '@daily', catchup=False) as dag:

    start = BashOperator(task_id = "start", bash_command = "echo 'start'")

    group_training_task = training_groups()

    end = BashOperator(task_id = "end", bash_command = "echo 'end'")

    start >> group_training_task >> end
