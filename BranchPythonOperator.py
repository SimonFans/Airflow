# BranchPythonOperator returns task id to decide which one goes next.  

from airflow.models import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import date, datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 9),
    'email': ['xxx@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=0.5)
}

def _check_accuracy():
    accuracy = 1.6
    if accuracy > 1:
        return ['accurate', 'top_accurate']
    return 'inaccurate'

with DAG('ml_branch_dag', default_args=default_args, schedule_interval= '@daily', catchup=False) as dag:
    training_ml = DummyOperator(task_id = "training_ml")

    check_accuracy = BranchPythonOperator(
        task_id = "check_accuracy",
        python_callable=_check_accuracy
    )

    accurate = DummyOperator(task_id = "accurate")
    inaccurate = DummyOperator(task_id = "inaccurate")
    top_accurate = DummyOperator(task_id = "top_accurate")

    publish_ml = DummyOperator(task_id = "publish_ml", trigger_rule = "one_success")

    training_ml >> check_accuracy >> [accurate, inaccurate,top_accurate] >> publish_ml
