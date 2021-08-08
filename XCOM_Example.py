'''
By default, return value of each operator will auto do xcom push. If you don't want to do xcom push for a task, then you can set do_xcom_push = False
{{ti.xcom_push(key='<>',value='<>')}}
xcom = ti.xcom_pull(task_ids = ['task1_id', 'task2_id', 'task3_id'],key = 'return_value'), key should be unique.
'''

from __future__ import print_function
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

# Define Dag Info

DAG_ID = 'XCOM_Example'
DAG_START_DATE=airflow.utils.dates.days_ago(1)
DAG_SCHEDULE_INTERVAL='0 23 * * *'

DAG_DEFAULT_ARGS = {
  'owner': 'Simon',
  'depends_on_past': False,
  'start_date': DAG_START_DATE,
  'email': ['xzhao@groupon.com'],
  'email_on_failure': True,
  'email_on_retry': False
}

value_1 = [1, 2, 3]


def generate_values(**kwargs):
    values = list(range(0, 10))
    return values

def manipulate_values(**kwargs):
    ti = kwargs['ti']
    v1 = ti.xcom_pull(key=None, task_ids='push_values')
    return [x / 2 for x in v1]

with DAG(
  DAG_ID,
  default_args=DAG_DEFAULT_ARGS,
  schedule_interval=DAG_SCHEDULE_INTERVAL
  ) as dag:
  
	start = DummyOperator(task_id="Start")
  
	t1 = PythonOperator(
        task_id='push_values',
        python_callable=generate_values,
        do_xcom_push=True,
        provide_context=True )

	t2 = PythonOperator(
        task_id='pull_values',
        python_callable=manipulate_values,
        do_xcom_push=True,
        provide_context=True )

	start >> t1 >> t2
