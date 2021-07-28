from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.email import EmailOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.utils.trigger_rule import TriggerRule

value_1 = [1, 2, 3]
value_2 = {'a': 'b'}


def push(**kwargs):
    """Pushes an XCom without a specific target"""
    kwargs['ti'].xcom_push(key='value from pusher 1', value=value_1)
    kwargs['ti'].xcom_push(key='value from pusher 2', value=value_2)


def puller(**kwargs):
    """Pull all previously pushed XComs and check if the pushed values match the pulled values."""
    val_1 = kwargs['ti'].xcom_pull(key='value from pusher 1', task_ids='push')
    val_2 = kwargs['ti'].xcom_pull(key='value from pusher 2', task_ids='push')
    print('I received the first value is an array: ', val_1)
    print('%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%')
    print('I received the second value is a dictionary: ', val_2)

    print("Let's check if they are equal")

    if val_1 == value_1:
        print('First value equals')

    if val_2 == value_2:
        print(f'The second two values equals {val_2} and {value_2}')

def custom_success_function(context):
    print('Let me show you the context below')
    print(context)
    print('---------------------------')
    dag_run = context['dag_run']
    task_instances = context['task_instance']
    print(dag_run)
    print('Task instance success: ', task_instances)

def custom_failure_function(context):
    print('Let me show you the context below')
    print(context)
    print('---------------------------')
    dag_run = context['dag_run']
    task_instances = context['task_instance']
    print(dag_run)
    print('Task instance failed: ', task_instances)

with DAG(
    'Simon_XCOM_LEARN',
    schedule_interval="@once",
    start_date=days_ago(2),
    default_args={'owner': 'Simon', "email": "xxx0@gmail.com"},
    tags=['XCOM'],
) as dag:

    my_push = PythonOperator(
        task_id='push',
        python_callable=push,
        )


    pull = PythonOperator(
        task_id='puller',
        python_callable=puller,
        )

    success_task = DummyOperator(
        task_id = 'success_task',
        trigger_rule=TriggerRule.ALL_SUCCESS,
        on_success_callback = custom_success_function,

    )

    failure_task = DummyOperator(
        task_id = 'failure_task',
        trigger_rule=TriggerRule.ALL_FAILED,
        on_success_callback = custom_failure_function
    )

    send_email = EmailOperator(
        task_id="send_email",
        to=["xxx@gmail.com"],
        subject="Hello from Simon's Airflow",
        html_content="<h3>Hi, Simon sent you an email via Airflow!!</h3>",
        trigger_rule=TriggerRule.ONE_SUCCESS
        )


    my_push >> pull >> [success_task, failure_task] >> send_email
