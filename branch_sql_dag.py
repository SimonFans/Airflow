'''
BranchSQLOperator: Based on returned sql result, either true or false, >0, ='off', such kind of conditions, it will implement the next task id.
Airflow UI -> Connection -> Host: postgres, login: airlfow, password: airlfow, port: 5432
'''

from airflow.models import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.sql import BranchSQLOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import date, datetime, timedelta

default_args = {
    'owner': 'Ximeng',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 13),
    'email': ['xxx@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=0.5)
}

with DAG('branch_sql_dag', default_args=default_args, schedule_interval= '@daily', catchup=False) as dag:
    create_table = PostgresOperator(
        task_id = "create_table",
        postgres_conn_id = "postgres",
        sql = "sql/create_table_users.sql"
    )

    insert_data = PostgresOperator(
        task_id = "insert_data",
        postgres_conn_id = "postgres",
        sql = "sql/insert_into_users.sql"
    )

    choose_task = BranchSQLOperator(
        task_id = "choose_task",
        sql = "SELECT COUNT(1) FROM users where user_status = TRUE",
        follow_task_ids_if_true = ['process'],
        follow_task_ids_if_false = ['send_email', 'send_slack'],
        conn_id = 'postgres'
        #database = 'airflow'
    )

    process = DummyOperator(
        task_id = 'process'
    )

    send_email = DummyOperator(
        task_id = 'send_email'
    )

    send_slack = DummyOperator(
        task_id = 'send_slack'
    )

    create_table >> insert_data >> choose_task >> [process, send_email, send_slack]
