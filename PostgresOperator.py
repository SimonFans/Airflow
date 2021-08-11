# How to import parameters into the PostgreSQL

from airflow.models import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import date, datetime, timedelta
from airflow.operators.python import PythonOperator

class CustomPostgresOperator(PostgresOperator):
    template_fields = ('sql', 'parameters')

# Variables
dt = (datetime.now() + timedelta(days=0)).strftime("%Y-%m-%d")

def _task():
    return dt

default_args = {
    'owner': 'Ximeng',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 9),
    'email': ['ximengzhao1220@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=0.5)
}

with DAG('my_postgres_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    create_table = PostgresOperator(
        task_id = "create_table",
        postgres_conn_id = "postgres",
        sql = "sql/create_my_table.sql"
    )

    my_task = PythonOperator(
        task_id = "my_task",
        python_callable = _task

    )

    store = CustomPostgresOperator(
        task_id = "store",
        postgres_conn_id = "postgres",
        sql = [
            "sql/insert_into_my_table.sql",
            "SELECT * FROM my_table"
        ],
        parameters = {
            'filename': '{{ti.xcom_pull(task_ids=["my_task"])[0]}}'
        }
    )

    create_table >> my_task >> store
    
    
