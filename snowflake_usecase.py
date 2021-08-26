from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.email import EmailOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.utils.trigger_rule import TriggerRule
import time
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def count_records(**context):
    dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    result = dwh_hook.get_first("select count(*) from <DB>.<schema>.<table_name>")
    logging.info("Number of rows in `<schema>.<table_name>` - %s", result[0])

with DAG(
    'XM_SNOWFLAKE',
    schedule_interval="@once",
    start_date=days_ago(2),
    default_args={'owner': 'Ximeng', "email": "xxx@gmail.com"},
    tags=['Snowflake']
) as dag:

    start = DummyOperator(
        task_id = 'start'
    )

    duplicate_table= SnowflakeOperator(
        task_id="duplicate_table",
        sql="sql/snowflake_test_query.sql",
        snowflake_conn_id="snowflake_conn"
    )

    count_query = PythonOperator(
        task_id="count_query",
        python_callable=count_records
    )

    end = DummyOperator(
        task_id = 'end'
    )

    start >> duplicate_table >> count_query >> end
