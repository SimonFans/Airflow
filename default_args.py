from airflow import DAG
from datetime import datetime, timedelta

default_args = {
  'owner': 'airflow',
  'start_date': datetime(2020,1,30),
  'email': ['ximengzhao1220@gmail.com'],
  'email_on_failure': True,
  'email_on_retry': True,
  'retry_exponential_backoff': True,
  'retry_delay' = timedelta(seconds=300),
  'retries': 3
  'depend_on_past': False
}

with DAG('My dag name',
         default_args = default_args,
         schedule_interval = '@daily',
         catchup = False
        ) as dag:
