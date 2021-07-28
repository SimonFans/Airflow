from airflow import DAG
from datetime import datetime, timedelta

# depends_on_past: (boolean) when set to True, keeps a task from getting triggered if the previous schedule for the task hasn’t succeeded.

default_args = {
  'owner': 'airflow',
  'start_date': datetime(2020,1,30),
  'email': ['xxx@gmail.com'],
  'email_on_failure': True,
  'email_on_retry': True,
  'retry_exponential_backoff': True,
  'retry_delay' = timedelta(seconds=300),
  'retries': 3
  'depend_on_past': False,
  'sla': timedelta(seconds=30)
}

with DAG('My dag name',
         default_args = default_args,
         schedule_interval = '@daily',
         catchup = False
        ) as dag:
