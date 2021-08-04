from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

# depends_on_past: (boolean) when set to True, keeps a task from getting triggered if the previous schedule for the task hasn’t succeeded.
# Never make start date to be current date. It won't run the dag because the dag will trigger after (start_date + schedule_interval). You can use days_ago to set up the start_date
# retry_exponential_backoff use case is multiple calls to one API. It's based on an algorithm to extend time for each retry delay

default_args = {
  'owner': 'airflow',
  'start_date': datetime(2020,1,30),
  # 'start_date': days_ago(5),
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
