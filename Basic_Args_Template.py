from airflow import DAG 
from datetime import datatime, timedelta 

default_args = {
    "owner" : "airflow",
    "start_date" : datetime(2020,4,8),
    "depend_on_past" : False,
    "email_on_failure" : False,
    "email_on_retry" : False,
    "email" : "xxx@gmail.com",
    "retries" : 1,
    "retry_delay": timedelta(minutes=5)
}

dag_id="forex_data_pipeline"
schedule_interval="@daily"


with DAG(dag_id, schedule_interval, default_args=default_args, catchup=False) as dag:
    None
