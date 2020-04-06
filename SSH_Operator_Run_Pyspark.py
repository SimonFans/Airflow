# load the dependencie

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from datetime import date, timedelta
from datetime import datetime as dt
from airflow.contrib.operators.ssh_operator import SSHOperator


# Local directory where the data file ise
LOCAL_DIR='/home/simon'

# HDFS directory where the data file will be uploaded
HDFS_DIR='/user/simon'

# each DAG must have a unique identifier
DAG_ID = 'Spark_Query_Example'

# start_time is a datetime object to indicate
# at which time your DAG should start (can be either in the past or future)
DAG_START_DATE=airflow.utils.dates.days_ago(1)

# schedule interval is a timedelta object
# here our DAG will be run every day
DAG_SCHEDULE_INTERVAL='@daily'

# default_args are the default arguments applied to the DAG
# and all inherited tasks
DAG_DEFAULT_ARGS = {
	'owner': 'Simon',
	'depends_on_past': False,
	'start_date': DAG_START_DATE,
	'retries': 1,
	'retry_delay': timedelta(minutes=5)
}

yesterday = date.today() - timedelta(days=1)
dt = yesterday.strftime("%Y-%m-%d")


spark_script='../spark-2.4.0/bin/spark-submit ../spark_test.py'

with DAG(
	DAG_ID,
	default_args=DAG_DEFAULT_ARGS,
	schedule_interval=DAG_SCHEDULE_INTERVAL
	) as dag:

	run_spark_task = SSHOperator(ssh_conn_id='push_notify-seo2-ssh',task_id='run_spark_job',command=spark_script, xcom_push=True, dag=dag)	
	
  run_spark_task
  
  
