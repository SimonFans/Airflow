# HDFS_Consumer_Relevance => conn id, needs to set host (server name) and port number (8020)

from __future__ import print_function
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow import settings
from airflow.hooks.hdfs_hook import HDFSHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.sensors.hdfs_sensor import HdfsSensor
from datetime import datetime
from datetime import timedelta
from airflow.models import Variable

default_args = {
    'owner': 'Simon',
    'start_date': datetime(2019, 8, 4),
    'end_date': datetime(2019, 8, 15),
    'email': ['x@.com'],
    'email_on_failure': True,
    'email_on_retry': False    
}

dag = DAG('hdfs_sensor_example', 
    schedule_interval="@once", 
    default_args=default_args)

t=(datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
# Config variables
## Common
# var1 = "value1"
# var2 = [1, 2, 3]
# var3 = {'k': 'value3'}

## 3 DB connections called
# var1 = Variable.get("var1")
# var2 = Variable.get("var2")
# var3 = Variable.get("var3")

## Recommended way

#dag_config = Variable.get("example_variables_config", deserialize_json=True)
#var1 = dag_config["var1"]
#var2 = dag_config["var2"]
#var3 = dag_config["var3"]

start = DummyOperator(
    task_id="Start",
    dag=dag
)

# To test this task, run this command:
t1 = HdfsSensor(
    task_id="hdfs_sensor_AE",
    filepath='/...../eventdate='+t+'/datasource=email/countrycode=AE',
    hdfs_conn_id='HDFS_Consumer_Relevance',
    dag=dag,
)

start >> t1

