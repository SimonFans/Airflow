'''
Use S3Hook to list keys in the S3 bucket.
Check if the keyname datetime is current to make sure new file arrives on time.
'''

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.hooks.S3_hook import S3Hook
from datetime import date, datetime, timedelta
from collections import defaultdict
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from pprint import pprint

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 7, 19),
    'email': ['xxx@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=0.5)
}

dag = DAG('s3_dag_test', default_args=default_args, schedule_interval= '@daily', catchup=False)

# Variables
dt = (datetime.now() + timedelta(days=-1)).strftime("%Y-%m-%d")
#bucket_name = 'ssdp-s3-csai-dsde-dev'
bucket_name = 'gainsight-csg-prod-us-west-2-bucket'
most_recent_files = defaultdict(set)
most_recent_files_lst = []
#key_name = 'GainsightActivityAttendee-'+ dt +'-*'



# Methods
def time_to_epoch(item):
    temp_lst = []
    for sub_item in item:
        utc_time = datetime.strptime(sub_item, "%Y-%m-%d-%H:%M:%S")
        epoch_time = (utc_time - datetime(1970, 1, 1)).total_seconds()
        temp_lst.append((epoch_time, sub_item))
    temp_lst.sort(key = lambda x: x[0], reverse = True)
    pprint(temp_lst)
    return temp_lst[0][1]

def printFilesName():
    s3 = S3Hook(aws_conn_id='aws_prod_conn')
    s3.get_conn()
    lst_files = s3.list_keys(bucket_name = bucket_name, prefix = 'gainsight_1.0/Gainsight')
    for sub_lst in lst_files:
        idx = sub_lst.index('-')
        key, val =sub_lst[:idx], sub_lst[idx+1:len(sub_lst)-4]
        most_recent_files[key].add(val)
    print("\n".join("{!r}: {!r}".format(k, v) for k, v in most_recent_files.items()))
    #sorted_lst = {k: list(v) for k, v in sorted(most_recent_files.items(), key=lambda item: time_to_epoch(item[1]), reverse=True)}
    print('#####################')
    for k,v in most_recent_files.items():
        combine_filename = k + '-' + time_to_epoch(v) + '.tsv'
        most_recent_files_lst.append(combine_filename)
    return most_recent_files_lst

def getFiles(**kwargs):
    s3 = S3Hook(aws_conn_id='aws_prod_conn')
    s3.get_conn()
    lst_files = kwargs['ti'].xcom_pull(task_ids='print_file_name')
    res_map = {}
    for sub_lst in lst_files:
        idx = sub_lst.index('-')
        key, val = sub_lst[14:idx], sub_lst[idx+1:]
        if val[:10] == dt:
            res_map[key] = 'Normal'
        else:
            res_map[key] = 'Miss'
    return "\n".join("{!r}: {!r}".format(k, v) for k, v in res_map.items())

t1 = BashOperator(
    task_id='Run_Bash',
    bash_command='echo "We are going to detect S3 objects....... "',
    dag=dag)

t2 = PythonOperator(
    task_id='print_file_name',
    python_callable=printFilesName,
    dag=dag)

t3 = PythonOperator(
    task_id='list_file_status',
    python_callable=getFiles,
    dag=dag)

send_email = EmailOperator(
        task_id = 'Send_Email',
        to = 'xxx@gmail.com',
        subject = 'Airflow: Gainsight Files!!',
        html_content = """<p>{{task_instance.xcom_pull(task_ids = 'print_file_name')}}<br><br>
                        {{task_instance.xcom_pull(task_ids = 'list_file_status')}}</p>""",
        dag = dag
        )

'''
sensor = S3KeySensor(
        task_id='check_s3_for_file_in_s3',
        #bucket_key='GainsightActivityAttendee-*',
        bucket_key=key_name,
        wildcard_match=True,
        bucket_name='ssdp-s3-csai-dsde-dev',
        aws_conn_id='my_conn_S3',
        timeout=0.5*20,
        poke_interval=5,
        dag=dag)
'''

t1 >> t2 >> t3 >> send_email
