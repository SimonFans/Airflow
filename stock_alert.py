from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import date, datetime, timedelta, time
from collections import defaultdict
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from pprint import pprint
from yahoo_fin import stock_info as si
from airflow.models import Variable
from airflow.operators.datetime import BranchDateTimeOperator
from airflow.operators.weekday import BranchDayOfWeekOperator
from airflow.utils.weekday import WeekDay
import json

default_args = {
    'owner': 'Ximeng',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 15),
    'email': [''],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=0.5)
}

dag = DAG('my_stock', default_args=default_args, schedule_interval= '*/30 * * * 1-5', catchup=False)

# Variables
dag_config = Variable.get("stock_file",deserialize_json=True)


# Methods
def get_my_stock_list(**kwargs):
    cleaned_lst = []
    ordered_stock_str = kwargs['ti'].xcom_pull(task_ids='read_variables')
    rm_parenthesis_str = ordered_stock_str.lstrip('{').rstrip('}')
    ordered_stock_lst = rm_parenthesis_str.split(',')
    for each_stock in ordered_stock_lst:
        stock_name, price = each_stock.split(':')[0].strip().replace("'", ""), float(each_stock.split(':')[1])
        cleaned_lst.append((stock_name, price))
    print(cleaned_lst)
    return cleaned_lst

def get_my_stock_name_list(**kwargs):
    pull_stock_list = kwargs['ti'].xcom_pull(task_ids='get_my_stock_list')
    ordered_stock_namelist = []
    for i in range(len(pull_stock_list)):
        ordered_stock_namelist.append(pull_stock_list[i][0])
    return ordered_stock_namelist

def parsePrice(stock_code):
	price = si.get_live_price(stock_code)
	return round(price,3)

def check_stock(**kwargs):
    #name_list=['Lyft', 'Alibaba' 'Airbnb', 'Coupang', 'Delta', 'Zillow', 'Apple', 'Uber']
    name_list = kwargs['ti'].xcom_pull(task_ids='get_my_stock_name_list')
    my_stock_lst = kwargs['ti'].xcom_pull(task_ids='get_my_stock_list')
    name=['LYFT', 'BABA', 'CPNG', 'Z','PYPL','AAPL','SNAP']
    consider_to_all = []
    for i in range(len(my_stock_lst)):
        my_stock_price = my_stock_lst[i][1]
        current_stock_price = parsePrice(name[i])
        # if my_stock_price <= current_stock_price:
        consider_to_all.append((my_stock_lst[i][0],my_stock_price,current_stock_price))
    print(consider_to_all)
    email_content = []
    email_key_name =  ["Stock_Name","Your_Purchased_$","Current_Stock_$"]
    test = []
    for val in consider_to_all:
        email_content.append(dict(zip(email_key_name, val)))
    email_content_json = json.dumps(email_content, indent=4)
    return email_content_json

def sell_stock(**kwargs):
    name_list = kwargs['ti'].xcom_pull(task_ids='get_my_stock_name_list')
    my_stock_lst = kwargs['ti'].xcom_pull(task_ids='get_my_stock_list')
    name=['LYFT', 'BABA', 'CPNG', 'Z','PYPL','AAPL','SNAP']
    consider_to_sell = []
    for i in range(len(my_stock_lst)):
        my_stock_price = my_stock_lst[i][1]
        current_stock_price = parsePrice(name[i])
        if my_stock_price <= current_stock_price:
            consider_to_sell.append(my_stock_lst[i][0])
    return consider_to_sell

is_in_timeframe = BranchDateTimeOperator(
        task_id = "is_in_timeframe",
        follow_task_ids_if_true = ['read_variables'],
        follow_task_ids_if_false = ['not_in_timeframe'],
        # If you set time lower > upper, such as lower: 1Pm, upper: 11:00 AM, then 1PM means the next day 1PM, so timeframe becomes today 11AM - 1:00PM (tomorrow)
        target_lower = time(14,0,0),
        target_upper = time(21,0,0),
        # By default, use_task_execution_date is False. But if you want to run past dags, you have to set it to True
        use_task_execution_date = True,
        dag=dag
        )

is_weekday = BranchDayOfWeekOperator(
        task_id = "is_weekday",
        follow_task_ids_if_true = ['is_in_timeframe'],
        follow_task_ids_if_false = ['non_weekday_task'],
        week_day = {WeekDay.MONDAY, WeekDay.TUESDAY, WeekDay.WEDNESDAY, WeekDay.THURSDAY, WeekDay.FRIDAY},
        use_task_execution_day = True,
        dag=dag
        )

not_in_timeframe = DummyOperator(
        task_id = 'not_in_timeframe',
        dag=dag
        )

non_weekday_task = DummyOperator(
        task_id = 'non_weekday_task',
        dag=dag
        )

t2 = BashOperator(
    task_id="read_variables",
    bash_command='echo "{0}"'.format(dag_config),
    dag=dag
    )

t3 = PythonOperator(
    task_id='get_my_stock_list',
    python_callable=get_my_stock_list,
    dag=dag)

t4 = PythonOperator(
    task_id='get_my_stock_name_list',
    python_callable=get_my_stock_name_list,
    dag=dag)

t5 = PythonOperator(
    task_id='check_stock',
    python_callable=check_stock,
    dag=dag)

t6 = PythonOperator(
    task_id='sell_stock',
    python_callable=sell_stock,
    dag=dag)

send_email = EmailOperator(
        task_id = 'Send_Email',
        to = ['', ''],
        #
        subject = 'Airflow Alert: Stock Check!!',
        html_content = """
                        <p><mark>Current Stocks Market $: </mark></p>
                        {{task_instance.xcom_pull(task_ids = 'check_stock')}}<br>
                        <p><mark>You may consider to sell stock: </mark></p>
                        <p>{{task_instance.xcom_pull(task_ids = 'sell_stock')}}</p><br><br>
                        """,
        dag = dag
        )

is_weekday >> [is_in_timeframe, non_weekday_task]
is_in_timeframe >> [t2, not_in_timeframe]
t2 >> t3 >> t4 >> t5 >> t6 >> send_email
