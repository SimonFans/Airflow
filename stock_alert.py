from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import date, datetime, timedelta
from collections import defaultdict
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from pprint import pprint
from yahoo_fin import stock_info as si
from airflow.models import Variable


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 15),
    'email': ['xxx@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=0.5)
}

dag = DAG('my_stock', default_args=default_args, schedule_interval= '0 */3 * * 1-5', catchup=False)

# Variables
dag_config = Variable.get("stock_file",deserialize_json=True)


# Methods
def get_my_stock_list(**kwargs):
    cleaned_lst = []
    ordered_stock_str = kwargs['ti'].xcom_pull(task_ids='read_variables')
    rm_parenthesis_str = ordered_stock_str.lstrip('{').rstrip('}')
    ordered_stock_lst = rm_parenthesis_str.split(',')
    for each_stock in ordered_stock_lst:
        stock_name, price = each_stock.split(':')[0].strip(), float(each_stock.split(':')[1])
        cleaned_lst.append((stock_name, price))
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
    name=['LYFT', 'BABA', 'ABNB', 'CPNG', 'DAL', 'Z', 'AAPL', 'UBER']
    consider_to_sell = []
    for i in range(len(my_stock_lst)):
        my_stock_price = my_stock_lst[i][1]
        current_stock_price = parsePrice(name[i])
        if my_stock_price <= current_stock_price:
            consider_to_sell.append((my_stock_lst[i][0],my_stock_price,current_stock_price))
    email_content = []
    email_key_name =  ['Stock_name','Your_purchased_price','Current_stock_price']
    for val in consider_to_sell:
        email_content.append(dict(zip(email_key_name, val)))
    return email_content


t1 = BashOperator(
    task_id='Start',
    bash_command='echo "Start checking my ordered stock....."',
    dag=dag)

t2 = BashOperator(
    task_id="read_variables",
    bash_command='echo "{0}"'.format(dag_config),
    dag=dag,
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

send_email = EmailOperator(
        task_id = 'Send_Email',
        to = ['xxx@gmail.com'],
        subject = 'Airflow: Ximeng Stock Check!!',
        html_content = """
                        <p>You may consider to sell: </p>
                        <p>{{task_instance.xcom_pull(task_ids = 'check_stock')}}<br><br>
                        """,
        dag = dag
        )

t1 >> t2 >> t3 >> t4 >> t5 >> send_email
