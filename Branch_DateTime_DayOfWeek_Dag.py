'''
BranchDateTimeOperator:
Set a timeframe to run some specific tasks

BranchDayOfWeekOperator:
Set a list of days to run some specific tasks
'''

from airflow.models import DAG
from airflow.operators.datetime import BranchDateTimeOperator
from airflow.operators.weekday import BranchDayOfWeekOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import date, datetime, timedelta, time
from airflow.utils.weekday import WeekDay

default_args = {
    'owner': 'Ximeng',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 13),
    'email': ['xxx@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=0.5)
}

with DAG('branch_datetime_dag', default_args=default_args, schedule_interval= '@daily', catchup=False) as dag:
    is_in_timeframe = BranchDateTimeOperator(
        task_id = "is_in_timeframe",
        follow_task_ids_if_true = ['move_forward'],
        follow_task_ids_if_false = ['end'],
        # If you set time lower > upper, such as lower: 1Pm, upper: 11:00 AM, then 1PM means the next day 1PM, so timeframe becomes today 11AM - 1:00PM (tomorrow)
        target_lower = time(18,0,0),
        target_upper = time(19,0,0),
        # By default, use_task_execution_date is False. But if you want to run past dags, you have to set it to True
        use_task_execution_date = True
    )

    is_weekend = BranchDayOfWeekOperator(
        task_id = "is_weekend",
        follow_task_ids_if_true = ['weekend_task'],
        follow_task_ids_if_false = ['non_weekend_task'],
        week_day = {WeekDay.SATURDAY, WeekDay.SUNDAY},
        use_task_execution_day = True
    )

    move_forward = DummyOperator(
        task_id = 'move_forward'
    )

    end = DummyOperator(
        task_id = 'end'
    )

    weekend_task = DummyOperator(
        task_id = 'weekend_task'
    )

    non_weekend_task = DummyOperator(
        task_id = 'non_weekend_task'
    )

    is_in_timeframe >> [move_forward, end]
    move_forward >> is_weekend >> [weekend_task, non_weekend_task]
    
    
