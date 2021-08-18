from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

def training_groups():
    with TaskGroup("training_tasks") as training_tasks:
        training_a = BashOperator(task_id = "training_a", bash_command = "echo 'training_a'")
        training_b = BashOperator(task_id = "training_b", bash_command = "echo 'training_b'")
        training_c = BashOperator(task_id = "training_c", bash_command = "echo 'training_c'")

        with TaskGroup("publish_tasks") as publish_tasks:
            publish_a = BashOperator(task_id = "publish_a", bash_command = "echo 'publish_a'")
            publish_b = BashOperator(task_id = "publish_b", bash_command = "echo 'publish_b'")
            publish_c = BashOperator(task_id = "publish_c", bash_command = "echo 'publish_c'")

    return training_tasks
