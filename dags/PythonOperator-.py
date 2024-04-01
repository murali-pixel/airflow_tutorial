from datetime import datetime,timedelta
from airflow.utils.dates import days_ago
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator



default_args = {
    'owner': 'Murali'
}

def task_A():
    print("Task A")

def task_B():
    time.sleep(5)
    print("Task B")

def task_C():
    print("Task C")

def task_D():
    print("Task D")


with DAG(
    dag_id="Hello",
    description="Hi!Tis is my first Dag in Airflow",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval= '@daily'
) as dag:
    task1 = PythonOperator(
        task_id='task_A',
        python_callable=task_A
    )

    task2 = PythonOperator(
        task_id='task_B',
        python_callable=task_B
    )

    task3 = PythonOperator(
        task_id='task_C',
        python_callable=task_C
    )

    task4 = PythonOperator(
        task_id='task_D',
        python_callable=task_D
    )

task1 >> [task2, task3] >> task4

