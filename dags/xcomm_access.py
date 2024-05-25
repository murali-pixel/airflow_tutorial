import time
from datetime import datetime,timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'aMurali'
}

def increment_by_1(counter):
    print("Count {counter}!".format (counter))
    return counter + 1

def multiply_by_100(ti):
    value=ti.xcom_pull(task_ids='increment_by_1')
    print("Multiply {value} by 100!".format (value))
    return value * 100

def subract_9(ti):
    value=ti.xcom_pull(task_ids='multiply_by_100')
    print("Subtract 9 from {value}!".format (value))
    return value - 9

def print_value(ti):
    value=ti.xcom_pull(task_ids='subract_9')
    print("The value is {value}!".format (value))


with DAG(
    dag_id='cross communication',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
) as dag:

    increment_by_1= PythonOperator(
        task_id='increment_by_1',
        python_callable=increment_by_1,
    )

    multiply_by_100= PythonOperator(
        task_id='multiply_by_100',
        python_callable=multiply_by_100,
    )

    subract_9= PythonOperator(
        task_id='subract_9',
        python_callable=subract_9,
    )

    print_value= PythonOperator(
        task_id='print_value',
        python_callable=print_value,
    )

increment_by_1 >> multiply_by_100 >> subract_9 >> print_value
    