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

def multiply_by_100(counter):
    print(" Count{counter}!".format (counter))
    return counter * 100

with DAG(
    dag_id='cross communication',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
) as dag:

    t1 = PythonOperator(
        task_id='increment_by_1',
        python_callable=increment_by_1,
        op_kwargs={'counter': 100}
    )

    t2 = PythonOperator(
        task_id='multiply_by_100',
        python_callable=multiply_by_100,
        op_kwargs={'counter': 100}
    )

t1 >> t2
    