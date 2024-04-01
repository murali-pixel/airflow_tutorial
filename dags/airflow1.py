from datetime import datetime,timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Murali'
}
with DAG(
    dag_id="Hello",
    description="Hi!Tis is my first Dag in Airflow",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval= None
) as dag:
    task1 = BashOperator(
        task_id='hello_world',
        bash_command='echo Hello World!',
        dag=dag)

task1