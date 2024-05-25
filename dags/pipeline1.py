import time
from datetime import timedelta,datetime
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow import DAG
import pandas as pd

default_args={'owner':"mUrali"}

def read_csv_file():
    
        df=pd.read_csv("/Users/muralikrishnakancheti/Desktop/airflow_tutorial/insurance.csv")
        print(df)

        return df.to_json()

def remove_null_values(**kwargs):
      ti=kwargs['ti']
      json_data=ti.xcom_pull(task_ids='read_csv_file')
      df=pd.read_json(json_data)
      df=df.dropna()
      print(df)

      return df.to_json()
      
    
with DAG(
        dag_id="Pipeline one",
        description="Running python pipeline",
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@once'
) as dag:
    read_csv_file=PythonOperator(
          task_id=read_csv_file,
          python_callable=read_csv_file

    )

read_csv_file

       

