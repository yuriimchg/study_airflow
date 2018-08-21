from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from nbu_parser import NBUParser
from datetime import datetime, timedelta

default_args = {
        'owner':'yurii',
        'depends_on_past':False,
        'start_date':datetime(2018,1,1),
        'email': ['airflow@example.com'],
        'email_on_failure': ['yurii.machuga@ralabs.org'],
        'retries':2
    }

dag_nbu = DAG('nbu_daily_exchange_rate',
        default_args=default_args,
        catchup=True,
        schedule_interval='@daily',
        max_active_runs=1)

task_1 = PythonOperator(task_id='do_everything', python_callable = NBUParser().do_it, dag=dag_nbu)
task_2 = PythonOperator(task_id='sleeping_bastard', python_callable=NBUParser().sleeping_bastard, dag=dag_nbu)

task_1 >> task_2
