from currency_archive import Archive
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG

default_args = {
'owner': 'yurii',
'depends_on_past': False,
'start_date': datetime(2018, 1, 1),
'email': ['airflow@example.com'],
'email_on_failure': ['yurii.machuga@ralabs.org'],
'email_on_retry': False,
'retries': 1,
}

daily_currency_dag = DAG('daily_currency', default_args=default_args, catchup=True, schedule_interval='@daily', max_active_runs=1)

t1 = PythonOperator(task_id='get_data_from_api', python_callable=(Archive().to_csv), dag=daily_currency_dag)
t2 = PythonOperator(task_id='store_data_at_db', python_callable=(Archive().update_table), dag=daily_currency_dag)
t1 >> t2
