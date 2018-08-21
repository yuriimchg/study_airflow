from datetime import datetime, timedelta
import random
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def set_interval():
    path_to_file = # set_your_path
    interval = random.choice(['@hourly',
                            '* * * * *',
                            '*/11 * * * *',
                            '12 * * * *',
                            '@daily',
                            timedelta(minutes=13),
                            timedelta(minutes=7),
                            timedelta(minutes=14)
                            ])
    with open(path_to_file, 'w') as f:
        f.write(str(interval))

default_args = {
                'owner':'someone',
                'depends_on_past':False,
                'start_date':datetime(2018,1,1),
                'retries':3,
                'execution_timeout':timedelta(50)
            }

dag_0 = DAG('interval_resetter', default_args=default_args, catchup=False, schedule_interval='* * * * *')

task_4 = PythonOperator(task_id='reset_interval', python_callable=set_interval, dag=dag_0)

task_4
