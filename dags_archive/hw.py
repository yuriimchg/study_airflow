from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
import os

py_clock = lambda: 'Now is {0}:{1:02}:{2:02}'.format(datetime.now().hour,datetime.now().minute, datetime.now().second)

def write_time(path, filename):
    f = open(os.path.join(path, filename), 'a')
    f.write(f'Time: {datetime.now().hour}:{datetime.now().minute:02}:{datetime.now().second:02}\n')
    f.close()

default_args = {'owner': 'airflow',
                'retries':1,
                'depends_on_past': False,
                'retry_delay':timedelta(seconds=15),
                #'end_date':datetime(2018,8,3),
                'email': ['airflow@example.com'],
                'email_on_failure': False,
                'email_on_retry': False,}

dag = DAG('dag_id_123', description='Another new attempt',
            default_args=default_args,
            schedule_interval='@daily',
            catchup=True,
            start_date=datetime(2018, 4, 2))

def print_context(ds, **kwargs):
    print(ds)
    return 'Everything here is printed to logs'

run_this = PythonOperator(
    task_id='print_the_context',
    provide_context=True,
    python_callable=print_context,
    dag=dag)

time_operator = PythonOperator(task_id='time_task',
                                python_callable=py_clock,
                                provide_context=False,
                                dag=dag)

write_operator = PythonOperator(task_id='write_task',
                                python_callable=write_time,
                                provide_context=False,
                                op_kwargs={'path':'/home/yurii/Desktop', 'filename':'time2.txt'},
                                dag=dag)

run_this >> write_operator >> time_operator
