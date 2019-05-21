from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import os
from datetime import datetime, timedelta

counter = 0
interval_tuple = ('@once', '@hourly', '@daily', '@weekly', '@monthly', '* 16 * * 3')
default_args = {
    'owner':'Lys Mykyta',
    'retries': 1,
    'email_on_retry':'no_mail@nohost.org',
    'email_on_failure':'no_mail@nohost.org',
    'depends_on_past':False,
    'start_date':datetime(2009,12,1)}

dags = [DAG(f'this_is_{counter}_dag', default_args=default_args, catchup=True, schedule_interval=interval_tuple[counter]) for counter in range(len(interval_tuple))]
for interval in interval_tuple:
    def func_1(path, filename):
        f = open(os.path.join(path, filename), 'a')
        f.write(f'Time: {datetime.now().hour}:{datetime.now().minute:02}:{datetime.now().second:02}\n')
        f.close()

    func_2 = lambda: print(2 + 2)
    run_first = PythonOperator(
        task_id=f'first_task_{counter}',
        python_callable=func_1,
        op_kwargs={'path':'/home/yurii/Desktop', 'filename':f'{interval[1:]}.txt'},
        dag=dags[counter])
    run_second = PythonOperator(task_id=f'second_task_{counter}',
        python_callable=func_2,
        dag=dags[counter])

    run_first >> run_second
    counter += 1
