from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
import os
from datetime import datetime, timedelta

default_args = {
    'owner':'babaj',
    'retries': 1,
    'email_on_retry':'yurii.machuga@ralabs.org',
    'email_on_failure':'yurii.machuga@ralabs.org',
    'depends_on_past':False,
    'start_date':datetime(2009,12,1)}

dag = DAG('all_success_dag', default_args=default_args, catchup=True, schedule_interval='13 * 3 * 3')

def func_1(path, filename):
    f = open(os.path.join(path, filename), 'a')
    f.write(f'Time: {datetime.now().hour}:{datetime.now().minute:02}:{datetime.now().second:02}\n')
    f.close()

func_2 = lambda: print(2 + 2)
func_3 = lambda: print(23 + 2)
func_4 = lambda: print(22 + 2)
func_5 = lambda: print(2 + 12)
func_6 = lambda: print(2 + 21)
func_7 = lambda: print(2 + 43)
func_8 = lambda: print(223 + 2)
func_9 = lambda: print(21 + 4)
func_10 = lambda: print(2 + 2)
func_11 = lambda: print(2 + 3)

run_first = PythonOperator(
    task_id='first_task',
    python_callable=func_1,
    op_kwargs={'path':'/home/yurii/Desktop/new', 'filename':'all_succeed.txt'},
    dag=dag)

run_second = PythonOperator(task_id='second_task',
    python_callable=func_2,
    dag=dag)

run_third = PythonOperator(task_id='third_task', python_callable = func_10, trigger_rule='one_success', dag=dag)
#run_fourth = PythonOperator(task_id='fourth_task', python_callable = func_3, dag=dag)
run_fifth = PythonOperator(task_id='fifth_task', python_callable = func_4, dag=dag)
run_sixth = PythonOperator(task_id='sixth_task', python_callable = func_5, dag=dag)
run_seventh = PythonOperator(task_id='seventh_task', python_callable = func_6, dag=dag)
run_eighth = PythonOperator(task_id='eighth_task', python_callable = func_7, dag=dag)
run_ninth = PythonOperator(task_id='ninth_task', python_callable = func_8, dag=dag)
run_tenth = PythonOperator(task_id='tenth_task', python_callable = func_9, dag=dag)
run_eleventh = PythonOperator(task_id='eleventh_task', python_callable = func_11, trigger_rule='all_success', dag=dag)

run_first >> run_second >> run_third
run_first >> run_sixth >> run_eleventh
run_sixth << run_third >> run_fifth >> run_seventh >> run_eleventh
run_fifth >> run_ninth >> run_tenth
run_third >> run_eighth
