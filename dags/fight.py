from datetime import datetime, timedelta
import random
from airflow import DAG
import psycopg2
from airflow.operators.python_operator import PythonOperator

def dt_printer():
    print(datetime.now())

def save_to_file():
    path = '/home/yurii/airflow/dags/csv/scores.txt'
    f = open(path, 'a')
    f.write(str(datetime.now())+'\n')
    f.close()

with open('/home/yurii/airflow/dags/csv/interval.txt', 'r') as f:
    interval = f.read()

def write_to_db():
    conn = psycopg2.connect(database='exchange_rate', user='yurii', password='yurii', host='localhost')
    cursor = conn.cursor()
    sql_command = """
    INSERT INTO dynamical_interval (timestamp, interval)
    VALUES (%s, %s);
    """
    data = [datetime.now(), interval]
    cursor.execute(sql_command, data)
    conn.commit()
    cursor.close()

default_args = {
                'owner':'someone',
                'depends_on_past':False,
                'start_date':datetime(2018,1,1),
                'retries':3,
                'execution_timeout':timedelta(50)
            }

dag = DAG('dynamical_schedule', default_args=default_args, catchup=True, schedule_interval=f'{interval}')

#task_1 = PythonOperator(task_id='reset_scores', python_callable=init_scores, dag=dag)
task_2 = PythonOperator(task_id='dt', python_callable=dt_printer, dag=dag)
task_3 = PythonOperator(task_id='save_results', python_callable=save_to_file, dag=dag)
task_4 = PythonOperator(task_id='insert_to_db', python_callable=write_to_db, trigger_rule='all_done', dag=dag)

task_4 >> task_2 >> task_3
