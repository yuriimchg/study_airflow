from datetime import datetime, timedelta
from set_interval import set_interval
import random
from airflow import DAG
import psycopg2
from airflow.operators.python_operator import PythonOperator

def init_scores():
    return [0] * random.randint(2,30)

def one_vs_all():
    scores = init_scores()
    for i in range(2000):
        random.seed(i)
        win = random.choice(scores)
        if win < len(scores):
            scores[win] += 1
        random.shuffle(scores)
    return f'player #{scores.index(max(scores))} has won with score {max(scores)}!'

def save_to_file():
    path = '/home/yurii/airflow/dags/csv/scores.txt'
    f = open(path, 'a')
    f.write(one_vs_all()+'\n')
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

task_1 = PythonOperator(task_id='reset_scores', python_callable=init_scores, dag=dag)
task_2 = PythonOperator(task_id='fight', python_callable=one_vs_all, dag=dag)
task_3 = PythonOperator(task_id='save_results', python_callable=save_to_file, dag=dag)
task_4 = PythonOperator(task_id='insert_to_db', python_callable=write_to_db, trigger_rule='all_done', dag=dag)

task_1 >> task_4 >> task_2 >> task_3
