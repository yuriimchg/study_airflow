from datetime import datetime, timedelta
import random
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

#  Editable configs
dag_to_configure = 'select_a_dag_to_manage'
    path_to_file = 'interval.txt'
def select_interval():
    interval = random.choice(['@hourly',
                            '*/2 * * * *',
                            '* * * * *',
                            '*/3 * * * *',
                            '*/12 * * * *',
                            '*/10 * * * *',
                            '* * * * 2',
                            '* * * * 3'
                            ])

    with open(path_to_file, 'w') as f:
        f.write(str(interval))

def new_configs():
    """
    A place for some new configs. Since there are no configs, here is a simulator
    """
    a = random.randint(1,159)
    assert a > 133
    return None

default_args = {
                'owner':'someone',
                'depends_on_past':False,
                'start_date':datetime(2017,11,11),
                'retries':0,
            }

dag_mngr = DAG('interval_resetter', default_args=default_args, catchup=False, schedule_interval='* * * * *', max_active_runs=1)

task_1 = PythonOperator(task_id='assert_new_configs', python_callable=new_configs, dag=dag_mngr)
task_2 = BashOperator(task_id='pause_dag', bash_command=f'airflow pause {dag_to_configure}', dag=dag_mngr)
task_1 >> task_2

task_3 = PythonOperator(task_id='reset_interval', python_callable=select_interval, trigger_rule='all_success', dag=dag_mngr)
task_3_5 = BashOperator(task_id='sleep_2_mins', bash_command='sleep 120', dag=dag_mngr)
task_4 = BashOperator(task_id='unpause_dag', bash_command=f'airflow unpause {dag_to_configure}', dag=dag_mngr)
task_2 >> task_3 >> task_3_5 >> task_4

task_5 = BashOperator(task_id='printer', bash_command='echo assertion failed', trigger_rule='one_failed', dag=dag_mngr)
task_1 >> task_5
