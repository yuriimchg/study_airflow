from get_currency import ExchangeRate
import psycopg2
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
#from airflow.operators.dummy_operator import DummyOperator

currency_list = ('usd', 'eur', 'plz', 'czk', 'gbp')
def update_table(currency):
    """
    Connect to the PostgreSQL database
    """
    conn = None
    updated_rows = 0
    now = datetime.now()
    sql = """
INSERT INTO {0} (id, datetime, buy, sell)
VALUES (0, %s, %s, %s);
""".format(currency)
    data = (str(now), float(ExchangeRate(currency).buy()), float(ExchangeRate(currency).sell()))
    try:
        # connect to the PostgreSQL server
        print('Trying to connect to the server')
        conn = psycopg2.connect(database='exchange_rate', user='yurii', password='yurii', host='localhost')

        # create a cursor
        cur = conn.cursor()

        # Execute a statement
        cur.execute(sql, data)
        updated_rows = cur.rowcount

        # Commit changes to the DB
        conn.commit()

        # Close a communication with SQL
        cur.close()
        print('Closed communication with database')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Closed Database connection')
def total_run():
    for currency in currency_list:
        update_table(currency)


default_args = {
'owner': 'yurii',
'depends_on_past': False,
'start_date': datetime(2018, 6, 1),
'email': ['airflow@example.com'],
'email_on_failure': ['yurii.machuga@ralabs.org'],
'email_on_retry': False,
'retries': 1,
}

money_dag = DAG('exchange_rate', default_args=default_args, schedule_interval='@hourly')

t1 = PythonOperator(task_id='do_all', python_callable=total_run, dag=money_dag)
#t0 = DummyOperator(task_id='do_nothing', dag=money_dag)
t1
