from dags.currency_archive import Archive
from datetime import datetime, timedelta

def update_table(k):
    """
    Connect to the PostgreSQL database
    """
    conn = None
    updated_rows = 0
    # connect to the PostgreSQL server
#    print('Connecting to the server')
    #conn = psycopg2.connect(database='exchange_rate', user='yurii', password='yurii', host='localhost')

    # create a cursor
    #cur = conn.cursor()

    # Get the date
    #cur.execute('SELECT COUNT(DISTINCT day) FROM currencies_daily')

    days_from_initial = k
    # The proper date for the URL
    day = Archive().new_day(days_from_initial)

    print(day)
    # PrivatBank API data
    table = Archive().to_table(days_from_initial)
    base_ccy, ccy, sell, buy = [[row[i] for row in table] for i in range(len(table[0]))]
    for i in range(len(table)):
        print(table[i])
    # Execute an SQL statement


for i in range(20):
    print(update_table(i))
