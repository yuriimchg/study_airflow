from datetime import timedelta, date
import xml.etree.ElementTree as ET
from requests import get
import pandas as pd
from sqlalchemy import create_engine
import os
import psycopg2
from time import sleep
    # Little things to improve the code:

#  __init__  +
# Replace url with host_url and home_url +
# Move days and host_url to __init__
# Use private methods +
# f-strings +
# extractor should return only one object +
# DAG file should contain DAGs only! +
# Use list.extend(data) instead of list.append(data) +-
# !!! Contain SQL queries, connection configurations, etc in files (csv, json) --
# Use select last day SQL query instead of distinct to save resources +
#



class Archive:
    def __init__(self):
        # Paths and SQL queries will be stored in the external files
        self.initial_day = date(2018,1,1)
        self.host_url = 'https://api.privatbank.ua/'
        self.api_url = 'p24api/exchange_rates?'
        self.path_to_csv = '~/airflow/dags/csv/daily_currency.csv'
        self.csv_folder = '~/airflow/dags/csv'
        with open(os.path.join(self.csv_folder, 'set_order.txt'), 'r') as sql_set_order_file:
            self.sql_set_order = sql_set_order_file.read()
        with open(os.path.join(self.csv_folder, 'get_date.txt'), 'r') as sql_get_date_file:
            self.sql_get_date = sql_get_date_file.read()

    def __upd_date(self):
        conn = psycopg2.connect(database='db', user='user', password='password', host='localhost')
        # Create a connection cursor
        cur = conn.cursor()
        # Get the date
        cur.execute(self.sql_get_date)
        # Get the last day in the table
        try:
            prev_day = cur.fetchall()[0][0]
        except IndexError:
            return self.initial_day
        # The proper date for the dynamical API URL
        cur.execute(self.sql_set_order)
        # Commit changes to the DB
        conn.commit()
        # Close a communication with SQL
        cur.close()
        assert prev_day <= date.today()
        return prev_day + timedelta(days=1)

    def __extractor(self):
        day = self.__upd_date().strftime('%d.%m.%Y')
        url = self.host_url + self.api_url + f'date={day}'
        print(url)
        # Webpage is xml-like, so I operate with it like with xml-file
        root = ET.fromstring(get(url).content)
        currencies_list = []
        # Extract data from source
        for child in root.iter():
            currencies_list.append(dict(child.attrib))
        return currencies_list

    def __days_since_initial(self):
        return (self.__upd_date() - self.initial_day).days

    def to_csv(self):
        # Extract data from the API omitting dict of date/bank and AUD exchange rate
        currencies_list = self.__extractor()[2:]
        # Create pandas dataframe to simply put data into a csv-file
        print(currencies_list)
        df = pd.DataFrame.from_dict(currencies_list)
        # Add some additional information to the dataframe
        df['id'] = range(self.__days_since_initial() * len(currencies_list) + 1, (self.__days_since_initial() + 1) * len(currencies_list) + 1)
        df['day'] = self.__upd_date()
        df['provider'] = 'PrivatBank'
        # Rename tables and change their order
        df = df[['id', 'day', 'currency', 'baseCurrency', 'provider', 'saleRateNB', 'purchaseRateNB']]
        df.columns = ['id', 'day', 'ccy', 'base_ccy', 'provider', 'sell', 'buy']
        # Save all the data as csv
        df.to_csv(self.path_to_csv)

    def update_table(self):
        """
        Get data from local csv-file and load it to the database
        """
        # Read data from csv
        pb_df = pd.read_csv(self.path_to_csv, index_col='id').drop(["Unnamed: 0"], axis=1)
        # Create PostgreSQL engine
        engine = create_engine('postgresql://user:password@localhost:5432/exchange_rate')
        # Update the database
        try:
            pb_df.to_sql('currencies_daily', engine, if_exists='append')
            os.remove('csv/daily_currency.csv')
            print('Removed CSV')
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            return 'Closed Database connection'
print(Archive().to_csv())
print(Archive().update_table())
