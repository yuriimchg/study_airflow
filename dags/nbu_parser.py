from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from time import sleep
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import os

class NBUParser:
    def __init__(self):
        self.host = 'https://bank.gov.ua/'
        self.initial_day = datetime(2018,1,1)
        self.path_to_api = 'control/uk/curmetal/currency/search/form/day'
        self.path_to_csv = '/home/yurii/airflow/dags/csv/'
        self.csv = 'df.csv'
        with open(self.path_to_csv+'/set_order_nbu.txt', 'r') as set_order_sql:
            self.sql_set_order = set_order_sql.read()
        with open(self.path_to_csv+'/get_date_nbu.txt', 'r') as get_date_sql:
            self.sql_get_date = get_date_sql.read()

    def __start_firefox(self):
        """ Launch Firefox  """
        self.driver = webdriver.Firefox()

    def __start_chromium(self):
        """ Launch Chromium """
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        self.driver = webdriver.Chrome("/usr/lib/chromium-browser/chromedriver")

    def __kill_webdriver(self):
        """ Shut down browser """
        self.driver.quit()

    def __move_to_nbu(self):
        """ Follow the NBU API url """
        self.driver.get(os.path.join(self.host, self.path_to_api))

    def __set_date(self):
        current_date = self.__upd_date()
        """ Choose the date in a webpage """

        # Check daily frequency radiobutton
        radio_button_xpath = '/html/body/table/tbody/tr/td[2]/table/tbody/tr/td[2]/div[4]/form[1]/div[3]/div/div/label[1]/input'
        self.driver.find_element_by_xpath(radio_button_xpath).click()
        # Click the date (calendar button)
        date_xpath = '//*[@id="from"]'
        self.driver.find_element_by_xpath(date_xpath).click()
        sleep(1)

        # Open the year pop-up list
        year_xpath = '//*[@id="ui-datepicker-div"]/div/div/select[2]'
        self.driver.find_element_by_xpath(year_xpath).click()
        # Load BeautifulSoup
        soup = BeautifulSoup(self.driver.page_source, 'lxml')
        # Select year from the list
        year_list = soup.find(class_='ui-datepicker-year')
        # Define a counter
        i = 0
        # Which year
        year = current_date.year
        # Click wanted year
        for elem in year_list:
            i += 1
            if int(elem.text) == year:
                chosen_elem = self.driver.find_element_by_xpath(year_xpath+f'/option[{i}]')
                # Highlight year
            #    ActionChains(self.driver).move_to_element(chosen_elem).perform()
                sleep(1)
                # Select year
                chosen_elem.click()
                # Exit loop
                break
        sleep(1)

        # Open the month pop-up list
        month_xpath = '//*[@id="ui-datepicker-div"]/div/div/select[1]'
        self.driver.find_element_by_xpath(month_xpath).click()
        # Select month from the list
        month_list = soup.find(class_="ui-datepicker-month")
        # Which month
        month = current_date.month
        # Select month from the list
        for elem in month_list:
            if int(elem['value']) + 1 == int(month):
                chosen_elem = self.driver.find_element_by_xpath(month_xpath+f'/option[{month}]')
                #ActionChains(self.driver).move_to_element(chosen_elem).perform()
                sleep(1)
                chosen_elem.click()
                break
        sleep(1)

        # Select the day
        day = current_date.day
        choose_day = self.driver.find_element_by_link_text(str(day))
        #ActionChains(self.driver).move_to_element(choose_day)
        choose_day.click()
        # Sleep some time to prevent non-clickable button
        sleep(2)

        # Execute!
        executor = self.driver.find_element_by_name('execute')
        #ActionChains(self.driver).move_to_element(executor)
        executor.click()

    def __upd_date(self):
        """ Checks the last date in the PostgreSQL DB and adds a new one """
        conn = psycopg2.connect(database='exchange_rate', user='yurii', password='yurii', host='localhost'  )
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
        # Add a day to update the date
        d = prev_day + timedelta(days=1)
        return datetime(d.year, d.month, d.day)

    def __get_currency_table(self):
        """ Data transforming and saving to csv """
        # Count days
        days_count = (self.__upd_date() - self.initial_day).days
        # Parse HTML table with Pandas
        table = pd.read_html(self.driver.current_url)[-2][1:]
        # Convert to pandas dataframe
        pd_table = pd.DataFrame(data=table)
        # Add series ID
        pd_table['id'] = range(days_count * len(pd_table) + 1, (days_count + 1) * len(pd_table) + 1)    ### !!!!!!!!!!!
        # Add provider
        pd_table['provider'] = 'NBU'
        # Rename columns
        pd_table.columns = ['ccy_num', 'ccy_code', 'count', 'ccy_name', 'rate', 'id', 'provider']
        # Get rid of the 'count' column
        pd_table['rate'] = pd_table['rate'].astype('float64') / pd_table['count'].astype('int64')
        pd_table = pd_table.drop(columns=['count'])
        # Add date column
        pd_table['date'] = self.__upd_date()
        pd_table['base_ccy'] = 'UAH'
        # Change columns order
        pd_table = pd_table[['id', 'date', 'ccy_num', 'ccy_code', 'base_ccy', 'ccy_name', 'rate', 'provider']]
        # Save pandas dataframe to csv-file
        pd_table.to_csv(os.path.join(self.path_to_csv, self.csv))

    def __put_table_to_sql(self):
        """ Takes the data from csv-file and updates the table in the database """
        # Read csv-file
        pd_table = pd.read_csv(os.path.join(self.path_to_csv, self.csv), index_col='id').drop(["Unnamed: 0"], axis=1)
        # Create PostgreSQL engine
        engine = create_engine('postgresql://yurii:yurii@localhost:5432/exchange_rate')
        # Update the database
        try:
            pd_table.to_sql('nbu_api', engine, if_exists='append')
            os.remove(os.path.join(self.path_to_csv, self.csv))
            print('removed csv-file')
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            print('Closed connection to the database')

    def sleeping_bastard(self):
        """ Sleep """
        sleep(3)

    def do_it(self):
        """ Main executor """
        self.__start_chromium()
        #self.__start_firefox()
        self.__move_to_nbu()
        self.__set_date()
        self.__upd_date()
        self.__get_currency_table()
        self.__put_table_to_sql()
        self.__kill_webdriver()
