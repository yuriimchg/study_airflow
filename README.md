This repo contains some pet-projects with Airflow, Celery executor and Selenium. Airflow in conjunction with Selenium WebDriver is a very powerful engine for web-scraping, parsing APIs or collecting datasets for machine learning. 

## Usage:

* Create and activate virtual environment to work with Airflow:

`virtualenv -p python3.6 airflow`

`source airflow/bin/activate`

* Install airflow and other dependencies:

`pip install -r requirements.txt` 

* Set the airflow path environmental variable:

```export AIRFLOW_HOME=`pwd` ```
