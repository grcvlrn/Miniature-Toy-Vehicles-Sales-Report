'''
=================================================
Milestone 3

Nama  : Gracia Valerine 
Batch : FTDS HCK-017

This program automates the transformation and loading of data from PostgreSQL to Elasticsearch through Airflow. The data used in this program is sample data of toy vehicles.
=================================================
'''

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pytz
from sqlalchemy import create_engine
import pandas as pd
from elasticsearch import Elasticsearch
import logging

# Create functions that will be used to automate the transformation and loading of data from PostgreSQL to Elasticsearch through Airflow

def postgre_logins():
    ''' 
    Connects to a PostgreSQL database using specified credentials and returns an engine object.
    '''
    try:
        # Define the database name, username, password, and host 
        database = 'airflow'
        username = 'airflow'
        password = 'airflow'
        host = 'postgres'
        
        # Construct the PostgreSQL URL
        postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"
        
        # Connecting to PostgreSQL database using SQLAlchemy
        engine = create_engine(postgres_url)
        
        # Test the connection
        with engine.connect() as conn:
            print("Connection successful!")
            # You can also execute a simple query to test
            result = conn.execute("SELECT version()")
            print(result.fetchone())
        
        return engine
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        raise


def adjust_time_with_delta():
    try:
        time_delta = timedelta(hours=1)
        wib_tz = pytz.timezone('Asia/Jakarta')
        current_time_wib = datetime.now(wib_tz)
        new_time_wib = current_time_wib + time_delta

        # Convert datetime to string for JSON serialization
        current_time_wib_str = current_time_wib.isoformat()
        new_time_wib_str = new_time_wib.isoformat()

        return current_time_wib_str, new_time_wib_str
    except Exception as e:
        logging.error(f"Error adjusting time: {e}")
        raise

def sql_connect():
    '''
    Connects to a PostgreSQL database using SQLAlchemy (previous function) and loads data from a CSV file into a table named table_m3 in the database.
    '''
    try:
        conn = postgre_logins()
        df = pd.read_csv('/opt/airflow/dags/P2M3_gracia_valerine_data_raw.csv', sep=",", encoding='Latin-1')
        df.to_sql('table_m3', conn, index=False, if_exists='replace')
    except Exception as e:
        print(f"Error loading data to PostgreSQL: {e}")
        raise

def conversion():
    '''
    Connects to a PostgreSQL database, retrieves data from a table named table_m3, and saves this data into a new CSV file.
    '''
    try:
        conn = postgre_logins()
        df = pd.read_sql_query("SELECT * FROM table_m3", conn)
        df.to_csv('/opt/airflow/dags/P2M3_gracia_valerine_data_raw.csv', sep=',', index=False)
    except Exception as e:
        print(f"Error converting data from PostgreSQL: {e}")
        raise

def data_cleaning():
    ''' 
    Cleans a CSV file by removing duplicates, standardizing column names, removing specific columns, and saving the cleaned data to a new CSV file.
    '''
    try:
        df = pd.read_csv('/opt/airflow/dags/P2M3_gracia_valerine_data_raw.csv')
        df.drop_duplicates(inplace=True)
        df.columns = df.columns.str.lower().str.replace(' ', '_').str.strip()
        df['orderdate'] = pd.to_datetime(df['orderdate'])
        df.drop(columns=['addressline2', 'state', 'territory', 'postalcode'], inplace=True)
        df.to_csv('/opt/airflow/dags/P2M3_gracia_valerine_data_clean.csv', index=False)
    except Exception as e:
        print(f"Error cleaning data: {e}")
        raise

def elasticsearch():
    '''
    Connects to an Elasticsearch server and indexes data from a cleaned CSV file into an Elasticsearch index. Each row from the CSV is converted to a dictionary and added to the Elasticsearch index table_m3_clean.
    '''
    try:
        es = Elasticsearch("http://elasticsearch:9200")
        df = pd.read_csv('/opt/airflow/dags/P2M3_gracia_valerine_data_clean.csv')
        for i, r in df.iterrows():
            doc = r.to_dict()
            res = es.index(index="table_m3_clean", id=i+1, body=doc)
            print(f"Elasticsearch results: {res}")
    except Exception as e:
        print(f"Error indexing data to Elasticsearch: {e}")
        raise

# Define DAG
default_args = {
    'owner': 'gracia', 
    'start_date': datetime(2024, 6, 17, 6, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG with schedule
with DAG(
    "P2M3_P2M3_gracia_valerine_DAG",
    description='Milestone3',
    schedule_interval='30 6 * * *',
    default_args=default_args,
    catchup=False,
) as dag:
    
    # Define tasks
    adjust_time_task = PythonOperator(
        task_id='adjust_time_with_delta',
        python_callable=adjust_time_with_delta
    )
    
    sql_connection = PythonOperator(
        task_id='csv_to_pgsql',
        python_callable=sql_connect
    )
    
    pull_data_sql = PythonOperator(
        task_id='data_from_pgsql',
        python_callable=conversion
    )

    data_clean_func = PythonOperator(
        task_id='data_cleaning',
        python_callable=data_cleaning
    )

    elasticsearch_upload = PythonOperator(
        task_id='elasticsearch_connection',
        python_callable=elasticsearch
    )

    # Set task dependencies
    adjust_time_task >> sql_connection >> pull_data_sql >> data_clean_func >> elasticsearch_upload
