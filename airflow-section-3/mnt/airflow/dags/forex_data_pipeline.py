from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
# from airflow.providers.postgres.operators.postgres_sql import PostgresSQLExecuteOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine
import pandas as pd

from datetime import datetime, timedelta
import csv
import requests
import json

# https://gist.github.com/

import pandas as pd
from sqlalchemy import create_engine

def insert_csv_to_postgres(csv_file_path, table_name, postgres_conn_id):
    # Read the CSV file using pandas
    df = pd.read_csv(csv_file_path)

    # Fetch the PostgreSQL connection parameters from Airflow Connections
    conn_params = BaseHook.get_connection(postgres_conn_id)
    conn_url = f"postgresql+psycopg2://{conn_params.login}:{conn_params.password}@{conn_params.host}:{conn_params.port}/{conn_params.schema}"

    # Create a SQLAlchemy engine for PostgreSQL
    engine = create_engine(conn_url)

    # Insert the data into the PostgreSQL table
    df.to_sql(table_name, engine, if_exists='replace', index=False)

    return f"Data from {csv_file_path} inserted into {table_name}"



def create_table_from_csv(csv_file_path, table_name):
    # Read the CSV file using pandas
    df = pd.read_csv(csv_file_path)

    # Analyze the data types of each column
    column_data_types = df.dtypes

    # Generate the CREATE TABLE statement
    create_table_query = f"CREATE TABLE {table_name} ("

    for column_name, data_type in column_data_types.items():
        # Assign the appropriate SQL data type based on pandas data types
        if data_type == 'int64':
            sql_data_type = 'INTEGER'
        elif data_type == 'float64':
            sql_data_type = 'REAL'
        else:
            sql_data_type = 'TEXT'

        create_table_query += f"{column_name} {sql_data_type}, "

    # Remove the trailing comma and space
    create_table_query = create_table_query.rstrip(', ') + ")"
    print(create_table_query)

    # Return a dictionary containing the SQL query
    return {"create_table_query": create_table_query}

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}

def _get_message() -> str:
    return "Hi from forex_data_pipeline"

with DAG("forex_data_pipeline", 
            start_date = datetime(2021,1,1), 
            schedule_interval="@daily",
            default_args = default_args,
            catchup = False,
            ) as dag:
            
            is_forex_rates_available = HttpSensor(
                task_id = "is_forex_rates_available",
                http_conn_id = "forex_api",
                endpoint="marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b",
                response_check = lambda response: "rates" in response.text,
                poke_interval=5,
                timeout=20
            )

            is_forex_currencies_file_available = FileSensor(
            task_id="is_forex_currencies_file_available",
            fs_conn_id="forex_path",
            filepath="sales_data_sample.csv",
            poke_interval=5,
            timeout=60  # Adjust the timeout as needed
        )


            load_csv_task = PythonOperator(
            task_id="load_csv_to_postgres_task",
            python_callable=create_table_from_csv,  # Pass the function name as a reference
            op_kwargs={
                "csv_file_path": "/opt/airflow/dags/files/sales_data_sample.csv",
                "table_name": "sales_table"
            },
            provide_context=True,  
        )

            postgres_task = PostgresOperator(
            task_id='postgres_task',
            sql="{{ task_instance.xcom_pull(task_ids='load_csv_to_postgres_task')['create_table_query'] }}",
            postgres_conn_id='postgres_connection_id',
        )


            insert_csv_task = PythonOperator(
                task_id="insert_csv_to_postgres_task",
                python_callable=insert_csv_to_postgres,
                op_kwargs={
                    "csv_file_path": "/opt/airflow/dags/files/sales_data_sample.csv",
                    "table_name": "sales_table",
                    "postgres_conn_id": "postgres_connection_id",
                },
                provide_context=True,
            )

            is_forex_rates_available >> is_forex_currencies_file_available >> load_csv_task >> postgres_task >> insert_csv_task