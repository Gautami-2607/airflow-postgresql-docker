from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.dummy import DummyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models.connection import Connection
from airflow.decorators import task
from datetime import datetime, timedelta
import urllib.request
import os
import pandas as pd

c = Connection(
    conn_id = "http_sensor",
    conn_type="http",
    host="https://docs.google.com/spreadsheets/d/e/",
)

LINK_TO_DOWNLOAD = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQ59Q-NafCjOPNsIXLUqaPazXbZJvf9aPdD8W2CBpVAoSP-Eb0F92efvd2znIHFwRW7KelDcdx4ts9M/pub?gid=1304433456&single=true&output=csv"
ENDPOINT = "2PACX-1vQ59Q-NafCjOPNsIXLUqaPazXbZJvf9aPdD8W2CBpVAoSP-Eb0F92efvd2znIHFwRW7KelDcdx4ts9M/pub?gid=1304433456&single=true&output=csv"

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}


def insert_csv_to_postgres(csv_file_path, table_name, postgres_conn_id):
    df = pd.read_csv(csv_file_path)
    conn_params = BaseHook.get_connection(postgres_conn_id)
    conn_url = f"postgresql+psycopg2://{conn_params.login}:{conn_params.password}@{conn_params.host}:{conn_params.port}/{conn_params.schema}"
    engine = create_engine(conn_url)
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    return f"Data from {csv_file_path} inserted into {table_name}"


def create_table_from_csv(csv_file_path, table_name):
    df = pd.read_csv(csv_file_path)
    column_data_types = df.dtypes
    create_table_query = f"CREATE TABLE {table_name} ("
    for column_name, data_type in column_data_types.items():
        if data_type == 'int64':
            sql_data_type = 'INTEGER'
        elif data_type == 'float64':
            sql_data_type = 'REAL'
        else:
            sql_data_type = 'TEXT'
        create_table_query += f"{column_name} {sql_data_type}, "
    create_table_query = create_table_query.rstrip(', ') + ")"
    print(create_table_query)
    return {"create_table_query": create_table_query}


@task
def download_file(task_id, uri, target_path):
    with urllib.request.urlopen(uri) as file:
        with open(target_path+'/daily_report.csv', "wb") as new_file:
            new_file.write(file.read())
    print(f"Downloaded {uri} to {target_path}")
            

with DAG("weekly_time_sheet", 
            start_date = datetime(2024,1,1), 
            schedule_interval="@daily",
            default_args = default_args,
            catchup = False,
            ) as dag:


        start_process = DummyOperator(
            task_id = 'start_process',

        )

        put_http_sensor = HttpSensor(
            task_id = 'put_http_sensor',
            http_conn_id = 'http_sensor',
            endpoint = ENDPOINT ,
            poke_interval=5,
            timeout=20
        )

        get_spreadsheet_data = download_file(
            task_id = 'get_spreadsheet_data',
            uri=LINK_TO_DOWNLOAD, 
            target_path='/opt/airflow'
        )

        csv_file_available = FileSensor(
                task_id = "csv_file_available",
                fs_conn_id = "csv_path",
                filepath = "daily_report.csv",
                poke_interval=5,
                timeout=20
            )

        load_data_to_postgres = DummyOperator(
            task_id='load_data_to_postgres',
            sql="{{ task_instance.xcom_pull(task_ids='load_csv_to_postgres_task')['create_table_query'] }}",
            postgres_conn_id='postgres_connection_id',
        )

        # --------------------------------------------------------
        sum_by_day = DummyOperator(
            task_id = 'sum_by_day',
        )

        group_by_date_and_person = DummyOperator(
            task_id = 'group_by_date_and_person',
        )

        filter_by_week = DummyOperator(
            task_id = 'filter_by_week',
        )

        create_weekly_report = DummyOperator(
            task_id = 'create_weekly_report',
        )

        send_an_email = DummyOperator(
            task_id = 'send_an_email',
        )

        end_process = DummyOperator(
            task_id = 'end_process',
        )

        start_process >> put_http_sensor >> get_spreadsheet_data >> csv_file_available >> load_data_to_postgres 
        load_data_to_postgres >> sum_by_day >> group_by_date_and_person 
        group_by_date_and_person >> filter_by_week >> create_weekly_report
        create_weekly_report >> send_an_email >> end_process