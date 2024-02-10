from airflow import DAG,settings
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.dummy import DummyOperator
from airflow.models.connection import Connection
from airflow.hooks.base_hook import BaseHook
from airflow.decorators import task
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import urllib.request
import pandas as pd
import os

# conn_data = [
#     {"conn_id":"http_sensor","conn_type":"http","host":"https://docs.google.com/spreadsheets/d/e/"},
#     {"conn_id":"csv_path","conn_type":"file","extra":'{"path": "/opt/airflow/"}',},
#     {"conn_id":"postgres_connection_id","conn_type":"postgres","host":"postgres","login":"airflow","password":"airflow","schema":"airflow_db","port":"5432"}
# ]
# session = settings.Session()
# for conn_info in conn_data:
#     conn = Connection(**conn_info) 
#     session.add(conn)
# session.commit()     

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
    print(df.head())
    conn_params = BaseHook.get_connection(postgres_conn_id)
    conn_url = f"postgresql+psycopg2://{conn_params.login}:{conn_params.password}@{conn_params.host}:{conn_params.port}/{conn_params.schema}"
    engine = create_engine(conn_url)
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    return f"Data from {csv_file_path} inserted into {table_name}"


def create_table_from_csv(csv_file_path, table_name):
    df = pd.read_csv(csv_file_path)
    column_data_types = df.dtypes[:-1]
    print(column_data_types)
    create_table_query = f"CREATE TABLE {table_name} ("
    for column_name, data_type in column_data_types.items():
        if data_type == 'int64':
            sql_data_type = "INTEGER"
        elif data_type == 'float64':
            sql_data_type = "REAL"
        else:
            sql_data_type = "TEXT"
        create_table_query += f"{column_name.replace(' ', '_').replace('.','_')} {sql_data_type}, "
    create_table_query = create_table_query.rstrip(', ') + ")"
    print(create_table_query)
    return {"create_table_query": create_table_query}


@task
def download_file(task_id, uri, target_path):
    with urllib.request.urlopen(uri) as file:
        with open(target_path+'/daily_report.csv', "wb") as new_file:
            new_file.write(file.read())
    print(f"Downloaded {uri} to {target_path}")


@task
def sum_by_week(postgres_conn_id):
    conn_params = BaseHook.get_connection(postgres_conn_id)
    conn_url = f"postgresql+psycopg2://{conn_params.login}:{conn_params.password}@{conn_params.host}:{conn_params.port}/{conn_params.schema}"
    engine = create_engine(conn_url)
    
    sql = """
    CREATE TABLE weekly_report_table AS
    SELECT
        "Email Address",
        EXTRACT(YEAR FROM TO_DATE("Date", 'MM/DD/YYYY')) AS Years,
        EXTRACT(WEEK FROM TO_DATE("Date", 'MM/DD/YYYY')) AS Week,
        SUM(CAST(CASE WHEN "Hours Worked" NOT LIKE '%:%' AND "Hours Worked" NOT LIKE '%hrs%' AND "Hours Worked" NOT LIKE '%hr%' AND "Hours Worked" NOT LIKE '%mins%' AND "Hours Worked" NOT LIKE '%and%' AND "Hours Worked" NOT LIKE '%Hrs%' THEN CAST("Hours Worked" AS NUMERIC) ELSE 0 END AS NUMERIC)) AS Total_Hours_Worked
    FROM daily_report_table

    WHERE TO_DATE("Date", 'MM/DD/YYYY') >= CURRENT_DATE - CAST(EXTRACT(DOW FROM CURRENT_DATE) AS INTEGER) - 6
    AND TO_DATE("Date", 'MM/DD/YYYY') <= CURRENT_DATE - CAST(EXTRACT(DOW FROM CURRENT_DATE) AS INTEGER)

    GROUP BY "Email Address", Years, Week
    ORDER BY "Email Address", Years, Week;
    """
    engine.execute(text(sql))
    print("Weekly report table created successfully!")

            

# DAG creation
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
            timeout=30
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

        load_csv_task = PythonOperator(
            task_id="load_csv_to_postgres_task",
            python_callable=create_table_from_csv,  # Pass the function name as a reference
            op_kwargs={
                "csv_file_path": "/opt/airflow/daily_report.csv",
                "table_name": "daily_report_table",
            },
            provide_context=True,  
        )    

        load_data_to_postgres = PostgresOperator(
            task_id='load_data_to_postgres',
            sql="{{ task_instance.xcom_pull(task_ids='load_csv_to_postgres_task')['create_table_query'] }}",
            postgres_conn_id='postgres_connection_id',
        )

        insert_csv_task = PythonOperator(
                task_id="insert_csv_to_postgres_task",
                python_callable=insert_csv_to_postgres,
                op_kwargs={
                    "csv_file_path": "/opt/airflow/daily_report.csv",
                    "table_name": "daily_report_table",
                    "postgres_conn_id": "postgres_connection_id",
                },
                provide_context=True,
            )


        filter_by_week = sum_by_week(
            postgres_conn_id = 'postgres_connection_id',
        )

        # -------------------------------------------------------------------------------------

        create_weekly_report = DummyOperator(
            task_id = 'create_weekly_report',
        )

        send_an_email = DummyOperator(
            task_id = 'send_an_email',
        )

        end_process = DummyOperator(
            task_id = 'end_process',
        )

        start_process >> put_http_sensor >> get_spreadsheet_data >> csv_file_available
        csv_file_available >> load_csv_task >> load_data_to_postgres 
        load_data_to_postgres >> insert_csv_task >> filter_by_week >> create_weekly_report
        create_weekly_report >> send_an_email >> end_process
      