from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.dummy import DummyOperator
from airflow.models.connection import Connection
from airflow.decorators import task
from datetime import datetime, timedelta
import urllib.request

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

@task
def download_file(task_id, uri, target_path):
    with urllib.request.urlopen(uri) as file:
        with open(target_path, "wt") as new_file:
            new_file.write(file.read())

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
            target_path='/opt'
        )

        load_data_to_postgres = DummyOperator(
            task_id = 'load_data_to_postgres',
        )

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

        start_process >> put_http_sensor >> get_spreadsheet_data >> load_data_to_postgres 
        load_data_to_postgres >> sum_by_day >> group_by_date_and_person 
        group_by_date_and_person >> filter_by_week >> create_weekly_report
        create_weekly_report >> send_an_email >> end_process