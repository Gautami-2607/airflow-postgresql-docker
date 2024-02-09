from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from airflow.providers.http.sensors.http import HttpSensor

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}

with DAG("weekly_time_sheet", 
            start_date = datetime(2024,1,1), 
            schedule_interval="@daily",
            default_args = default_args,
            catchup = False,
            ) as dag:

        start_process = DummyOperator(
            task_id = 'start_process',

        )

        get_spreadsheet_data = DummyOperator(
            task_id = 'get_spreadsheet_data',
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

        start_process >> get_spreadsheet_data >> load_data_to_postgres 
        load_data_to_postgres >> sum_by_day >> group_by_date_and_person 
        group_by_date_and_person >> filter_by_week >> create_weekly_report
        create_weekly_report >> send_an_email >> end_process