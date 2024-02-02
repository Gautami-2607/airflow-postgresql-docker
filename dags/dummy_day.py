from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

args = {'start_date': days_ago(1)}

dag = DAG(
    dag_id='testing_dag',
    default_args=args, 
    schedule_interval=None
    )


with dag:

    op1 = PostgresOperator(
        task_id = "make_a_staging_table",
        postgres_conn_id = "my_pg_connection",
        sql = """CREATE TABLE scaled_data
        (
            idx_col integer,
            petal_width float,
            petal_length float
        );
            """
        )

    op1 

# from airflow import DAG
# from airflow.operators.dummy_operator import DummyOperator
# from datetime import datetime
# with DAG('example_dag', start_date=datetime(2016, 1, 1)) as dag:
#     op = DummyOperator(task_id='op')
