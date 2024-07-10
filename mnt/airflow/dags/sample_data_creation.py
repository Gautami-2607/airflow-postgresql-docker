import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from airflow.datasets import Dataset

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}

def download_data(**kwargs):
    # Define the public URL
    url = 'https://raw.githubusercontent.com/Gautami-07/gailforce-widgetware/main/OrderDetails.csv'
    example_dataset = Dataset(url)
    print("example_dataset", example_dataset)
    
    # Download the dataset
    response = requests.get(url)
    response.raise_for_status()  # Raise an error for bad status codes
    
    # Save the dataset content to XCom
    kwargs['ti'].xcom_push(key='raw_data', value=response.text)

def preprocess_data(**kwargs):
    ti = kwargs['ti']
    # Retrieve the raw data from XCom
    raw_data = ti.xcom_pull(key='raw_data', task_ids='download_data_task')
    
    # Convert the CSV string to a DataFrame
    from io import StringIO
    df = pd.read_csv(StringIO(raw_data))
    
    # Perform some preprocessing
    df['Quantity'] = df['Quantity'] * 2  # Example preprocessing step: doubling the quantity
    
    # Save the preprocessed data to XCom
    kwargs['ti'].xcom_push(key='preprocessed_data', value=df.to_csv(index=False))

def save_data(**kwargs):
    ti = kwargs['ti']
    # Retrieve the preprocessed data from XCom
    preprocessed_data = ti.xcom_pull(key='preprocessed_data', task_ids='preprocess_data_task')
    
    # Convert the CSV string back to a DataFrame
    from io import StringIO
    df = pd.read_csv(StringIO(preprocessed_data))
    
    # Define the output path
    output_path = '/opt/airflow/final_dataset.csv'
    
    # Save the DataFrame to a CSV file
    df.to_csv(output_path, index=False)
    print(f"Final dataset saved to {output_path}")

with DAG("public_data_pipeline",
         start_date=datetime(2021, 1, 1),
         schedule_interval="@daily",
         default_args=default_args,
         catchup=False) as dag:
    
    start_task = DummyOperator(
        task_id="start_task"
    )
    
    download_data_task = PythonOperator(
        task_id="download_data_task",
        python_callable=download_data,
        provide_context=True
    )
    
    preprocess_data_task = PythonOperator(
        task_id="preprocess_data_task",
        python_callable=preprocess_data,
        provide_context=True
    )
    
    save_data_task = PythonOperator(
        task_id="save_data_task",
        python_callable=save_data,
        provide_context=True
    )
    
    end_task = DummyOperator(
        task_id="end_task"
    )
    
    start_task >> download_data_task >> preprocess_data_task >> save_data_task >> end_task