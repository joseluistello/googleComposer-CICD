import gspread
from google.oauth2.service_account import Credentials
from google.cloud import storage
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import csv
import io
import json

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/drive'
]

def get_credentials_from_gcs(bucket_name, credentials_path):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(credentials_path)
    credentials_info = json.loads(blob.download_as_string())
    credentials = Credentials.from_service_account_info(credentials_info, scopes=SCOPES)
    return credentials

def move_data_from_sheet_to_gcs(**kwargs):
    creds = get_credentials_from_gcs('credentials-buckket', 'credentials.json')
    client = gspread.authorize(creds)

    spreadsheet = client.open_by_url('https://docs.google.com/spreadsheets/d/1JLXYJgWp5knCfIicIb6qrgLJxSGNPZZIezqwBUxoysk')
    sheet = spreadsheet.get_worksheet(0)
    data = sheet.get_all_values()

    in_memory_csv = io.StringIO()
    writer = csv.writer(in_memory_csv)
    writer.writerows(data)
    in_memory_csv.seek(0)
    
    storage_client = storage.Client()
    bucket = storage_client.bucket('bronze_layer')
    blob = bucket.blob('data/db_bronze_layer.csv')
    blob.upload_from_string(in_memory_csv.getvalue(), content_type='text/csv')

default_args = {
    'owner': 'you',
    'start_date': datetime(2023, 11, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5), 
    'catchup': False
}

dag = DAG(
    'data_swag_2',
    default_args=default_args,
    description='DAG para mover datos de Google Sheets a GCS',
    schedule_interval='@daily',
)

move_data_task = PythonOperator(
    task_id='move_data_from_sheet_to_gcs',
    python_callable=move_data_from_sheet_to_gcs,
    provide_context=True,
    dag=dag,
)

if __name__ == "__main__":
    dag.clear(reset_dag_runs=True)
    dag.run()
