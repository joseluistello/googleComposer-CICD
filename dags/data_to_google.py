import gspread
from google.oauth2.service_account import Credentials
from google.cloud import storage
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import csv
import io
import json

def get_credentials_from_gcs(bucket_name, credentials_path):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(credentials_path)
    credentials_info = json.loads(blob.download_as_string())
    credentials = Credentials.from_service_account_info(credentials_info)
    return credentials

def move_data_from_sheet_to_gcs():
    # Configura la autenticación para Google Sheets y GCS
    creds = get_credentials_from_gcs('credentials-buckket', 'credentials.json')
    client = gspread.authorize(creds)

    # Abre el Google Sheet y lee los datos
    spreadsheet = client.open_by_url('https://docs.google.com/spreadsheets/d/1JLXYJgWp5knCfIicIb6qrgLJxSGNPZZIezqwBUxoysk')
    sheet = spreadsheet.get_worksheet(0)
    data = sheet.get_all_values()

    # Escribe los datos en un flujo de bytes en memoria
    in_memory_csv = io.StringIO()
    writer = csv.writer(in_memory_csv)
    writer.writerows(data)
    in_memory_csv.seek(0)

    # Carga el archivo en GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket('bronze_layer')
    blob = bucket.blob('data/temp_data.csv')
    blob.upload_from_string(in_memory_csv.getvalue(), content_type='text/csv')

default_args = {
    'owner': 'you',
    'start_date': datetime(2023, 11, 2),
    'retries': 1,
}

dag = DAG('move_data_dag', default_args=default_args, schedule_interval='@daily')

move_data_task = PythonOperator(
    task_id='move_data_from_sheet_to_gcs',
    python_callable=move_data_from_sheet_to_gcs,
    dag=dag
)
