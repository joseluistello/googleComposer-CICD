import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import storage
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def move_data_from_sheet_to_gcs():
    # Configura la autenticación para Google Sheets
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('../credentials/credentials.json', scope)
    client = gspread.authorize(creds)

    # Abre el Google Sheet y lee los datos
    spreadsheet = client.open_by_url('https://docs.google.com/spreadsheets/d/1JLXYJgWp5knCfIicIb6qrgLJxSGNPZZIezqwBUxoysk')
    sheet = spreadsheet.get_worksheet(0)
    data = sheet.get_all_values()

    # Escribe los datos en un archivo temporal
    with open('temp_data.csv', 'w') as f:
        for row in data:
            f.write(','.join(row))
            f.write('\n')

    # Carga el archivo en GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket('bronze_layer')
    blob = bucket.blob('data/temp_data.csv')
    blob.upload_from_filename('temp_data.csv')

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

