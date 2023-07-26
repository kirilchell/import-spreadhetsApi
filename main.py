import os 
import json 
import logging 
import pandas as pd 
import numpy as np 
import requests 
from pydrive.auth import GoogleAuth 
from pydrive.drive import GoogleDrive 
from google.oauth2 import service_account 
import time 
from googleapiclient.discovery import build 
from googleapiclient.http import MediaFileUpload 
from google.cloud import storage 
from flask import escape 
import gspread 
from google.auth.transport.requests import Request 
import datetime 
from googleapiclient.errors import HttpError 
import gc as garbage_collector 
import chardet 
import itertools 
from google.cloud import storage 
from google.cloud import pubsub_v1
import base64
from io import StringIO


chunksize = 40000
bucket_name = 'csv-chunk'

def main(event, context):
    # event['data'] содержит сообщение в формате base64.
    # Декодируем это сообщение .
    if 'data' in event:
        base64_message = event['data']
        decoded_message = base64.b64decode(base64_message).decode('utf-8')
        data_file_path, key_filename, spreadsheet_id = decoded_message.split(',')
    else:
        return 'No data provided.'
    
    session = requests.Session()
    try:
        credentials = get_credentials(key_filename)

        # Ваша оригинальная строка здесь была:
        # process_and_upload_files(data_file_path, chunksize, file_objects, spreadsheet_id, credentials) 
        # Вы заменили credentials_list на credentials, и убрали service_drive
        process_and_upload_files(data_file_path, chunksize, credentials, spreadsheet_id)

        if os.path.isfile(data_file_path):
            os.remove(data_file_path)
        else:
            return 'Ошибка: %s файл не найден' % escape(data_file_path)
    except requests.RequestException as e:
        return 'Ошибка при выполнении запроса: %s.' % escape(e)

    except IOError as e:
        return 'Ошибка при записи файла: %s.' % escape(e)

    except Exception as e:
        return 'Произошла непредвиденная ошибка: %s.' % escape(e)

    return 'Файл успешно загружен.'

def get_credentials(key_filename): 
     # Создайте клиент Cloud Storage. 
     storage_client = storage.Client() 
  
     # Получите объект Blob для файла ключа сервисного аккаунта. 
     bucket = storage_client.get_bucket('ia_sam') 
     blob = bucket.blob(key_filename) 
  
     # Скачайте JSON файл ключа сервисного аккаунта. 
     key_json_string = blob.download_as_text() 
  
     # Загрузите ключ сервисного аккаунта из JSON строки. 
     key_dict = json.loads(key_json_string) 
  
     # Создайте учетные данные из ключа сервисного аккаунта. 
     SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 
               'https://www.googleapis.com/auth/drive.file'] 
     credentials = service_account.Credentials.from_service_account_info( 
         key_dict, scopes=SCOPES) 
  
     return credentials

def read_csv_gcs(bucket_name, blob_name): 
    storage_client = storage.Client() 
    bucket = storage_client.get_bucket(bucket_name) 
    blob = storage.Blob(blob_name, bucket) 
    content = blob.download_as_text().decode('utf-8') 
    # Если вы хотите использовать detect_encoding, вам нужно вызвать ее здесь
    # encoding = detect_encoding(content)
    return pd.read_csv(StringIO(content))

def process_and_upload_files(data_file_path, chunksize, credentials, spreadsheet_id, bucket_name):  
    try:  
        header = None  

        df = read_csv_gcs(bucket_name, data_file_path)

        logging.info("Reading and processing CSV file...")  

        logging.info("Beginning chunk processing...") 

        for chunk_id, chunk in enumerate(np.array_split(df, chunksize)):  
            logging.info(f'Processing chunk number: {chunk_id}')  

            if header is None:  
                logging.info("Processing header...")  
                header = chunk.columns.values[:8].tolist() + ['Инфо Магазин']  
                logging.info("Header processed.") 

            logging.info("Processing chunk data...")  
            chunk['Инфо Магазин'] = chunk.iloc[:, 8:].apply(lambda row: '_'.join(row.dropna().astype(str)), axis=1)  
            logging.info("Chunk data processed.") 
            chunk = chunk[header]  
            chunk = chunk.astype(str)  

            append_datagapi(credentials, chunk, spreadsheet_id) 

            logging.info("Chunk uploaded.") 

    finally: 
        logging.info("Done processing and uploading files.")


def append_datagapi(credentials, chunk, spreadsheet_id, chunk_size=40000):
    logging.info(f"Authorizing credentials account: {credentials.service_account_email}")
    service_sheet = build('sheets', 'v4', credentials=credentials)
    sheet = service_sheet.spreadsheets()
    spreadsheet = sheet.get(spreadsheetId=spreadsheet_id).execute()

    # Получить все листы в таблице
    sheets = spreadsheet.get('sheets', '')
    # Найти лист с названием "transit"
    worksheet = next((item for item in sheets if item["properties"]["title"] == "transit"), None)
    if worksheet is not None:
        # Получить id листа
        worksheet_id = worksheet["properties"]["sheetId"]

    last_row = 0
    chunks = [chunk[i:i + chunk_size] for i in range(0, chunk.shape[0], chunk_size)]

    for i, chunk in enumerate(chunks):
        try:
            chunk_str = chunk.astype(str)
            chunk_list = chunk_str.values.tolist()
            request = service_sheet.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=f"transit!A{last_row + 1}",  # Вставляем данные в первую пустую строку
                valueInputOption='RAW',
                insertDataOption='INSERT_ROWS',
                body={'values': chunk_list}
            )
            response = request.execute()
            logging.info(f"Successfully appended chunk {i + 1} of {len(chunks)} to the worksheet.")
        except Exception as e:
            logging.error(f"Error appending chunk {i + 1} to the worksheet: {e}")
            continue
        time.sleep(1)

    print("Data appended.")
    return spreadsheet_id