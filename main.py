import base64

def main(event, context):
    # event['data'] содержит сообщение в формате base64.
    # Декодируем это сообщение.
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
        # process_and_upload_files(data_file_path, chunksize, file_objects, service_drive, credentials_list) 
        # Вы заменили credentials_list на credentials, и убрали service_drive
        process_and_upload_files(data_file_path, chunksize, file_objects, credentials)

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


def process_and_upload_files(local_file_path, chunksize, file_objects, service_drive, credentials_list): 

    try: 

        csv_file = local_file_path[:-3] 

        header = None 
        chunkssize = 40000

        logging.info("Reading and processing CSV file...") 
        encoding = detect_encoding(csv_file) 
        logging.info(f"Detected encoding: {encoding}")  # вывод кодировки в логи 

        

        logging.info("Beginning chunk processing...")
        

        

        for chunk_id, chunk in enumerate(pd.read_csv(csv_file, encoding=encoding, sep=',', chunksize=chunksize, dtype=str)): 
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
                
            upload_to_gsheetsgapi(credentials, file_objects, service_drive, [chunk], spreadsheet)
            spreadsheet_ids.add(spreadsheet_id)
            logging.info("Chunk uploaded.")

        logging.info("Beginning renaming process...")

        credentials = credentials_list[0]
        service_sheet = build('sheets', 'v4', credentials=credentials) 
        gc = gspread.authorize(credentials)
        
        for spreadsheet_id in spreadsheet_ids: 
            try:
                logging.info(f"Renaming sheet to 'ready' in spreadsheet {spreadsheet_id}...") 
                spreadsheet = gc.open_by_key(spreadsheet_id)
                worksheet = spreadsheet.worksheet("transit") 
                worksheet_id = worksheet.id
                request = service_sheet.spreadsheets().batchUpdate( 
                    spreadsheetId=spreadsheet_id, 
                    body={ 
                        "requests": [ 
                            { 
                                "updateSheetProperties": { 
                                    "properties": { 
                                       
                                        "title": "ready" 
                                    }, 
                                    "fields": "title" 
                                } 
                            } 
                        ] 
                    } 
                ) 
                logging.info("Request prepared.")
                response = request.execute() 
                logging.info("Request executed.")
            except Exception as e: 
                logging.error(f"Error renaming sheet: {str(e)}")  # добавляем str(e) для вывода подробностей ошибки
    except Exception as e: 
        logging.error(f"An error occurred: {e}") 

    finally: 
        logging.info("Done processing and uploading files.")


def upload_to_gsheetsgapi(credentials, file_objects, service_drive, chunks, spreadsheet): 

    for i, chunk in enumerate(chunks): 
        
        try:
            logging.info(f"Authorizing credentials account: {credentials.service_account_email}")
            service_sheet = build('sheets', 'v4', credentials=credentials) 
        except Exception as e:
            logging.error(f"Error authorizing credentials: {e}")
            continue
        try:
            print("Appending data to spreadsheet...") 
            file = file_objects[i % len(file_objects)]  # выбираем соответствующий файл для чанка
            spreadsheet_id = spreadsheet.id  # get the spreadsheet ID from the spreadsheet object 
            worksheet = spreadsheet.worksheet("transit")
            #worksheet_name = worksheet.title 
            worksheet_id = worksheet.id  # get the worksheet ID from the worksheet object 
            append_datagapi(chunk, service_sheet, spreadsheet_id, worksheet_id, worksheet) 
            print("Data appended.") 
        except Exception as e: 
            logging.error(f"Error appending data to spreadsheet: {e}")  
            continue
    print("Done uploading files.") 
    return spreadsheet_id

def append_datagapi(df, service_sheet, spreadsheet_id, worksheet_id, worksheet,  chunk_size=40000):
    # Получаем текущее количество заполненных строк на листе
    # response = service_sheet.spreadsheets().values().get(
    #    spreadsheetId=spreadsheet_id,
    #    range=worksheet_id,
    #    majorDimension='ROWS'
    #).execute()
    #values = response.get('values', [])
    #last_row = len(values)
    last_row = 0
    chunks = [df[i:i + chunk_size] for i in range(0, df.shape[0], chunk_size)]

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
            logging.info(f"Successfully appended chunk {i+1} of {len(chunks)} to the worksheet.")
        except Exception as e:
            logging.error(f"Error appending chunk {i+1} to the worksheet: {e}")
            continue
        time.sleep(1)