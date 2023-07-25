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
