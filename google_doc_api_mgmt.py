import logging
from google.oauth2 import service_account
import io
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from global_constant import GlobalConstant
import pandas
from datetime import datetime
from transform_inputs import TransformInputs

class GoogleDocAPIMGMT:

    CLEAR_RANGES = 'A2:Z'
    DELETE_ROW_INDEX_START = 1
    DEFAULT_BQ_TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S'

    def __init__(self, scopes: str, service_account_file: str):
        self.scopes = scopes
        self.service_account = service_account_file


    def _create_api_service(self, service_type):
        scopes = self.scopes
        service_account_file = self.service_account
        google_service_type , google_service_version = self.generate_service_type(service_type)
        credentials = service_account.Credentials.from_service_account_file(
            service_account_file, scopes=scopes)
        drive_api_service = build(google_service_type, google_service_version, credentials=credentials, cache_discovery=False)
        return drive_api_service

    @staticmethod
    def generate_service_type(service_type):
        if service_type == GlobalConstant.GOOGLE_DRIVE_TYPE:
            return 'drive', 'v3'
        elif service_type ==GlobalConstant.GOOGLE_SHEETS_TYPE:
            return 'sheets', 'v4'
        else:
            raise ValueError('Service Type does not match :' + service_type )

    def download_sheet_file(self, file_id: str, mime_type: str, output_path: str, service_type: str) -> str:
        service = self._create_api_service(service_type)
        request = service.files().export_media(fileId=file_id,
                                                     mimeType=mime_type)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)

        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print("Create file : "+ output_path +" progress: %d%%. " % int(status.progress() * 100))
            with open(output_path, 'wb') as file_obj:
                file_obj.write(fh.getvalue())
        return output_path

    def download_sheets_ranges_csv(self, file_id: str, service_type: str, ranges: str, output_path: str, sheet_timestamp : str, transform_inputs: TransformInputs):
        service = self._create_api_service(service_type)
        ranges_name = ranges

        request = service.spreadsheets().values().batchGet(
            spreadsheetId=file_id, ranges=ranges_name)

        response = request.execute()

        try:
            response_values = response.get('valueRanges')[0]['values']
            data = pandas.DataFrame(response_values)
            data = data[data[0].notnull()]
            data = GoogleDocAPIMGMT.transform_mobile_column(data=data, transform_inputs=transform_inputs)
            data = GoogleDocAPIMGMT.transform_timestamp_column(data=data, transform_inputs=transform_inputs, sheet_timestamp=sheet_timestamp)
            data.to_csv(output_path, index=False)
            deleted_row_index_end = data.shape[0] + GoogleDocAPIMGMT.DELETE_ROW_INDEX_START
            return output_path, deleted_row_index_end
        except KeyError as key_exception:
            logging.error('No Data found in range :' + ranges )
            logging.error(key_exception)
            exit(2)
        except Exception as exception:
            raise exception

    @staticmethod
    def transform_mobile_column(data : pandas.DataFrame, transform_inputs : TransformInputs):
        try:
            for mobile_cols in transform_inputs.transform_mobile_number_column_indices:
                data[mobile_cols] = data[mobile_cols].apply(GoogleDocAPIMGMT.convert_mobile_number)
            return data
        except KeyError as key_exception:
            logging.error("Transform at Mobile Column Index : " + key_exception.__str__() + " => Error or out of range")
            exit(1)

    @staticmethod
    def transform_timestamp_column(data : pandas.DataFrame, transform_inputs : TransformInputs, sheet_timestamp : str):
        try:
            for timestamp_cols in transform_inputs.transform_timestamp_column_indices:
                data[timestamp_cols] = data[timestamp_cols].apply(GoogleDocAPIMGMT.convert_date_time, input_format=sheet_timestamp)
            return data
        except KeyError as key_exception:
            logging.error("Transform at Timestamp Column Index :" + key_exception.__str__() + " => Error or out of range")
            exit(1)

    @staticmethod
    def convert_mobile_number(mobile_number: str):
        if GoogleDocAPIMGMT.is_int(str(mobile_number)):
            return '0' + str(mobile_number)
        else:
            return str(mobile_number)

    @staticmethod
    def convert_date_time(input_date_time: str, input_format : str):
        original_datetime = datetime.strptime(input_date_time, input_format)
        return original_datetime.strftime(GoogleDocAPIMGMT.DEFAULT_BQ_TIMESTAMP_FORMAT)


    @staticmethod
    def is_int(number: str):
        try:
            int(number)
            return True
        except ValueError:
            logging.info('The values are not all numeric: ' + number + ' Then stop converting')
            return False


    def clean_sheets_file(self, file_id: str, service_type: str):
        service = self._create_api_service(service_type)
        batch_clear_values_request_body = {
            'ranges': [GoogleDocAPIMGMT.CLEAR_RANGES]
        }

        request = service.spreadsheets().values().batchClear(spreadsheetId=file_id,
                                                             body=batch_clear_values_request_body)
        response = request.execute()
        logging.info(response)

    def delete_sheets_rows_by_index(self, file_id: str, service_type: str, sheet_id: str, end_index : int):
        service = self._create_api_service(service_type)
        start_index = GoogleDocAPIMGMT.DELETE_ROW_INDEX_START
        if end_index > 0:
            requests = [{
                "deleteDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": start_index,
                        "endIndex": end_index
                    }
                }
            }]

            request_body = {"requests" : requests}
            response = service.spreadsheets().batchUpdate(spreadsheetId=file_id, body=request_body).execute()
            logging.info(response)
        else:
            logging.warning("No deleted index detected. Will not activate any deleted rows action.")
