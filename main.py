import logging
import os
import pytz
from datetime import datetime, timedelta
from ruamel import yaml
from argparse import ArgumentParser, Namespace
from google_doc_api_mgmt import GoogleDocAPIMGMT
from global_constant import GlobalConstant
from google_storage_mgmt import GoogleCloudStorageClient
from transform_inputs import TransformInputs

OUTPUT_DIR = '/outputs'


def read_args(parser_args: ArgumentParser) -> Namespace:
    required = parser_args.add_argument_group('required arguments')
    required.add_argument('-p', '--profile',
                          help='Profile of job to get config from (.yaml)')
    required.add_argument('-mode', '--mode',
                          help='daily or monthly mode input')
    optional = parser_args.add_argument_group('option argument')
    optional.add_argument('-clean', '--clean_sheet',
                          default="False",
                          help='Clean sheet after uploading data from google sheets, flag True or False')
    return parser_args.parse_args()


def validate_args(parser: ArgumentParser, arguments: Namespace):
    if not arguments.profile:
        logging.error("Please specific -p or --profile for profile path")
        parser.print_help()
        exit(1)

    if not arguments.mode:
        logging.error("Please specific mode -mode or --mode for mode select")
        parser.print_help()
        exit(1)

    if not arguments.clean_sheet or arguments.clean_sheet.__str__().lower() in ['false']:
        logging.info("Set clean sheet flag to default -> False")
        arguments.clean_sheet = False
    elif arguments.clean_sheet.__str__().lower() in ['true']:
        arguments.clean_sheet = True
    else:
        logging.info("Set clean sheet flag to default -> False")
        arguments.clean_sheet = False


def create_output_folder() -> str:
    output_directory = os.getcwd() + OUTPUT_DIR
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    return output_directory


def create_schema_file(file_name: str, content: str) -> str:
    with open(file_name, 'wt') as schema_file:
        schema_file.write(content)


class ProfileItem:

    def __init__(self, config_path: str, mode: str, is_clean_sheet: bool):
        profile = yaml.safe_load(open(config_path))
        input_timezone = profile['timezone']
        local_time_zone = pytz.timezone(input_timezone)
        utc_time = datetime.utcnow()
        current_time = utc_time.replace(tzinfo=pytz.utc).astimezone(tz=local_time_zone)
        self.file_name = profile['gsc_file_pattern'] + current_time.strftime(profile['date_time_format']) + '_1' + '.' + \
                         profile['gsc_file_type']
        self.file_name_daily = profile['gsc_file_pattern'] + datetime.strftime(current_time - timedelta(1),
                                                                               profile['date_format']) + '_1' + '.' + \
                               profile['gsc_file_type']
        self.credential_scopes = profile['credential_api_scope']
        self.service_account_file_path = profile['service_account_file_path']
        self.google_doc_id = profile['google_doc_id']
        self.google_doc_mime_type = profile['mime_type']
        self.mode = mode
        self.gcs_project = profile['gcs_project']
        self.gcs_bucket = profile['gcs_bucket_name']
        self.gcs_destination_file_path = profile['gcs_bucket_destination'] + '/' + current_time.strftime(
            '%Y') + '/' + current_time.strftime('%m') + '/' + self.file_name
        self.gcs_destination_daily_file_path = profile['gcs_bucket_destination'] + '/' + current_time.strftime(
            '%Y') + '/' + self.file_name_daily
        self.google_sheets_range = profile['goolge_data_range']
        self.schema_file_name = profile['gsc_file_pattern'] + current_time.strftime(
            profile['date_time_format']) + '.schema'
        self.schema_file_name_daily = profile['gsc_file_pattern'] + datetime.strftime(current_time - timedelta(1),
                                                                                      profile[
                                                                                          'date_format']) + '.schema'
        self.schema_file_content = profile['schema_content']
        self.gcs_destination_schema_path = profile['gcs_bucket_destination'] + '/' + current_time.strftime(
            '%Y') + '/' + current_time.strftime('%m') + '/' + self.schema_file_name
        self.gcs_destination_schema_path_daily = profile['gcs_bucket_destination'] + '/' + current_time.strftime(
            '%Y') + '/' + self.schema_file_name_daily
        self.google_sheet_id = convert_str_to_int(text=profile['google_sheet_id'])
        self.google_sheet_timestamp_format = profile['google_sheet_timestamp_format']
        self.is_clean_sheet = is_clean_sheet
        self.columns_transform_mobile_number = profile['google_sheet_column_transform_mobile_number']
        self.columns_transform_timestamp = profile['google_sheet_column_transform_timestamp']


def convert_str_to_int(text: str):
    try:
        return int(text)
    except ValueError as exception:
        logging.error("Cannot parse :" + text + "to number.")
        raise exception


def extract_data_to_gcs(profile: ProfileItem, drive_mgmt: GoogleDocAPIMGMT, output_file_path: str,
                        schema_file_path: str, gcs_file_destination: str, gcs_schema_destination: str, transform_inputs: TransformInputs):
    download_file, delete_row_index = drive_mgmt.download_sheets_ranges_csv(file_id=profile.google_doc_id,
                                                          service_type=GlobalConstant.GOOGLE_SHEETS_TYPE,
                                                          ranges=profile.google_sheets_range,
                                                          output_path=output_file_path,
                                                          sheet_timestamp=profile.google_sheet_timestamp_format, transform_inputs=transform_inputs)

    google_storage = GoogleCloudStorageClient(service_account_path=profile.service_account_file_path,
                                              project=profile.gcs_project)
    google_storage.upload_file_to_gcs(bucket_name=profile.gcs_bucket,
                                      gcs_file_name=gcs_schema_destination,
                                      local_file_path=schema_file_path,
                                      content_type='Application/json')
    google_storage.upload_file_to_gcs(bucket_name=profile.gcs_bucket,
                                      gcs_file_name=gcs_file_destination,
                                      local_file_path=download_file,
                                      content_type=profile.google_doc_mime_type)
    return delete_row_index


def clean_google_sheets(profile_item: ProfileItem):
    drive_management = GoogleDocAPIMGMT(scopes=profile_item.credential_scopes,
                                        service_account_file=profile_item.service_account_file_path)
    drive_management.clean_sheets_file(file_id=profile_item.google_doc_id,
                                       service_type=GlobalConstant.GOOGLE_SHEETS_TYPE)

def deleted_rows_google_sheets(profile_item : ProfileItem, delete_index_end : int):
    drive_management = GoogleDocAPIMGMT(scopes=profile_item.credential_scopes,
                                        service_account_file=profile_item.service_account_file_path)
    drive_management.delete_sheets_rows_by_index(file_id=profile_item.google_doc_id,
                                       service_type=GlobalConstant.GOOGLE_SHEETS_TYPE,sheet_id=profile_item.google_sheet_id , end_index=delete_index_end)

def main():
    parser = ArgumentParser()
    args = read_args(parser)
    validate_args(parser=parser, arguments=args)
    profile_item = ProfileItem(config_path=args.profile, mode=args.mode, is_clean_sheet=args.clean_sheet)
    transform_inputs = TransformInputs()
    transform_inputs.convert_transform_inputs(mobile_column_inputs=profile_item.columns_transform_mobile_number,
                                              timestamp_column_inputs=profile_item.columns_transform_timestamp)
    output_path = create_output_folder()
    local_output_path = ''
    local_schema_file_path = ''
    row_delete_index = 0

    try:
        drive_management = GoogleDocAPIMGMT(scopes=profile_item.credential_scopes,
                                            service_account_file=profile_item.service_account_file_path)
        if profile_item.mode == GlobalConstant.MODE_DAILY:
            local_output_path = output_path + '/' + profile_item.file_name_daily
            local_schema_file_path = output_path + '/' + profile_item.schema_file_name_daily
            create_schema_file(file_name=local_schema_file_path,
                               content=profile_item.schema_file_content)
            row_delete_index = extract_data_to_gcs(profile=profile_item, drive_mgmt=drive_management, output_file_path=local_output_path,
                                schema_file_path=local_schema_file_path,
                                gcs_file_destination=profile_item.gcs_destination_daily_file_path,
                                gcs_schema_destination=profile_item.gcs_destination_schema_path_daily, transform_inputs=transform_inputs)
        elif profile_item.mode == GlobalConstant.MODE_HOURLY:
            local_output_path = output_path + '/' + profile_item.file_name
            local_schema_file_path = output_path + '/' + profile_item.schema_file_name
            create_schema_file(file_name=local_schema_file_path,
                               content=profile_item.schema_file_content)
            row_delete_index = extract_data_to_gcs(profile=profile_item, drive_mgmt=drive_management, output_file_path=local_output_path,
                                schema_file_path=local_schema_file_path,
                                gcs_file_destination=profile_item.gcs_destination_file_path,
                                gcs_schema_destination=profile_item.gcs_destination_schema_path, transform_inputs=transform_inputs)
        else:
            logging.error('Invalid mode input')
            raise Exception("Please use only daily or hourly mode.")

        if profile_item.is_clean_sheet:
            deleted_rows_google_sheets(profile_item=profile_item, delete_index_end=row_delete_index)

    except Exception as exception:
        logging.error('Error occurs at :' + str(exception))
        raise exception
    finally:
        if os.path.exists(local_output_path):
            os.remove(local_output_path)
        if os.path.exists(local_schema_file_path):
            os.remove(local_schema_file_path)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)-7s - %(message)s'
    )
    main()
