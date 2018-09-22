import unittest
import main
from unittest.mock import MagicMock, patch, mock_open
from argparse import Namespace


def create_mock_arguments(profile: str, mode: str):
    options = MagicMock()
    options.profile = profile
    options.mode = mode
    return options


def create_mock_invalid_arguments() -> MagicMock(Namespace):
    options = MagicMock(Namespace)
    options.profile = None
    options.mode = None
    options.is_clear_sheet = "False"
    return options


def create_mock_profile_item(mode: str, clean_flag: str):
    profile_item_obj = MagicMock()
    profile_item_obj.file_name = 'temp_file_name'
    profile_item_obj.file_name_daily = 'temp_file_name_daily'
    profile_item_obj.credential_scopes = ['scope_1', 'scope_2']
    profile_item_obj.service_account_file_path = 'path_to_service_account/service_account.json'
    profile_item_obj.google_doc_id = '12345'
    profile_item_obj.google_doc_mime_type = 'application/json'
    profile_item_obj.mode = mode
    profile_item_obj.gcs_project = 'staging'
    profile_item_obj.gcs_bucket = 'bucket_staging'
    profile_item_obj.gcs_destination_file_path = 'file_name_20180809_010203_1.csv'
    profile_item_obj.gcs_destination_daily_file_path = 'file_name_20180809_1.csv'
    profile_item_obj.google_sheets_range = 'A1:A2'
    profile_item_obj.schema_file_name = 'file_name_20180809_010203.schema'
    profile_item_obj.schema_file_name_daily = 'file_name_20180809_010203.schema'
    profile_item_obj.schema_file_content = [{"mode": "nullable", "name": "col1", "type": "TIMESTAMP", "order": "1"}]
    profile_item_obj.gcs_destination_schema_path = 'gs://bucket_staging/file_name_20180809_010203.schema'
    profile_item_obj.gcs_destination_schema_path_daily = 'gs://bucket_staging/file_name_20180809.schema'
    profile_item_obj.google_sheet_id = '123456789'
    profile_item_obj.google_sheet_timestamp_format = '%m/%d/%Y %H:%M:%S'
    if clean_flag.lower() == 'false':
        profile_item_obj.is_clean_sheet = False
    elif clean_flag.lower() == 'true':
        profile_item_obj.is_clean_sheet = True
    else:
        profile_item_obj.is_clean_sheet = False
    return profile_item_obj


class TestMain(unittest.TestCase):

    def setUp(self):
        self.application = main
        self.application.ArgumentParser = MagicMock()

    @patch('main.ArgumentParser')
    def test_read_arguments_successful(self, mock_argument_parser):
        # Given
        options = create_mock_arguments('profile', 'hourly')
        mock_argument_parser.parse_args = MagicMock(return_value=[options, 'argv'])

        # When
        self.application.read_args(mock_argument_parser)

        # Then
        mock_argument_parser.parse_args.assert_called_once()
        self.assertEqual(mock_argument_parser.add_argument_group.call_count, 2)

    @patch('main.ArgumentParser')
    def test_read_arguments_unsuccessful(self, mock_argument_parser):
        # Given
        with self.assertRaises(SystemExit) as exitException:
            options = create_mock_invalid_arguments()

            # When
            self.application.validate_args(parser=mock_argument_parser, arguments=options)

        # Then
        self.assertEqual(exitException.exception.code, 1)

    @patch('main.os')
    def test_create_output_folder(self, mock_os):
        # Given
        mock_os.path.exists = MagicMock(return_value=False)
        mock_os.makedirs = MagicMock()

        # When
        self.application.create_output_folder()

        # Then
        self.assertEqual(mock_os.makedirs.call_count, 1)

    def test_create_schema(self):
        # Given
        with patch("builtins.open", mock_open(read_data="data")) as mock_file:
            # When
            self.application.create_schema_file(file_name='file', content='data')

            # Then
            mock_file.assert_called_once_with('file', 'wt')
            write_handle = mock_file()
            write_handle.write.assert_called_once_with('data')

    @patch('main.GoogleDocAPIMGMT')
    @patch('main.ProfileItem')
    @patch('main.GoogleCloudStorageClient')
    @patch('main.TransformInputs')
    def test_extract_data_to_gcs_hourly(self, mock_transform_inputs, mock_google_storage, mock_profile_item,
                                        mock_doc_api):
        # Given
        output_file_path = 'output/mockdata.csv'
        schema_file_path = 'output/file.schema'
        gcs_file_destination = 'gs://destination.csv'
        gcs_schema_destination = 'gs://destination.schema'
        mock_doc_api.download_sheets_ranges_csv.return_value = 'file_output_path', 1

        # When
        self.application.extract_data_to_gcs(profile=mock_profile_item, drive_mgmt=mock_doc_api,
                                             output_file_path=output_file_path, schema_file_path=schema_file_path,
                                             gcs_file_destination=gcs_file_destination,
                                             gcs_schema_destination=gcs_schema_destination,
                                             transform_inputs=mock_transform_inputs)

        # Then
        google_storage = mock_google_storage()
        self.assertEqual(google_storage.upload_file_to_gcs.call_count, 2)

    @patch('main.ArgumentParser')
    @patch('main.read_args')
    @patch('main.ProfileItem')
    @patch('main.create_output_folder')
    @patch('main.create_schema_file')
    @patch('main.GoogleDocAPIMGMT')
    @patch('main.extract_data_to_gcs')
    @patch('main.os')
    def test_main_with_no_mode(self, mock_os, mock_exract_data_to_gcs, mock_google_api, mock_create_schema_file,
                               mock_create_output, mock_profile_item, mock_read_args, mock_argument_parser):
        # Given
        with self.assertRaises(Exception):
            # When
            self.application.main()

    @patch('main.ArgumentParser')
    @patch('main.read_args')
    @patch('main.ProfileItem')
    @patch('main.create_output_folder')
    @patch('main.create_schema_file')
    @patch('main.GoogleDocAPIMGMT')
    @patch('main.extract_data_to_gcs')
    @patch('main.os')
    @patch('main.deleted_rows_google_sheets')
    @patch('main.TransformInputs')
    def test_main_mode_daily(self, mock_transform_intpus, mock_clean_sheets, mock_os, mock_exract_data_to_gcs,
                             mock_google_api, mock_create_schema_file,
                             mock_create_output, mock_profile_item, mock_read_args, mock_argument_parser):
        # Given
        mock_profile_item_obj = create_mock_profile_item('daily', "True")
        mock_profile_item.return_value = mock_profile_item_obj

        # When
        self.application.main()

        # Then
        mock_create_schema_file.assert_called_once()
        mock_exract_data_to_gcs.assert_called_once()
        mock_google_api.assert_called_with(scopes=mock_profile_item_obj.credential_scopes,
                                                service_account_file=mock_profile_item_obj.service_account_file_path)
        mock_clean_sheets.assert_called_once()
        mock_transform_intpus.assert_called_once()

    @patch('main.ArgumentParser')
    @patch('main.read_args')
    @patch('main.ProfileItem')
    @patch('main.create_output_folder')
    @patch('main.create_schema_file')
    @patch('main.GoogleDocAPIMGMT')
    @patch('main.extract_data_to_gcs')
    @patch('main.os')
    @patch('main.clean_google_sheets')
    @patch('main.TransformInputs')
    def test_main_mode_hourly(self, mock_transform_intpus, mock_clean_sheets, mock_os, mock_exract_data_to_gcs,
                              mock_google_api,
                              mock_create_schema_file,
                              mock_create_output, mock_profile_item, mock_read_args, mock_argument_parser):
        # Given
        mock_profile_item_obj = create_mock_profile_item('hourly', "False")
        mock_profile_item.return_value = mock_profile_item_obj

        # When
        self.application.main()

        # Then
        mock_create_schema_file.assert_called_once()
        mock_exract_data_to_gcs.assert_called_once()
        mock_google_api.assert_called_once_with(scopes=mock_profile_item_obj.credential_scopes,
                                                service_account_file=mock_profile_item_obj.service_account_file_path)
        mock_clean_sheets.assert_not_called()
        mock_transform_intpus.assert_called_once()


if __name__ == '__main__':
    unittest.main()
