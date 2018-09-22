from google.cloud import storage
from google.oauth2 import service_account
from google.cloud.storage import Blob
import logging

class GoogleCloudStorageClient:

    def __init__(self, service_account_path, project):
        self.service_account_file = service_account_path
        self.project = project

    def _create_credential(self):
        credentials = service_account.Credentials.from_service_account_file(
            self.service_account_file)
        return storage.Client(credentials=credentials, project=self.project)

    def upload_file_to_gcs(self, bucket_name: str, gcs_file_name: str, local_file_path: str, content_type: str):
        client = self._create_credential()
        bucket = client.get_bucket(bucket_name=bucket_name)
        blob = Blob(name=gcs_file_name, bucket=bucket)
        blob.upload_from_filename(filename=local_file_path, content_type=content_type)
        logging.info('Successfully uploaded file : gs://' + bucket_name + '/' + gcs_file_name)