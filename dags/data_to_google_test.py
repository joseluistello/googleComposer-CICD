import unittest
from unittest.mock import patch, Mock
from data_to_google import get_credentials_from_gcs, move_data_from_sheet_to_gcs  # Replace 'data_to_google' with the actual name of your Python file containing the DAG.
import json

class TestDAG(unittest.TestCase):
    @patch('data_to_google.storage.Client')
    def test_get_credentials_from_gcs(self, mock_storage_client):
        # Setup Mock objects
        mock_bucket = Mock()
        mock_blob = Mock()
        mock_storage_client().bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.download_as_string.return_value = json.dumps({
            'type': 'service_account',
            'project_id': 'your-project-id',
        })

        # Call the function
        credentials = get_credentials_from_gcs('mock_bucket', 'mock_credentials.json')

        # Asserts
        self.assertIsNotNone(credentials)
        mock_storage_client().bucket.assert_called_with('mock_bucket')
        mock_bucket.blob.assert_called_with('mock_credentials.json')
        mock_blob.download_as_string.assert_called_once()

    @patch('data_to_google.storage.Client')
    @patch('data_to_google.gspread.authorize')
    @patch('data_to_google.get_credentials_from_gcs')
    def test_move_data_from_sheet_to_gcs(self, mock_get_credentials, mock_authorize, mock_storage_client):
        # Setup Mock objects
        mock_creds = Mock()
        mock_client = Mock()
        mock_sheet = Mock()
        mock_worksheet = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()

        mock_get_credentials.return_value = mock_creds
        mock_authorize.return_value = mock_client
        mock_client.open_by_url.return_value = mock_sheet
        mock_sheet.get_worksheet.return_value = mock_worksheet
        mock_worksheet.get_all_values.return_value = [['Header1', 'Header2'], ['Value1', 'Value2']]
        mock_storage_client().bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        # Call the function
        move_data_from_sheet_to_gcs()

        # Asserts
        mock_get_credentials.assert_called_once()
        mock_authorize.assert_called_once()
        mock_client.open_by_url.assert_called_once()
        mock_sheet.get_worksheet.assert_called_once()
        mock_worksheet.get_all_values.assert_called_once()
        mock_storage_client().bucket.assert_called_with('bronze_layer')
        mock_bucket.blob.assert_called_with('data/db_bronze_layer.csv')
        mock_blob.upload_from_string.assert_called_once()

if __name__ == '__main__':
    unittest.main()
