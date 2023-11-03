import unittest
from unittest.mock import patch, Mock
import internal_unit_testing
from dags import data_to_google

class TestStockDataDag(unittest.TestCase):

    def test_dag_import(self):
        internal_unit_testing.assert_has_valid_dag(data_to_google)

    def test_move_data_task(self):
        task = data_to_google.dag.get_task('move_data_from_sheet_to_gcs')
        self.assertIsNotNone(task)
        self.assertEqual(task.task_id, 'move_data_from_sheet_to_gcs')

    @patch("dags.data_to_google.storage.Client")
    @patch("dags.data_to_google.gspread.authorize")
    def test_move_data_function(self, mock_gspread_authorize, mock_storage_client):
        # Configura los mocks
        mock_sheet = Mock()
        mock_sheet.get_all_values.return_value = [['data1', 'data2'], ['data3', 'data4']]
        mock_client = Mock()
        mock_client.open_by_url.return_value.get_worksheet.return_value = mock_sheet
        mock_gspread_authorize.return_value = mock_client

        mock_bucket = Mock()
        mock_blob = Mock()
        mock_storage_client.return_value.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        # Llama a la función que estás probando
        data_to_google.move_data_from_sheet_to_gcs()

        # Verifica que los métodos fueron llamados con los argumentos esperados
        mock_gspread_authorize.assert_called_once()
        mock_storage_client.assert_called_once()
        mock_blob.upload_from_string.assert_called_once()

if __name__ == '__main__':
    unittest.main()
