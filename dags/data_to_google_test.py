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

    @patch("data_to_google.storage.Client")
    @patch("data_to_google.gspread.authorize")
    def test_move_data_function(self, mock_gspread_authorize, mock_storage_client):
        # Configura el mock para devolver una cadena JSON válida
        mock_blob = Mock()
        mock_credentials_as_string = '{"type": "service_account", "project_id": "your_project_id"}'  # Un ejemplo de JSON de credenciales
        mock_blob.download_as_string.return_value = mock_credentials_as_string

        # Configura el mock del bucket para devolver el mock_blob cuando se llame a blob()
        mock_bucket = Mock()
        mock_bucket.blob.return_value = mock_blob

        # Configura el mock del cliente de almacenamiento para devolver el mock_bucket cuando se llame a bucket()
        mock_storage_client.return_value.bucket.return_value = mock_bucket

        # Ahora puedes llamar a la función que estás probando
        result = data_to_google.move_data_from_sheet_to_gcs()

        # Realiza tus aserciones aquí
        # self.assertEqual(result, expected_result)  # Asegúrate de definir 'expected_result'

if __name__ == '__main__':
    unittest.main()
