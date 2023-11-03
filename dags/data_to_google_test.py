import json
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
        # Configura el mock para devolver una cadena JSON válida con datos ficticios
        mock_blob = Mock()
        mock_credentials_as_string = json.dumps({
            "type": "service_account",
            "project_id": "fake-project-id",
            "private_key_id": "fake-private-key-id",
            "private_key": "-----BEGIN PRIVATE KEY-----\nfake-private-key\n-----END PRIVATE KEY-----\n",
            "client_email": "fake-email@fake-project-id.iam.gserviceaccount.com",
            "client_id": "fake-client-id",
            "auth_uri": "https://fake-auth-uri.com/o/oauth2/auth",
            "token_uri": "https://fake-token-uri.com/token",
            "auth_provider_x509_cert_url": "https://fake-cert-url.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://fake-cert-url.com/robot/v1/metadata/x509/fake-email%40fake-project-id.iam.gserviceaccount.com"
        })
        mock_blob.download_as_string.return_value = mock_credentials_as_string

        # Configura el mock del bucket para devolver el mock_blob cuando se llame a blob()
        mock_bucket = Mock()
        mock_bucket.blob.return_value = mock_blob

        # Configura el mock del cliente de almacenamiento para devolver el mock_bucket cuando se llame a bucket()
        mock_storage_client.return_value.bucket.return_value = mock_bucket

        # Ahora puedes llamar a la función que estás probando
        data_to_google.move_data_from_sheet_to_gcs()
        # Verifica que se llamó al método upload_from_string del blob con los argumentos correctos
        mock_blob.upload_from_string.assert_called_once_with(ANY, content_type='text/csv')

        # Verifica que se llamó al método authorize de gspread con las credenciales correctas
        mock_gspread_authorize.assert_called_once()

        # Verifica que se obtuvo el bucket correcto
        mock_storage_client.return_value.bucket.assert_called_once_with('credentials-buckket')

        # Verifica que se obtuvo el blob correcto
        mock_bucket.blob.assert_called_once_with('credentials.json')

if __name__ == '__main__':
    unittest.main()
