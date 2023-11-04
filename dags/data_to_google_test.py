import os
import sys
import unittest

# Asegúrate de que el directorio actual es el directorio raíz del proyecto
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

from dags.data_to_google import get_credentials_from_gcs, move_data_from_sheet_to_gcs

class TestImport(unittest.TestCase):
    def test_import_get_credentials_from_gcs(self):
        """Testea que la función get_credentials_from_gcs se pueda importar correctamente."""
        self.assertTrue(callable(get_credentials_from_gcs))

    def test_import_move_data_from_sheet_to_gcs(self):
        """Testea que la función move_data_from_sheet_to_gcs se pueda importar correctamente."""
        self.assertTrue(callable(move_data_from_sheet_to_gcs))

if __name__ == '__main__':
    unittest.main()
