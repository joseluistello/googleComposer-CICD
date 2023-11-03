import internal_unit_testing

def test_dag_import():
    from . import data_to_google

    internal_unit_testing.assert_has_valid_dag(data_to_google)