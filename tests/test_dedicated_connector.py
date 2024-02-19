import pytest

from db_utils.default_db_config import DEFAULT_DB_CONFIG_LOCAL
from db_utils.validation_connector import ValidationConnector


class TestDedicatedConnector:
    class TestDBConnectorFunctionality:
        @pytest.fixture(autouse=True)
        def setup(self):
            self.connector = ValidationConnector(**DEFAULT_DB_CONFIG_LOCAL)

        @pytest.mark.parametrize("length, expected", [(22, 3), (33, 7), (44, 16)])
        def test_find_short_headers(self, length, expected):
            res = self.connector.find_short_headers(length)
            headers = [doc['headers'] for doc in res]
            assert len(headers) == expected

        @pytest.mark.parametrize("date_str, expected_num_of_results",
                                 [("2020-01-01", 15), ("2021-01-01", 8), ("2022-01-01", 4)])
        def test_find_late_date_of_creation(self, date_str, expected_num_of_results):
            res = self.connector.find_late_date_of_creation(date_str)
            documents = [doc for doc in res]
            dates = [doc['date_of_creation'] for doc in documents]
            assert len(dates) == expected_num_of_results

        @pytest.mark.parametrize("given_sum, expected_num_of_results", [(1000, 62), (2000, 55), (3000, 40), (5000, 21)])
        def test_find_high_sum_by_precalculated_value(self, given_sum, expected_num_of_results):
            res = self.connector.find_high_sum_by_precalculated_value(given_sum)
            documents = list(res)
            first_rows = [doc['rows_list'][0] for doc in documents]
            first_rows_sums = [sum(map(int, row[1:])) for row in first_rows]
            assert all([sum > given_sum for sum in first_rows_sums])
            assert len(first_rows) == expected_num_of_results

        @pytest.mark.parametrize("given_sum, expected_num_of_results", [(1000, 62), (2000, 55), (3000, 40), (5000, 21)])
        def test_find_high_sum_by_query(self, given_sum, expected_num_of_results):
            res = self.connector.find_high_sum_by_query(given_sum)
            documents = list(res)
            first_rows = [doc['rows_list'][0] for doc in documents]
            first_rows_sums = [sum(map(int, row[1:])) for row in first_rows]
            assert all([sum > given_sum for sum in first_rows_sums])
            assert len(first_rows) == expected_num_of_results
