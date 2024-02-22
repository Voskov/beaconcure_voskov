from datetime import datetime

import pytest

from data_classes.validation_status import ValidationStatus
from data_utils.document_validator import DocumentValidator


class TestDocumentValidator:
    '''
    This is a BAD test suite because it is based on the real db and the real data.
    Which I'd never do in a real project.
    But also I don't want to spend time on mocking the db and the data.
    '''

    @pytest.fixture(autouse=True)
    def setup(self):
        self.document_validator = DocumentValidator()

    def test_short_header_discrepancies(self):
        self.document_validator.max_headers_length = 22
        self.document_validator.collect_short_header_discrepancies()
        expected_short_header_discrepancies = [
            (ValidationStatus.INVALID, {'headers': ['Laurie Wade'], 'length': 15}),
            (ValidationStatus.INVALID, {'headers': ['Matthew Hart'], 'length': 16}),
            (ValidationStatus.INVALID, {'headers': ['Kelly Thomas'], 'length': 16})]
        found_short_header_discrepancies = self.document_validator.all_discrepancies
        assert found_short_header_discrepancies == expected_short_header_discrepancies

    def test_late_date_discrepancies(self):
        self.document_validator.late_date = '2022-01-01'
        self.document_validator.collect_late_date_discrepancies()
        expected_late_date_discrepancies = [
            (ValidationStatus.INVALID, {'date_of_creation': datetime(2022, 10, 12, 0, 0)}),
            (ValidationStatus.INVALID, {'date_of_creation': datetime(2022, 1, 20, 0, 0)}),
            (ValidationStatus.INVALID, {'date_of_creation': datetime(2022, 8, 31, 0, 0)}),
            (ValidationStatus.INVALID, {'date_of_creation': datetime(2022, 1, 21, 0, 0)})]
        found_late_date_discrepancies = self.document_validator.all_discrepancies
        assert found_late_date_discrepancies == expected_late_date_discrepancies

    def test_high_sum_discrepancies(self):
        self.document_validator.high_sum = 8000
        self.document_validator.collect_high_sum_discrepancies()
        expected_high_sum_discrepancies = [(ValidationStatus.INVALID, {'sum_of_first_row': 8246}),
                                           (ValidationStatus.INVALID, {'sum_of_first_row': 9689}),
                                           (ValidationStatus.INVALID, {'sum_of_first_row': 8462})]
        found_high_sum_discrepancies = self.document_validator.all_discrepancies
        assert found_high_sum_discrepancies == expected_high_sum_discrepancies

    @pytest.mark.xfail(reason="This test is based on the real db and the real data")
    def test_saved_discrepancies(self):
        self.document_validator.collect_saved_discrepancies()
        found_saved_discrepancies = self.document_validator.all_discrepancies
        assert len(found_saved_discrepancies) == 7
