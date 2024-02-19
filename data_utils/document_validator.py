from typing import Optional

from data_classes.discrepancy import DiscrepancyType
from data_classes.validation_status import ValidationStatus
from db_utils.discrepancy_db_connector import DiscrepancyDBConnector
from db_utils.validation_connector import ValidationConnector
from db_utils.default_db_config import DEFAULT_DB_CONFIG_LOCAL, DISCREPANCIES_DB_CONFIG_LOCAL


class DocumentValidator:
    def __init__(self, max_headers_length: Optional[int] = None,
                 late_date: Optional[str] = None,
                 high_sum: Optional[int] = None):
        self.max_headers_length = max_headers_length
        self.late_date = late_date
        self.high_sum = high_sum
        self.validation_connector = ValidationConnector(**DEFAULT_DB_CONFIG_LOCAL)
        self.discrepancies_connector = DiscrepancyDBConnector(**DISCREPANCIES_DB_CONFIG_LOCAL)
        self.all_discrepancies = []

    def validate(self):
        # so... I'm not sure whether I should iterate over ALL the saved documents, look for predefined issues
        # should this method receive a document and validate it, or just... run and look for issues.
        # So I'm going to look for the predefined issues and return a list of documents that have issues.
        # Which also means that there are no NOT_PROCESSED issues, since this validation is supposed to run after the parser

        self.collect_short_header_discrepancies()
        self.collect_late_date_discrepancies()
        self.collect_high_sum_discrepancies()
        self.collect_saved_discrepancies()

        return self.all_discrepancies

    def collect_short_header_discrepancies(self) -> None:
        if self.max_headers_length is None:
            return
        documents_with_short_headers = self.validation_connector.find_short_headers(self.max_headers_length)
        for doc in documents_with_short_headers:
            discrepancy = (ValidationStatus.INVALID, {
                "headers": doc['headers'],
                "length": len(str(doc['headers']))
            })
            self.all_discrepancies.append(discrepancy)

    def collect_late_date_discrepancies(self) -> None:
        if self.late_date is None:
            return
        documents_with_late_date = self.validation_connector.find_late_date_of_creation(self.late_date)
        for doc in documents_with_late_date:
            discrepancy = (ValidationStatus.INVALID, {
                "date_of_creation": doc['date_of_creation']
            })
            self.all_discrepancies.append(discrepancy)

    def collect_high_sum_discrepancies(self) -> None:
        if self.high_sum is None:
            return
        documents_with_high_sum = self.validation_connector.find_high_sum_by_precalculated_value(self.high_sum)
        for doc in documents_with_high_sum:
            discrepancy = (ValidationStatus.INVALID, {
                "sum_of_first_row": doc['sum_of_first_row']
            })
            self.all_discrepancies.append(discrepancy)

    def collect_saved_discrepancies(self) -> None:
        saved_discrepancies = self.discrepancies_connector.find({})
        for discrepancy in saved_discrepancies:
            if discrepancy.get('discrepancy_type') in [member.value for name, member in vars(DiscrepancyType).items() if
                                                       name.startswith('MISSING')]:
                self.all_discrepancies.append((ValidationStatus.NOT_FOUND, discrepancy))
            else:
                self.all_discrepancies.append((ValidationStatus.INVALID, discrepancy))


if __name__ == "__main__":
    dv = DocumentValidator(max_headers_length=22, late_date="2020-01-01", high_sum=1000)
    dv.validate()
