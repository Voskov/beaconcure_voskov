from dataclasses import dataclass
from enum import Enum


class DiscrepancyType(Enum):
    MISSING_DOCUMENT_ID = 'MISSING_DOCUMENT_ID'
    MISSING_TITLE = 'MISSING_TITLE'
    MISSING_HEADERS = 'MISSING_HEADERS'
    MISSING_BODY = 'MISSING_BODY'
    MISSING_FOOTER = 'MISSING_FOOTER'
    MISSING_COUNTRY = 'MISSING_COUNTRY'
    MISSING_CREATION_DATE = 'MISSING_CREATION_DATE'
    INCORRECT_CREATION_DATE = 'INCORRECT_CREATION_DATE'
    INVALID_SUM = 'INVALID_SUM'


@dataclass  # Why dataclass and not pydantic? Because we don't need to validate the data, we just need to store it.
class Discrepancy:
    discrepancy_type: DiscrepancyType
    file_name: str = None
    raw_data: str = None
    description: str = None
    location: int = None

    def dict(self):
        return {
            "file_name": self.file_name,
            "discrepancy_type": self.discrepancy_type.value,
            "raw_data": self.raw_data,
            "description": self.description,
            "location": self.location
        }
