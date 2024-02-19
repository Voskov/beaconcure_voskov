from enum import Enum


class ValidationStatus(Enum):
    VALID = "VALID"
    INVALID = "INVALID"
    ERROR = "ERROR"
    NOT_FOUND = "NOT_FOUND"
    NOT_PROCESSED = "NOT_PROCESSED"
