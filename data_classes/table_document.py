from datetime import datetime

from pydantic import BaseModel


class TableDocument(BaseModel):
    document_id: str | None
    title: str | None
    headers: list[str] | None
    body_by_columns: dict | None
    body_by_rows: dict | None
    rows_list: list | None
    sum_of_first_row: int | None
    footer: str | None
    country_of_creation: str | None
    date_of_creation: datetime | None
