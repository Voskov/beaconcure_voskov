import datetime
import inspect
import re
from collections import defaultdict
from pathlib import Path
from typing import List, Optional

import pycountry
from bs4 import BeautifulSoup
from dateutil.parser import parse, ParserError
from loguru import logger

from data_classes.discrepancy import Discrepancy, DiscrepancyType
from data_classes.table_document import TableDocument
from data_utils.parsing_config import TableParseTags
from db_utils.default_db_config import DEFAULT_DB_CONFIG_LOCAL, DISCREPANCIES_DB_CONFIG_LOCAL
from db_utils.discrepancy_db_connector import DiscrepancyDBConnector
from db_utils.mongo_db_tables_connector import MongoDBTablesConnector

VALID_FILE_TYPES = [".html", ".htm"]



class Parser:
    def __init__(self):
        self.all_discrepancies = defaultdict(list)
        self.file_discrepancies = []
        self.tables_db_client = MongoDBTablesConnector(**DEFAULT_DB_CONFIG_LOCAL)
        self.discrepancies_db_client = DiscrepancyDBConnector(**DISCREPANCIES_DB_CONFIG_LOCAL)

    def parse(self, path_str: str):
        path = Path(path_str)
        # A simple and naive approach is to parse each document and then insert it into the database, one by one.
        # This approach is simple and easy to implement, but it's not efficient since it will open a new connection to the database for each document.
        #
        # A more efficient approach would be to parse all the documents and then insert them all at once.
        # Or batch them in groups and then insert them in batches.
        # But then I may be required to handle cases like huge documents or other unexpected cases.
        #
        # Since I insert these documents once, and I don't expect to have a large number of documents,
        # I've decided to go with the simple and naive approach.
        files_in_path = self._get_valid_files(path)
        for file in files_in_path:
            if table_document := self.parse_file(file):
                logger.debug(f'Inserting document: {file.name}')
                self.tables_db_client.upsert(table_document)

        if self.all_discrepancies:
            for file_name, discrepancies in self.all_discrepancies.items():
                logger.warning(f'Saving discrepancies for file: {file_name}')
                self.discrepancies_db_client.insert_many(discrepancies)

    def parse_file(self, file_path: Path) -> TableDocument | None:
        with open(file_path, encoding='utf-8') as f:
            self.file_discrepancies = []
            soup = BeautifulSoup(f, 'html.parser')
            # I could use some kind of complex single helper function to parse the whole document,
            # bud I've decided to use a simple and straightforward approach to parse each part of the document separately.
            document_id = self._get_document_id(soup)
            title = self._get_title(soup)
            headers = self._get_headers(soup)
            body_by_columns, body_by_rows, rows_list = self._get_body(soup)
            if rows_list:
                sum_of_first_row = self._get_sum_of_first_row(rows_list[0])
            footer = self._get_footer(soup)
            country_of_creation = self._get_country_of_creation(soup)
            date_of_creation = self._get_date_of_creation(soup)

            for discrepancy in self.file_discrepancies:
                discrepancy.file_name = file_path.name
            if self.file_discrepancies:
                self.all_discrepancies[file_path.name] = self.file_discrepancies
            return TableDocument(document_id=document_id,
                                 title=title,
                                 headers=headers,
                                 body_by_columns=body_by_columns,
                                 body_by_rows=body_by_rows,
                                 rows_list=rows_list,
                                 sum_of_first_row=sum_of_first_row,
                                 footer=footer,
                                 country_of_creation=country_of_creation,
                                 date_of_creation=date_of_creation)

    @staticmethod
    def _get_valid_files(path: Path):
        if not path.exists() or not path.is_dir():
            raise FileNotFoundError(f'Invalid path: {path}')
        # yield a file only if it's a html file
        for file_path in path.iterdir():
            if file_path.is_file() and file_path.suffix in VALID_FILE_TYPES:
                yield file_path

    def _get_document_id(self, soup: BeautifulSoup) -> Optional[str]:
        table_tag = soup.find(TableParseTags.table)
        if document_id := table_tag and table_tag.get('id'):
            return str(document_id)
        else:
            discrepancy = Discrepancy(DiscrepancyType.MISSING_DOCUMENT_ID)
            if table_tag:
                discrepancy.raw_data = str(table_tag)
            self.file_discrepancies.append(discrepancy)
            return None

    def _get_title(self, soup: BeautifulSoup) -> str | None:
        title_tag = soup.find(TableParseTags.caption)
        if not title_tag:
            discrepancy = Discrepancy(DiscrepancyType.MISSING_TITLE, description='No title tag found')
            self.file_discrepancies.append(discrepancy)
            return None
        if title := title_tag.text.strip():
            return title
        else:
            discrepancy = Discrepancy(DiscrepancyType.MISSING_TITLE, raw_data=str(title_tag),
                                      description='Empty title tag',
                                      location=int(title_tag.sourceline))
            self.file_discrepancies.append(discrepancy)
            return None

    def _get_headers(self, soup: BeautifulSoup) -> List[str] | None:
        head_tag = soup.find(TableParseTags.table_head)
        if not head_tag:
            discrepancy = Discrepancy(DiscrepancyType.MISSING_HEADERS, description='No thead tag found')
            self.file_discrepancies.append(discrepancy)
            return None
        tags = head_tag.find_all(TableParseTags.table_header)
        headers = [header for header in (tag.text.strip() for tag in tags) if header]
        if headers:
            return headers
        else:
            discrepancy = Discrepancy(DiscrepancyType.MISSING_HEADERS, raw_data=str(tags), description='Empty headers',
                                      location=tags[0].sourceline)
            self.file_discrepancies.append(discrepancy)
            return None

    def _get_body(self, soup: BeautifulSoup, fill_missing_headers=True) -> tuple[
        Optional[dict], Optional[dict], Optional[list]]:
        """
        So I wasn't sure how I'd want to a approach this
        My intuition told me to that we may later want to access the data by columns or by rows
        So I've decided to parse the data in both ways and return them both.
        But the assignment requires the first row to be summed, so I've decided to return the rows as a list as well.
        """

        headers = self._get_headers(soup) or []
        tbody_tag = soup.find(TableParseTags.table_body)
        if not tbody_tag:
            discrepancy = Discrepancy(DiscrepancyType.MISSING_BODY, description='No tbody tag found')
            self.file_discrepancies.append(discrepancy)
            return None, None, None
        raw_rows = tbody_tag.find_all('tr')
        columns_parsed = defaultdict(dict)
        rows_parsed = defaultdict(dict)
        rows_list = []
        if fill_missing_headers:
            self._fill_missing_headers(headers, raw_rows[0])
        for raw_row in raw_rows:
            all_cells = raw_row.find_all('td')
            row_name = all_cells[0].text.strip()
            rows_list.append([cell.text.strip() for cell in all_cells])
            data_cells = all_cells[1:] if fill_missing_headers else all_cells[1:len(headers) + 1]
            for column_index, data_cell in enumerate(data_cells):
                columns_parsed[headers[column_index]][row_name] = data_cell.text.strip()
                rows_parsed[row_name][headers[column_index]] = data_cell.text.strip()

        return dict(columns_parsed), dict(rows_parsed), rows_list

    def _get_sum_of_first_row(self, row):
        """
        The assignment requires a sum of the first row
        If it's REALLY useful, I may want to precalculate it beforehand and store it in the database
        So that's what I did here
        """
        if not all([cell.isdigit() for cell in row[1:]]):
            discrepancy = Discrepancy(DiscrepancyType.INVALID_SUM, raw_data=row,
                                      description="First row doesn't contain only numbers")
            self.file_discrepancies.append(discrepancy)
            return None
        return sum([int(cell) for cell in row[1:] if cell.isdigit()])

    def _get_footer(self, soup: BeautifulSoup) -> str | None:
        footer_tag = soup.find(TableParseTags.table_footer)
        if not footer_tag:
            if self._is_called_by_parse_file():
                discrepancy = Discrepancy(DiscrepancyType.MISSING_FOOTER, description='No tfoot tag found')
                self.file_discrepancies.append(discrepancy)
            return None
        if footer_text := footer_tag.text.strip():
            return footer_text
        else:
            if self._is_called_by_parse_file():
                discrepancy = Discrepancy(DiscrepancyType.MISSING_FOOTER, raw_data=footer_tag,
                                          description='Empty footer tag',
                                          location=footer_tag.sourceline)
                self.file_discrepancies.append(discrepancy)
            return None

    @staticmethod
    def _is_called_by_parse_file() -> bool:
        current_frame = inspect.currentframe()
        outer_frame = inspect.getouterframes(current_frame, 2)
        called_by_parse_file = outer_frame[2][3] == 'parse_file'
        return called_by_parse_file

    def _get_country_of_creation(self, soup: BeautifulSoup) -> str | None:
        footer_str = self._get_footer(soup)
        if not footer_str:
            return None
        date_of_creation_str = self._get_date_str(footer_str)
        footer_str = footer_str.replace('Creation: ',
                                        '').strip()  # I'll assume that every footer starts with "Creation: "
        if date_of_creation_str:
            footer_str = (footer_str.replace(date_of_creation_str, '')).strip()
        if footer_str:
            # If we're already digging into it, let's do some validation and refinement
            # Of course, none of this is necessary, we could simply return `footer_str`
            # but I've decided to take some liberty here and do some validation and refinement
            try:
                country = pycountry.countries.search_fuzzy(footer_str)
                return country[0].name if country else footer_str
            except LookupError:
                # that's not a discrapency, it's just a warning
                logger.warning(f'Failed to find country: {footer_str}')
            return footer_str
        else:
            discrepancy = Discrepancy(discrepancy_type=DiscrepancyType.MISSING_COUNTRY,
                                      raw_data=footer_str,
                                      description="Didn't find country in footer",
                                      location=footer_tag.sourceline if (
                                          footer_tag := soup.find(TableParseTags.table_footer)) else None)
            footer_tag = soup.find(TableParseTags.table_footer)
            discrepancy.location = footer_tag.sourceline
            self.file_discrepancies.append(discrepancy)
            return None

    def _get_date_of_creation(self, soup: BeautifulSoup) -> datetime.datetime | None:
        footer_str = self._get_footer(soup)
        if not footer_str:
            return None
        # date_str = footer_str.split(' ', 2)[1]  # to simple
        if date_str := self._get_date_str(footer_str):
            try:
                res = parse(date_str)
                logger.debug(f'Date parsed: {date_str} -> {res}')
                return res
            except (ValueError, ParserError):
                # Taking some liberty here and decided to log the error and return None for a bad date.
                logger.warning(f'Failed to parse date from footer: {footer_str}')
                discrepancy = Discrepancy(DiscrepancyType.INCORRECT_CREATION_DATE, raw_data=footer_str,
                                          description='Failed to parse date from footer')
                footer_tag = soup.find(TableParseTags.table_footer)
                discrepancy.location = footer_tag.sourceline
                return None

        else:
            discrepancy = Discrepancy(DiscrepancyType.MISSING_CREATION_DATE, raw_data=footer_str,
                                      description="Didn't find date in footer")
            footer_tag = soup.find(TableParseTags.table_footer)
            discrepancy.location = footer_tag.sourceline
            self.file_discrepancies.append(discrepancy)
            return None

    @staticmethod
    def _fill_missing_headers(headers, row):
        '''
        I've stumbled upon a table that has more columns than headers,
        so I took some liberty here and 've added this function to fill the missing headers with empty headers.
        Normally I would have consulted with the product owner or the client to see if this is a valid case or an error,
        But since at the time I was working on it, there was no such person, I've decided to take this liberty.
        '''
        number_of_columns = len(row.find_all('td')) - 1
        if len(headers) == number_of_columns:
            return
        for i in range(number_of_columns - len(headers)):
            headers.append(f'empty_header_{i}')

    @staticmethod
    def _get_date_str(footer_str: str) -> str | None:
        re_pattern = r'(\d{1,2}[A-Za-z]{3,9}\d{2,4})'  # 1-2 digits, 3-9 letters, 2-4 digits
        if match := re.search(re_pattern, footer_str):
            return match.group(0)
        return None


if __name__ == '__main__':
    parser = Parser()
    parser.parse('../documents/')
