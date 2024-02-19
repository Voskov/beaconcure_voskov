from datetime import datetime
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

from data_utils.parser import Parser


class TestParser:
    """
    This test suite assumes some hard assumptions about the documents directory and given data
    Assumptions that may not have been assumed for more general use cases
    """

    class TestInternalFunctionalities:
        @pytest.fixture(autouse=True)
        def setup(self):
            self.parser = Parser()
            self.documents_dir = "../documents"
            valid_file_path = Path(self.documents_dir) / "0_table.html"
            with open(valid_file_path, encoding="utf-8") as valid_file:
                self.valid_soup = BeautifulSoup(valid_file, "html.parser")

        def test_get_valid_files(self):
            valid_files = list(self.parser._get_valid_files(Path(self.documents_dir)))
            assert len(valid_files) == 67

        def test_get_document_id(self):
            document_id = self.parser._get_document_id(self.valid_soup)
            assert document_id == "Table5999962Lossadjusterchartered"

        def test_get_title(self):
            title = self.parser._get_title(self.valid_soup)
            assert title == "Table 59.99.9.62 Loss adjuster, chartered"

        def test_get_header(self):
            header = self.parser._get_headers(self.valid_soup)
            assert header == ['Daniel Brown', 'Shane Barnes DDS', 'Nicole Carpenter', 'Kristin Duarte']
            with open(Path(self.documents_dir) / "66_table.html", encoding="utf-8") as file_66:
                soup_66 = BeautifulSoup(file_66, "html.parser")
            headers_66 = self.parser._get_headers(soup_66)
            assert headers_66 == ['Kenneth Decker', 'Benjamin Newman', 'Susan Miller']

        def test_get_body(self):
            expected_body_by_columns = {'Daniel Brown': {'Roberts LLC': '1060'},
                                        'Kristin Duarte': {'Roberts LLC': '1364'},
                                        'Nicole Carpenter': {'Roberts LLC': '1593'},
                                        'Shane Barnes DDS': {'Roberts LLC': '37'}}
            expected_body_by_rows = {'Roberts LLC': {'Daniel Brown': '1060', 'Kristin Duarte': '1364',
                                                     'Nicole Carpenter': '1593', 'Shane Barnes DDS': '37'}}
            expected_rows_list = [['Roberts LLC', '1060', '37', '1593', '1364']]
            body_by_columns, body_by_rows, rows_list = self.parser._get_body(self.valid_soup)
            assert body_by_columns == expected_body_by_columns
            assert body_by_rows == expected_body_by_rows
            assert rows_list == expected_rows_list

        def test_get_body_fill_missing_headers(self):
            with open(Path(self.documents_dir) / "66_table.html", encoding="utf-8") as file_66:
                soup_66 = BeautifulSoup(file_66, "html.parser")
            body_by_columns, body_by_rows, rows_list = self.parser._get_body(soup_66, fill_missing_headers=True)
            expected_body_by_columns = {
                'Kenneth Decker': {'Reeves-George': '699', 'Atkinson and Sons': '846%', 'Hudson-Diaz': '25%',
                                   'Simpson PLC': '1015%', 'Richmond, Garcia and Gonzales': '1202',
                                   'Washington-Riley': '23%'},
                'Benjamin Newman': {'Reeves-George': '1465', 'Atkinson and Sons': '356',
                                    'Hudson-Diaz': '482', 'Simpson PLC': '774%',
                                    'Richmond, Garcia and Gonzales': '1092', 'Washington-Riley': '1518%'},
                'Susan Miller': {'Reeves-George': '1281', 'Atkinson and Sons': '850%', 'Hudson-Diaz': '368%',
                                 'Simpson PLC': '372', 'Richmond, Garcia and Gonzales': '1516%',
                                 'Washington-Riley': '930%'},
                'empty_header_0': {'Reeves-George': '587', 'Atkinson and Sons': '1060',
                                   'Hudson-Diaz': '1143', 'Simpson PLC': '1370',
                                   'Richmond, Garcia and Gonzales': '611', 'Washington-Riley': '474%'}}
            expected_body_by_rows = {
                'Reeves-George': {'Kenneth Decker': '699', 'Benjamin Newman': '1465', 'Susan Miller': '1281',
                                  'empty_header_0': '587'},
                'Atkinson and Sons': {'Kenneth Decker': '846%', 'Benjamin Newman': '356', 'Susan Miller': '850%',
                                      'empty_header_0': '1060'},
                'Hudson-Diaz': {'Kenneth Decker': '25%', 'Benjamin Newman': '482', 'Susan Miller': '368%',
                                'empty_header_0': '1143'},
                'Simpson PLC': {'Kenneth Decker': '1015%', 'Benjamin Newman': '774%', 'Susan Miller': '372',
                                'empty_header_0': '1370'},
                'Richmond, Garcia and Gonzales': {'Kenneth Decker': '1202', 'Benjamin Newman': '1092',
                                                  'Susan Miller': '1516%', 'empty_header_0': '611'},
                'Washington-Riley': {'Kenneth Decker': '23%', 'Benjamin Newman': '1518%', 'Susan Miller': '930%',
                                     'empty_header_0': '474%'}}
            expected_rows_list = [['Reeves-George', '699', '1465', '1281', '587'],
                                  ['Atkinson and Sons', '846%', '356', '850%', '1060'],
                                  ['Hudson-Diaz', '25%', '482', '368%', '1143'],
                                  ['Simpson PLC', '1015%', '774%', '372', '1370'],
                                  ['Richmond, Garcia and Gonzales', '1202', '1092', '1516%', '611'],
                                  ['Washington-Riley', '23%', '1518%', '930%', '474%']]
            assert body_by_columns == expected_body_by_columns
            assert body_by_rows == expected_body_by_rows
            assert rows_list == expected_rows_list

        def test_get_body_drop_missing_headers(self):
            with open(Path(self.documents_dir) / "66_table.html", encoding="utf-8") as file_66:
                soup_66 = BeautifulSoup(file_66, "html.parser")
            body_by_columns, body_by_rows, rows_list = self.parser._get_body(soup_66, fill_missing_headers=False)
            expected_body_by_columns = {
                'Kenneth Decker': {'Reeves-George': '699', 'Atkinson and Sons': '846%', 'Hudson-Diaz': '25%',
                                   'Simpson PLC': '1015%', 'Richmond, Garcia and Gonzales': '1202',
                                   'Washington-Riley': '23%'},
                'Benjamin Newman': {'Reeves-George': '1465', 'Atkinson and Sons': '356',
                                    'Hudson-Diaz': '482', 'Simpson PLC': '774%',
                                    'Richmond, Garcia and Gonzales': '1092', 'Washington-Riley': '1518%'},
                'Susan Miller': {'Reeves-George': '1281', 'Atkinson and Sons': '850%', 'Hudson-Diaz': '368%',
                                 'Simpson PLC': '372', 'Richmond, Garcia and Gonzales': '1516%',
                                 'Washington-Riley': '930%'}}
            expected_body_by_rows = {
                'Reeves-George': {'Kenneth Decker': '699', 'Benjamin Newman': '1465', 'Susan Miller': '1281'},
                'Atkinson and Sons': {'Kenneth Decker': '846%', 'Benjamin Newman': '356', 'Susan Miller': '850%'},
                'Hudson-Diaz': {'Kenneth Decker': '25%', 'Benjamin Newman': '482', 'Susan Miller': '368%'},
                'Simpson PLC': {'Kenneth Decker': '1015%', 'Benjamin Newman': '774%', 'Susan Miller': '372'},
                'Richmond, Garcia and Gonzales': {'Kenneth Decker': '1202', 'Benjamin Newman': '1092',
                                                  'Susan Miller': '1516%'},
                'Washington-Riley': {'Kenneth Decker': '23%', 'Benjamin Newman': '1518%', 'Susan Miller': '930%'}}
            expected_rows_list = [['Reeves-George', '699', '1465', '1281', '587'],
                                  ['Atkinson and Sons', '846%', '356', '850%', '1060'],
                                  ['Hudson-Diaz', '25%', '482', '368%', '1143'],
                                  ['Simpson PLC', '1015%', '774%', '372', '1370'],
                                  ['Richmond, Garcia and Gonzales', '1202', '1092', '1516%', '611'],
                                  ['Washington-Riley', '23%', '1518%', '930%', '474%']]
            assert body_by_columns == expected_body_by_columns
            assert body_by_rows == expected_body_by_rows
            assert rows_list == expected_rows_list

        def test_get_footer(self):
            footer = self.parser._get_footer(self.valid_soup)
            assert footer == "Creation: 3Feb2013 Chad"

        @pytest.mark.parametrize("file_name, expected",
                                 [("0_table.html", "Chad"),
                                  ("50_table.html", "Bosnia and Herzegovina"),
                                  ("55_table.html", "Bosnia and Herzegovina")])
        def test_get_country_of_creation(self, file_name, expected):
            with open(Path(self.documents_dir) / file_name, encoding="utf-8") as table_file:
                soup = BeautifulSoup(table_file, "html.parser")
            country_of_creation = self.parser._get_country_of_creation(soup)
            assert country_of_creation == expected

        @pytest.mark.parametrize("file_name, expected",
                                 [("0_table.html", datetime(2013, 2, 3)),
                                  ("50_table.html", datetime(2022, 8, 31))])
        def test_get_date_of_creation(self, file_name, expected):
            with open(Path(self.documents_dir) / file_name, encoding="utf-8") as table_file:
                soup = BeautifulSoup(table_file, "html.parser")
            date_of_creation = self.parser._get_date_of_creation(soup)
            assert isinstance(date_of_creation, datetime)
            assert date_of_creation == expected

        def test_get_bad_date_of_creation(self):
            with open(Path(self.documents_dir) / "60_table.html", encoding="utf-8") as table_file:
                soup_60 = BeautifulSoup(table_file, "html.parser")
            date_of_creation = self.parser._get_date_of_creation(soup_60)
            assert date_of_creation is None
