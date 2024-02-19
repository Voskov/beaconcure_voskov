import datetime

import pytest

from data_classes.table_document import TableDocument
from db_utils.default_db_config import DEFAULT_DB_CONFIG_REMOTE, DEFAULT_DB_CONFIG_LOCAL
from db_utils.mongo_db_tables_connector import MongoDBTablesConnector

LOCAL_TEST_CONNECTOR = MongoDBTablesConnector(host=DEFAULT_DB_CONFIG_LOCAL['host'],
                                              port=DEFAULT_DB_CONFIG_LOCAL['port'],
                                              db_name='test_db',
                                              collection_name='test_collection')
REMOTE_TEST_CONNECTOR = MongoDBTablesConnector(host=DEFAULT_DB_CONFIG_REMOTE['host'],
                                               port=DEFAULT_DB_CONFIG_REMOTE['port'],
                                               db_name='test_db',
                                               collection_name='test_collection',
                                               username=DEFAULT_DB_CONFIG_REMOTE['username'],
                                               password=DEFAULT_DB_CONFIG_REMOTE['password'])


class TestMongoDBTablesConnector:
    class TestDBConnectorBasicFunctionality:
        @pytest.fixture(autouse=True)
        def setup(self):
            self.connector = LOCAL_TEST_CONNECTOR
            self.connector.db.drop_collection(self.connector.collection.name)

            yield

            self.connector.db.drop_collection(self.connector.collection.name)

        @pytest.fixture(autouse=True)
        def default_document(self):
            self.table_document = TableDocument(**{'document_id': 'Table5999962Lossadjusterchartered',
                                                   'title': 'Table 59.99.9.62 Loss adjuster, chartered',
                                                   'headers': ['Daniel Brown', 'Shane Barnes DDS', 'Nicole Carpenter',
                                                               'Kristin Duarte'],
                                                   'body_by_columns': {'Daniel Brown': {'Roberts LLC': '1060'},
                                                                       'Shane Barnes DDS': {'Roberts LLC': '37'},
                                                                       'Nicole Carpenter': {'Roberts LLC': '1593'},
                                                                       'Kristin Duarte': {'Roberts LLC': '1364'}},
                                                   'body_by_rows': {
                                                       'Roberts LLC': {'Daniel Brown': '1060', 'Shane Barnes DDS': '37',
                                                                       'Nicole Carpenter': '1593',
                                                                       'Kristin Duarte': '1364'}},
                                                   'rows_list': [['Roberts LLC', '1060', '37', '1593', '1364']],
                                                   'sum_of_first_row': 4054, 'footer': 'Creation: 3Feb2013 Chad',
                                                   'country_of_creation': 'Chad',
                                                   'date_of_creation': datetime.datetime(2013, 2, 3, 0, 0)})

        def test_insert(self):
            self.connector.insert(self.table_document)
            assert self.connector.collection.count_documents({}) == 1

        def test_insert_many(self):
            self.connector.insert_many([self.table_document, self.table_document])
            assert self.connector.collection.count_documents({}) == 2

        def test_find(self):
            self.connector.insert(self.table_document)
            res = self.connector.find({"document_id": "Table5999962Lossadjusterchartered"})
            fetched_table = res.next()
            assert self.table_document.dict().items() <= fetched_table.items()
            with pytest.raises(StopIteration):
                res.next()

        def test_find_one(self):
            self.connector.insert(self.table_document)
            res = self.connector.find_one({"document_id": "Table5999962Lossadjusterchartered"})
            assert self.table_document.dict().items() <= res.items()

        def test_update(self):
            self.connector.insert(self.table_document)
            self.connector.update({"document_id": "Table5999962Lossadjusterchartered"},
                                  {"$set": {"title": "new title"}})
            res = self.connector.find_one({"document_id": "Table5999962Lossadjusterchartered"})
            assert res['title'] == "new title"

        def test_upsert(self):
            assert self.connector.collection.count_documents({}) == 0
            self.connector.upsert(self.table_document)
            assert self.connector.collection.count_documents({}) == 1
            self.connector.upsert(self.table_document)
            assert self.connector.collection.count_documents({}) == 1
            assert self.connector.find_one({"document_id": self.table_document.document_id})['title'] == \
                   self.table_document.title

        def test_delete(self):
            self.connector.insert(self.table_document)
            self.connector.delete({"document_id": "Table5999962Lossadjusterchartered"})
            res = self.connector.find_one({"document_id": "Table5999962Lossadjusterchartered"})
            assert res is None

        def test_drop_collection(self):
            self.connector.drop_collection()
            assert self.connector.collection.count_documents({}) == 0
