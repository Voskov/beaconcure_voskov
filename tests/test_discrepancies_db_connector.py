import pytest

from data_classes.discrepancy import Discrepancy, DiscrepancyType
from db_utils.default_db_config import DISCREPANCIES_DB_CONFIG_LOCAL
from db_utils.discrepancy_db_connector import DiscrepancyDBConnector


class TestDiscrepanciesDBConnector:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.connector = DiscrepancyDBConnector(host=DISCREPANCIES_DB_CONFIG_LOCAL['host'],
                                                port=DISCREPANCIES_DB_CONFIG_LOCAL['port'],
                                                db_name='test_db',
                                                collection_name='test_discrepancy_collection')
        self.connector.db.drop_collection(self.connector.collection.name)

        yield

        self.connector.db.drop_collection(self.connector.collection.name)

    @pytest.fixture(autouse=True)
    def default_discrepancy(self):
        self.discrepancy = Discrepancy(
            file_name='test_file_name.html',
            discrepancy_type=DiscrepancyType.MISSING_DOCUMENT_ID,
            raw_data='Some raw data',
            description='Test discrepancy',
            location=1
        )

    def test_insert(self):
        self.connector.insert(self.discrepancy)
        assert self.connector.collection.count_documents({}) == 1

    def test_upsert(self):
        assert self.connector.collection.count_documents({}) == 0
        self.connector.upsert(self.discrepancy)
        assert self.connector.collection.count_documents({}) == 1
        self.connector.upsert(self.discrepancy)
        assert self.connector.collection.count_documents({}) == 1

    def test_insert_many(self):
        another_discrepancy = Discrepancy(
            file_name='another_file_name.html',
            discrepancy_type=DiscrepancyType.MISSING_TITLE,
            raw_data='Some raw data',
            description='Another test discrepancy',
            location=2
        )
        self.connector.insert_many([self.discrepancy, another_discrepancy])
        assert self.connector.collection.count_documents({}) == 2

    def test_find(self):
        self.connector.insert(self.discrepancy)
        res = self.connector.find({"file_name": "test_file_name.html"})
        fetched_discrepancy = res.next()
        assert self.discrepancy.dict().items() <= fetched_discrepancy.items()
        with pytest.raises(StopIteration):
            res.next()

    def test_find_one(self):
        self.connector.insert(self.discrepancy)
        fetched_discrepancy = self.connector.find_one({"file_name": "test_file_name.html"})
        assert self.discrepancy.dict().items() <= fetched_discrepancy.items()

    def test_update(self):
        self.connector.insert(self.discrepancy)
        self.connector.update({"file_name": "test_file_name.html"}, {"$set": {"description": "Updated description"}})
        fetched_discrepancy = self.connector.find_one({"file_name": "test_file_name.html"})
        assert fetched_discrepancy["description"] == "Updated description"

    def test_delete(self):
        self.connector.insert(self.discrepancy)
        self.connector.delete({"file_name": "test_file_name.html"})
        assert self.connector.collection.count_documents({}) == 0
