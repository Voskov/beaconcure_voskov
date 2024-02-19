from typing import List

import pymongo

from data_classes.discrepancy import Discrepancy
from db_utils.base_mongo_db_connector import BaseMongoDBConnector


class DiscrepancyDBConnector(BaseMongoDBConnector):
    _shared_state = {}

    def __init__(self, host=None, port=None, db_name=None, collection_name=None, username=None, password=None):
        self.__dict__ = self._shared_state
        if not self._shared_state:
            self.client = pymongo.MongoClient(host, port, username=username, password=password)
            self.db = self.client[db_name]
            self.collection = self.db[collection_name]

    def insert(self, discrepancy: Discrepancy):
        self.collection.insert_one(discrepancy.dict())

    def insert_many(self, discrepancies: List[Discrepancy]):
        self.collection.insert_many([discrepancy.dict() for discrepancy in discrepancies])

    def upsert(self, discrepancy: Discrepancy):
        self.collection.replace_one({'file_name': discrepancy.file_name}, discrepancy.dict(), upsert=True)

    def upsert_many(self, discrepancies: List[Discrepancy]):
        for discrepancy in discrepancies:
            self.upsert(discrepancy)
