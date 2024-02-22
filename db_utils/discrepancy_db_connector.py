from typing import List, Any

import pymongo

from data_classes.discrepancy import Discrepancy
from db_utils.base_mongo_db_connector import BaseMongoDBConnector
from db_utils.config_loader import load_local_env_config


class DiscrepancyDBConnector(BaseMongoDBConnector):
    _shared_state: dict[Any, Any] = {}

    def __init__(self, host=None, port=None, db_name=None,
                 collection_name=None, username=None, password=None):
        self.__dict__ = self._shared_state
        if not self._shared_state:
            self.client = pymongo.MongoClient(host, port, username=username, password=password)
            self.db = self.client[db_name]
            self.collection = self.db[collection_name]

    @staticmethod
    def get_local_connector():
        local_env_conf = load_local_env_config()
        return DiscrepancyDBConnector(host=local_env_conf['HOST'],
                                      port=int(local_env_conf['PORT']),
                                      db_name=local_env_conf['TABLES_DB_NAME'],
                                      collection_name=local_env_conf['DISCREPANCIES_COLLECTION_NAME'])

    def insert(self, discrepancy: Discrepancy):
        self.collection.insert_one(discrepancy.dict())

    def insert_many(self, discrepancies: List[Discrepancy]):
        self.collection.insert_many([discrepancy.dict() for discrepancy in discrepancies])

    def upsert(self, discrepancy: Discrepancy):
        self.collection.replace_one({'file_name': discrepancy.file_name},
                                    discrepancy.dict(), upsert=True)

    def upsert_many(self, discrepancies: List[Discrepancy]):
        for discrepancy in discrepancies:
            self.upsert(discrepancy)
