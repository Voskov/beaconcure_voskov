from typing import List, Any

import pymongo
from loguru import logger

from data_classes.table_document import TableDocument
from db_utils.base_mongo_db_connector import BaseMongoDBConnector
from db_utils.config_loader import load_local_env_config
from db_utils.default_db_config import DEFAULT_DB_CONFIG_REMOTE


class MongoDBTablesConnector(BaseMongoDBConnector):
    """
    This is a specific and dedicated class to connect to the specific MongoDB database.
    It can be generalized and use heirarchy and inperfaces to make it more flexible and reusable with different database types and configurations.
    Or used with different tables and databases within the same MongoDB instance.
    But I've decided so skip this step for now, as it's not necessary for the current project.
    It is a singleton class, meaning that it will only create one instance of the class and share it across all instances of the class.
    """
    _shared_state: dict[Any, Any] = {}

    def __init__(self, host=None, port=None, db_name=None, collection_name=None, username=None, password=None):
        # simple singleton implementation
        self.__dict__ = self._shared_state
        if not self._shared_state:
            super().__init__(host, port, db_name, collection_name, username, password)

    @staticmethod
    def get_local_connector():
        local_env_conf = load_local_env_config()
        return MongoDBTablesConnector(host=local_env_conf['HOST'],
                                      port=int(local_env_conf['PORT']),
                                      db_name=local_env_conf['TABLES_DB_NAME'],
                                      collection_name=local_env_conf['TABLES_COLLECTION_NAME'])

    def insert(self, table_document: TableDocument):
        self.collection.insert_one(table_document.dict())

    def insert_many(self, table_documents: List[TableDocument]):
        self.collection.insert_many([table_document.dict() for table_document in table_documents])

    def upsert(self, table_document: TableDocument):
        # I use replace_one instead of update_one, because In this case, I use it for insertion rather than updating.
        # I don't want to overwrite the 'insert', I want to 'insert if not exists'.
        # usually I'd use upsert for updating, but in this case, I use it for insertion.
        self.collection.replace_one({'document_id': table_document.document_id}, table_document.dict(), upsert=True)
