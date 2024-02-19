from logging import getLogger
from typing import List

import pymongo

from data_classes.table_document import TableDocument
from db_utils.default_db_config import DEFAULT_DB_CONFIG_REMOTE
from db_utils.base_mongo_db_connector import BaseMongoDBConnector

logger = getLogger(__name__)


class MongoDBTablesConnector(BaseMongoDBConnector):
    """
    This is a specific and dedicated class to connect to the specific MongoDB database.
    It can be generalized and use heirarchy and inperfaces to make it more flexible and reusable with different database types and configurations.
    Or used with different tables and databases within the same MongoDB instance.
    But I've decided so skip this step for now, as it's not necessary for the current project.
    It is a singleton class, meaning that it will only create one instance of the class and share it across all instances of the class.
    """
    _shared_state = {}

    def __init__(self, host=None, port=None, db_name=None, collection_name=None, username=None, password=None):
        # simple singleton implementation
        self.__dict__ = self._shared_state
        if not self._shared_state:
            # defaulting to the default db config
            if not all([host, port, db_name, collection_name]):
                if any([host, port, db_name, collection_name]):
                    logger.warning("Incomplete db config, using default db config")
                else:
                    logger.info("No db config provided, using default db config")

                host = DEFAULT_DB_CONFIG_REMOTE['hostname']
                port = DEFAULT_DB_CONFIG_REMOTE['port']
                db_name = DEFAULT_DB_CONFIG_REMOTE['db_name']
                collection_name = DEFAULT_DB_CONFIG_REMOTE['collection_name']

            self.client = pymongo.MongoClient(host, port, username=username, password=password)
            self.db = self.client[db_name]
            self.collection = self.db[collection_name]

    def insert(self, table_document: TableDocument):
        self.collection.insert_one(table_document.dict())

    def insert_many(self, table_documents: List[TableDocument]):
        self.collection.insert_many([table_document.dict() for table_document in table_documents])

    def upsert(self, table_document: TableDocument):
        # I use replace_one instead of update_one, because In this case, I use it for insertion rather than updating.
        # I don't want to overwrite the 'insert', I want to 'insert if not exists'.
        # usually I'd use upsert for updating, but in this case, I use it for insertion.
        self.collection.replace_one({'document_id': table_document.document_id}, table_document.dict(), upsert=True)
