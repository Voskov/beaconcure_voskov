from logging import getLogger

import pymongo

from db_utils.default_db_config import DEFAULT_DB_CONFIG_REMOTE

logger = getLogger(__name__)


class BaseMongoDBConnector:
    """
    This is a specific and dedicated class to connect to the specific MongoDB database.
    It can be generalized and use heirarchy and inperfaces to make it more flexible and reusable with different database types and configurations.
    Or used with different tables and databases within the same MongoDB instance.
    But I've decided so skip this step for now, as it's not necessary for the current project.
    It is a singleton class, meaning that it will only create one instance of the class and share it across all instances of the class.
    """
    _shared_state = {}

    def __init__(self, host=None, port=None, db_name=None, collection_name=None, username=None, password=None):
        """
        simple singleton implementation
        """
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

    def find(self, query: dict):
        return self.collection.find(query)

    def find_one(self, query: dict):
        return self.collection.find_one(query)

    def update(self, query: dict, update: dict):
        self.collection.update_one(query, update)

    def delete(self, query: dict):
        self.collection.delete_one(query)

    def drop_collection(self):
        self.db.drop_collection(self.collection.name)

    def close(self):
        self.client.close()
