from dateutil.parser import parse

from db_utils.config_loader import load_local_env_config
from db_utils.mongo_db_tables_connector import MongoDBTablesConnector


class ValidationConnector(MongoDBTablesConnector):
    _shared_state = {}

    def __init__(self, host=None, port=None, db_name=None, collection_name=None, username=None, password=None):
        self.__dict__ = self._shared_state
        if not self._shared_state:
            super().__init__(host, port, db_name, collection_name, username, password)

    @staticmethod
    def get_local_connector():
        local_env_conf = load_local_env_config()
        return ValidationConnector(host=local_env_conf['HOST'],
                                   port=int(local_env_conf['PORT']),
                                   db_name=local_env_conf['TABLES_DB_NAME'],
                                   collection_name=local_env_conf['TABLES_COLLECTION_NAME'])

    def find_short_headers(self, length: int):
        '''
        find the tables were the headers are shorter than a given length
        :param length:
        :return:
        '''
        query = {"$where": f"JSON.stringify(this.headers).length < {length}"}
        return self.collection.find(query)

    def find_late_date_of_creation(self, date_str: str):
        date = parse(date_str)
        query = {"date_of_creation": {"$gt": date}}
        return self.collection.find(query)

    def find_high_sum_by_precalculated_value(self, given_sum: int):
        query = {"sum_of_first_row": {"$gt": given_sum}}
        return self.collection.find(query)

    def find_high_sum_by_query(self, given_sum: int):
        query = {
            "$expr": {
                "$gt": [
                    {
                        "$sum": {
                            "$map": {
                                "input": {"$slice": [{"$arrayElemAt": ["$rows_list", 0]}, 1, 111111]},
                                "in": {"$toInt": "$$this"}
                            }
                        }
                    },
                    given_sum
                ]
            }
        }
        return self.collection.find(query)
