import logging
import pymongo
import abc
import time
import json
import timeit
import urllib.parse
import bson.json_util
import pymongo.errors

from src.executor import AbstractBenchmarkRunnable

logger = logging.getLogger(__name__)


class AbstractMongoDBRunnable(AbstractBenchmarkRunnable, abc.ABC):
    def __init__(self, **kwargs):
        super().__init__(working_system='MongoDB', **kwargs)
        self.database_uri = f'mongodb://{urllib.parse.quote_plus(self.config["username"])}:' \
                            f'{urllib.parse.quote_plus(self.config["password"])}@' \
                            f'{self.config["database"]["address"]}' + \
                            f':{self.config["database"]["port"]}'
        self.client = pymongo.MongoClient(self.database_uri)
        self.database = self.client[self.config['database']['name']]

    @staticmethod
    def format_strict(result):
        return json.loads(bson.json_util.dumps(result))

    def execute_mongoimport(self, file_path, collection):
        mongoimport_command = self.config['importCommand'][:-1] + [
            self.config['importCommand'][-1] + ' ' + ' '.join(f"""
                --authenticationDatabase admin
                --db {self.config['database']['name']}
                --collection {collection}
                --host {self.config['database']['address']}:{self.config['database']['port']}
                --username {self.config['username']}
                --password {self.config['password']}
                --quiet
                --drop 
                {file_path}
            """.split())
        ]
        return {'response': self.call_subprocess(mongoimport_command)}

    def initialize_database(self):
        self.drop_database()  # Start from a clean database.
        self.database = self.client[self.config['database']['name']]

        logger.info('Initializing database with a collection and a document.')
        init_collection = self.database['InitCollection']
        init_collection.insert_one({"a": 1})

        logger.info('Dropping the initialization collection.')
        init_collection.drop()
        if 'InitCollection' not in self.database.collection_names():
            logger.info(f'Collection dropped successfully.')
        else:
            logger.error(f'Collection could not be dropped! Server status: {self.client.admin.command("serverStatus")}')

    def drop_database(self):
        self.client.drop_database(self.database)
        logger.info(f'Dropping database {self.database}.')
        if self.database not in self.client.list_database_names():
            logger.info(f'Database dropped successfully.')
        else:
            logger.error(f'Database could not be dropped! Server status: {self.client.admin.command("serverStatus")}')

    def drop_collection(self, name):
        collection = self.database[name]
        collection.drop()

        if name not in self.database.collection_names():
            logger.info(f'Collection dropped successfully.')
        else:
            logger.error(f'Collection could not be dropped! Server status: {self.client.admin.command("serverStatus")}')

    def create_collection(self, name):
        self.database.create_collection(name)
        collection = self.database[name]

        if collection.count() == 0:
            logger.info('Collection successfully created.')
        else:
            logger.error('Could not create the collection successfully.')

    def execute_insert(self, name, documents, primary_key='_id'):
        # Insert into our buffer dataset.
        buffer_collection = self.database[name + 'Buffer']
        buffer_collection.insert(documents)

        # Perform the insert.
        t_before = timeit.default_timer()
        query_results = [self.format_strict(r) for r in buffer_collection.aggregate([
            {'$match': {}},
            {'$merge': {
                'into': name,
                'on': primary_key,
                'whenMatched': 'fail'
            }}
        ], allowDiskUse=True)]
        client_time = timeit.default_timer() - t_before

        # Remove all from our buffer dataset.
        buffer_collection.delete_many({})

        return {'queryResults': query_results, 'clientTime': client_time}

    def execute_upsert(self, name, documents, primary_key='_id'):
        # Insert into our buffer dataset.
        buffer_collection = self.database[name + 'Buffer']
        buffer_collection.insert(documents)

        # Perform the upsert.
        t_before = timeit.default_timer()
        query_results = [self.format_strict(r) for r in buffer_collection.aggregate([
            {'$match': {}},
            {'$merge': {
                'into': name,
                'on': primary_key,
                'whenMatched': 'replace'
            }}
        ], allowDiskUse=True)]
        client_time = timeit.default_timer() - t_before

        # Remove all from our buffer dataset.
        buffer_collection.delete_many({})

        return {'queryResults': query_results, 'clientTime': client_time}

    def execute_delete(self, name, predicate):
        collection = self.database[name]

        # Perform the delete.
        t_before = timeit.default_timer()
        delete_results = collection.delete_many(predicate).raw_result
        client_time = timeit.default_timer() - t_before

        return {'queryResults': delete_results, 'clientTime': client_time}

    def execute_select(self, name, predicate=None, aggregate=None, timeout=None):
        collection = self.database[name]

        if predicate is None and aggregate is None:
            raise ValueError("Either predicate or aggregate must be specified.")

        elif predicate is not None and aggregate is not None:
            raise ValueError("Both predicate and aggregate cannot be specified at the same time.")

        try:
            if predicate is not None:
                t_before = timeit.default_timer()
                query_results = [self.format_strict(r) for r in collection.find(predicate, max_time_ms=timeout)]
                client_time = timeit.default_timer() - t_before
                status = 'success'

            else:  # aggregate is not None
                t_before = timeit.default_timer()
                query_results = [self.format_strict(r) for r in
                                 collection.aggregate(aggregate, allowDiskUse=True, maxTimeMS=timeout)]
                client_time = timeit.default_timer() - t_before
                status = 'success'

        except pymongo.errors.ExecutionTimeout:
            query_results = None
            client_time = timeout
            status = 'timeout'

        return {'queryResults': query_results, 'clientTime': client_time, 'status': status}

    def restart_db(self):
        super(AbstractMongoDBRunnable, self).restart_db()
        while True:
            time.sleep(10)
            try:
                self.client.admin.command('ping')
                break
            except pymongo.mongo_client.ConnectionFailure:
                logger.warning(f'Instance is not currently online. Trying again in 10 seconds...')
