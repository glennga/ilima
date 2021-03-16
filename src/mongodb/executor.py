import logging
import pymongo
import abc
import time
import urllib.parse
import json


from src.executor import AbstractBenchmarkRunnable

logger = logging.getLogger(__name__)


class AbstractMongoDBRunnable(AbstractBenchmarkRunnable, abc.ABC):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.database_uri = f'mongodb://{urllib.parse.quote_plus(self.config["username"])}:' \
                            f'{urllib.parse.quote_plus(self.config["password"])}@' \
                            f'{self.config["database"]["address"]}' + \
                            f':{self.config["database"]["port"]}'
        self.client = pymongo.MongoClient(self.database_uri)
        self.database = self.client[self.config['database']['name']]
        # self.database.set_profiling_level(2)

    def _execute_mongocommand(self, command):
        mongo_command = self.config['mongoCommand'][:-1] + [
            self.config['mongoCommand'][-1] + ' ' + ' '.join(f"""
                --username {self.config['username']}
                --password {self.config['password']}
                --eval '{" ".join(command.split())}'
            """.split())
        ]
        raw_execution_output = self.call_subprocess(mongo_command, is_log=False)

        try:
            return {
                'mongoCommand': mongo_command,
                'response': json.loads(raw_execution_output[:-1].split('\n')[-1])
            }
        except json.JSONDecodeError:
            return {
                'mongoCommand': mongo_command,
                'output': raw_execution_output,
                'error': True
            }

    def execute_mongoimport(self, file_path, collection):
        mongoimport_command = self.config['importCommand'][:-1] + [
            self.config['importCommand'][-1] + ' ' + ' '.join(f"""
                --authenticationDatabase admin
                --db {self.config['database']['name']}
                --collection {collection}
                --host {self.config['database']['address']}:{self.config['database']['port']}
                --username {self.config['username']}
                --password {self.config['password']}
                --drop 
                {file_path}
            """.split())
        ]
        return {'response': self.call_subprocess(mongoimport_command)}

    def initialize_database(self):
        self.drop_database()  # Start from a clean database.
        self.database = self.client[self.config['database']['name']]
        # self.database.set_profiling_level(2)

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

    def execute_update(self, name, documents, primary_key='_id', is_upsert=False):
        # Insert into our buffer dataset.
        buffer_collection = self.database[name + 'Buffer']
        buffer_collection.insert(documents)

        # Perform the insert / upsert (this is done in a client-side shell).
        return self._execute_mongocommand(f""" JSON.stringify(
            db.getSiblingDB("{self.config['database']['name']}")
                .getCollection("{name}Buffer")
                .explain("executionStats")
                .aggregate([
                    {{ $match: {{ }} }},
                    {{ $merge: {{ 
                        into: "{name}", 
                        on: "{primary_key}",
                        whenMatched: "{'replace' if is_upsert else 'fail'}"     
                    }} }}
                ], {{ allowDiskUse: true }})
            )
        """)

    def execute_delete(self, name, predicate):
        return self._execute_mongocommand(f""" JSON.stringify(
            db.getSiblingDB("{self.config['database']['name']}")
                .getCollection("{name}")
                .explain("executionStats")
                .remove({predicate})
            )
        """)

    def execute_select(self, name, pipeline):
        return self._execute_mongocommand(f""" JSON.stringify(
            db.getSiblingDB("{self.config['database']['name']}")
                .getCollection("{name}")
                .explain("executionStats")
                .aggregate({pipeline}, {{ allowDiskUse: true }})
            )
        """)

    def restart_db(self):
        super(AbstractMongoDBRunnable, self).restart_db()
        while True:
            time.sleep(10)
            try:
                self.client.admin.command('ping')
                break
            except pymongo.mongo_client.ConnectionFailure:
                logger.warning(f'Instance is not currently online. Trying again in 10 seconds...')
