import __init__
import logging
import json
import random
import string
import faker
import abc
import re

from src.asterixdb.executor import AbstractBenchmarkRunnable

logger = logging.getLogger(__name__)


class AbstractInsertUpsertDelete(AbstractBenchmarkRunnable, abc.ABC):
    TOTAL_DATASET_SIZE = 160000000
    TARGET_INSERT_SIZE = TOTAL_DATASET_SIZE / 100
    INSERT_CHUNK_SIZE = 1000

    def __init__(self, **kwargs):
        self.index_names = kwargs['index_names']
        self.dataset_name = kwargs['dataset_name']
        self.primary_key = kwargs['primary_key']
        self.faker_dategen = faker.Faker()
        self.chunks = dict()
        super().__init__()

    @abc.abstractmethod
    def generate_atom_dataset(self, insert_handler, starting_id, ending_id):
        pass

    @abc.abstractmethod
    def generate_sarr_dataset(self, insert_handler, starting_id, ending_id):
        pass

    def _benchmark_insert(self, dataverse):
        for i in range(int(self.TARGET_INSERT_SIZE / self.INSERT_CHUNK_SIZE)):
            proposed_k = ''.join(random.choice(string.ascii_uppercase) for _ in range(20))
            while proposed_k in self.chunks.keys():  # This MUST be unique.
                proposed_k = ''.join(random.choice(string.ascii_uppercase) for _ in range(20))
            self.chunks[proposed_k] = set()
            logger.debug(f'Using chunk identifier {proposed_k}.')
            insert_chunk = []

            def _insert_handler(out_json):
                out_json['chunk_id'] = proposed_k
                self.chunks[proposed_k].add(out_json[self.primary_key])
                insert_chunk.append(json.dumps(out_json))

            if dataverse == 'atom':
                self.generate_atom_dataset(_insert_handler, 0, self.INSERT_CHUNK_SIZE)
            else:
                self.generate_sarr_dataset(_insert_handler, 0, self.INSERT_CHUNK_SIZE)
            insert_text = ',\n'.join(insert_chunk)

            results = self.execute_sqlpp(f"""
                INSERT INTO ShopALot.{dataverse.upper()}.{self.dataset_name} [{insert_text}];               
            """)
            if results['status'] != 'success':
                logger.error(f'Insert {i + 1} was not successful.')
                return False

            logger.debug(f'Insert {i + 1} was successful. Execution time: {results["metrics"]["elapsedTime"]}')
            results['runNumber'] = i + 1
            self.log_results(results)

        return True

    def _benchmark_upsert(self, dataverse):
        for i, (chunk_key, chunk) in enumerate(self.chunks.items()):
            logger.debug(f'Working with chunk identifier {chunk_key}.')
            chunk_iterator = iter(chunk)
            upsert_chunk = []

            def _upsert_handler(out_json):
                out_json['chunk_id'] = chunk_key
                out_json[self.primary_key] = next(chunk_iterator)
                upsert_chunk.append(json.dumps(out_json))

            if dataverse == 'atom':
                self.generate_atom_dataset(_upsert_handler, 0, self.INSERT_CHUNK_SIZE)
            else:
                self.generate_sarr_dataset(_upsert_handler, 0, self.INSERT_CHUNK_SIZE)
            upsert_text = ',\n'.join(upsert_chunk)

            results = self.execute_sqlpp(f"""
                UPSERT INTO ShopALot.{dataverse.upper()}.{self.dataset_name} [{upsert_text}];               
            """)
            if results['status'] != 'success':
                logger.error(f'Upsert {i + 1} was not successful.')
                return False

            logger.debug(f'Upsert {i + 1} was successful. Execution time: {results["metrics"]["elapsedTime"]}')
            results['runNumber'] = i + 1
            self.log_results(results)

        return True

    def _benchmark_delete(self, dataverse):
        # To avoid scanning the entire dataset for each deletion, we create an index on the chunk key.
        logger.info(f'Now indexing chunk_id for ShopALot.{dataverse.upper()}.{self.dataset_name}.')
        results = self.execute_sqlpp(f"""
            USE ShopALot.{dataverse.upper()};
            CREATE INDEX {self.dataset_name.lower()}ChunkIdx ON {self.dataset_name.capitalize()} (chunk_id : string ?);
        """)
        if results['status'] != 'success':
            logger.error(f'Unable to create index on chunk_id for ShopALot.{dataverse.upper()}.{self.dataset_name}.')
            return False

        for i, chunk_key in enumerate(self.chunks.keys()):
            results = self.execute_sqlpp(f"""
                DELETE FROM ShopALot.{dataverse.upper()}.{self.dataset_name}
                WHERE chunk_id = "{chunk_key}";
            """)
            if results['status'] != 'success':
                logger.error(f'Delete {i + 1} was not successful.')
                return False

            logger.debug(f'Delete {i + 1} was successful. Execution time: {results["metrics"]["elapsedTime"]}')
            results['runNumber'] = i + 1
            self.log_results(results)

        return True

    def perform_benchmark(self):
        if not self.do_indexes_exist(self.index_names, self.dataset_name):
            return

        logger.info(f'Executing insert_upsert_delete on {self.dataset_name} for ATOM.')
        logger.info('Running benchmark for ATOM inserts.')
        if not self._benchmark_insert(dataverse='atom'):
            return

        logger.info('Running benchmark for ATOM upserts.')
        if not self._benchmark_upsert(dataverse='atom'):
            return

        logger.info('Running benchmark for ATOM deletes.')
        if not self._benchmark_delete(dataverse='atom'):
            return

        logger.info(f'Executing insert_upsert_delete on {self.dataset_name} for SARR.')
        logger.info('Running benchmark for SARR inserts.')
        if not self._benchmark_insert(dataverse='sarr'):
            return

        logger.info('Running benchmark for SARR upserts.')
        if not self._benchmark_upsert(dataverse='sarr'):
            return

        logger.info('Running benchmark for SARR deletes.')
        if not self._benchmark_delete(dataverse='sarr'):
            return

    def perform_post(self):
        pass
