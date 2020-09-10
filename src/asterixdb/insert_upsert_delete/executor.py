import __init__
import logging
import json
import abc
import random
import re

from src.asterixdb.executor import AbstractBenchmarkRunnable

logger = logging.getLogger(__name__)


class AbstractInsertUpsertDelete(AbstractBenchmarkRunnable, abc.ABC):
    DATASET_INCREMENT_SIZE = 0.01
    DATASET_DECREMENT_SIZE = 0.05

    def __init__(self, **kwargs):
        self.index_names = kwargs['index_names']
        self.dataset_name = kwargs['dataset_name']
        self.primary_key = kwargs['primary_key']
        self.dataset_size = kwargs['dataset_size']
        self.chunk_size = kwargs['chunk_size']

        self.insert_epoch = kwargs['insert_epoch']
        self.upsert_epoch = kwargs['upsert_epoch']
        self.delete_epoch = kwargs['delete_epoch']

        class ForMemoryDatagen(kwargs['dataget_class']):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self.atom_json, self.sarr_json = [], []

            def json_consumer(self, atom_json, sarr_json):
                self.atom_json.append(atom_json)
                self.sarr_json.append(sarr_json)

            def reset_generation(self, start_id, end_id):
                self.start_id = start_id
                self.end_id = end_id
                self.atom_json = []
                self.sarr_json = []

        self.datagen = ForMemoryDatagen(start_id=0, end_id=self.dataset_size)
        super().__init__()

    def _generate_json(self, dataverse, start_id, end_id):
        if dataverse == 'atom':
            self.datagen.reset_generation(start_id, end_id)
            self.datagen()
            return self.datagen.atom_json
        else:
            self.datagen.reset_generation(start_id, end_id)
            self.datagen()
            return self.datagen.sarr_json

    def _perform_insert_upsert(self, i, dataverse, operation, text):
        results = self.execute_sqlpp(f"""
            {operation.upper()} INTO ShopALot.{dataverse.upper()}.{self.dataset_name} [{text}];               
        """)
        if results['status'] != 'success':
            logger.error(f'{operation.capitalize()} {i + 1} was not successful.')
            return False

        logger.debug(f'{operation.capitalize()} {i + 1} was successful. '
                     f'Execution time: {results["metrics"]["elapsedTime"]}')

        results['plans'] = {  # This removes the expression trees, and reduce the size of our results.
            'logicalPlan': re.sub(
                'ordered-list-constructor\({.*?}\)', 'ordered-list-constructor({...})',
                results['plans']['logicalPlan']
            ),
            'optimizedLogicalPlan': re.sub(
                'ordered-list-constructor\({.*}\)', 'ordered-list-constructor({...})',
                results['plans']['optimizedLogicalPlan']
            )
        }
        results['runNumber'] = i + 1
        self.log_results(results)
        return True

    def _benchmark_insert(self, dataverse):
        working_range = {'start': self.dataset_size, 'end': self.dataset_size + self.chunk_size}
        for i in range(self.insert_epoch):
            insert_chunk = self._generate_json(dataverse, working_range['start'], working_range['end'])

            # Perform the insert itself.
            insert_text = ',\n'.join([json.loads(s) for s in insert_chunk])
            if not self._perform_insert_upsert(i, dataverse, 'insert', insert_text):
                return False

        return True

    def _benchmark_upsert(self, dataverse):
        for i in range(self.upsert_epoch):
            upsert_chunk = self._generate_json(dataverse, 0, self.chunk_size)

            # Generate a random set of primary keys and chunk IDs to use for our UPSERT.
            primary_key_map = dict()
            for _ in range(self.chunk_size):
                # We are only trying to update "original" values, to minimize cache hits.
                new_primary_key = random.randint(0, self.dataset_size)
                new_chunk_id = new_primary_key % self.chunk_size
                while new_primary_key in primary_key_map:
                    new_primary_key = random.randint(0, self.dataset_size)
                    new_chunk_id = new_primary_key % self.chunk_size

                # We are adding a new PK to our hash map.
                primary_key_map[new_primary_key] = new_chunk_id

            # Update our UPSERT chunk to use these different PKs + chunk IDs.
            for record, (new_pk, new_chunk_id) in zip(upsert_chunk, primary_key_map.items()):
                record[self.primary_key] = new_pk
                record['chunk_id'] = new_chunk_id

            # Perform the upsert itself.
            if not self._perform_insert_upsert(i, dataverse, 'upsert', ',\n'.join(upsert_chunk)):
                return False

        return True

    def _benchmark_delete(self, dataverse):
        delete_chunk_ids = set()
        for i in range(self.delete_epoch):
            chunk_id = random.randint(0, self.chunk_size)
            while chunk_id in delete_chunk_ids:
                chunk_id = random.randint(0, self.chunk_size)

            logger.debug(f'Deleting using chunk_id: {chunk_id}')
            delete_chunk_ids.add(chunk_id)

            results = self.execute_sqlpp(f"""
                DELETE FROM ShopALot.{dataverse.upper()}.{self.dataset_name}
                WHERE chunk_id = "{chunk_id}";
            """)
            if results['status'] != 'success':
                logger.error(f'Delete {i + 1} was not successful.')
                return False

            logger.debug(f'Delete {i + 1} was successful. Execution time: {results["metrics"]["elapsedTime"]}')
            results['runNumber'] = i + 1
            self.log_results(results)

        return True

    def _index_chunk_id(self):
        logger.info('Indexing chunk_id for ATOM and SARR.')
        results = self.execute_sqlpp(f"""
            USE ShopALot.ATOM;
            CREATE INDEX atom{self.dataset_name.capitalize()}ChunkIdx ON 
                {self.dataset_name.capitalize()} (chunk_id : string ?);
            USE ShopALot.SARR;
            CREATE INDEX atom{self.dataset_name.capitalize()}ChunkIdx ON 
                {self.dataset_name.capitalize()} (chunk_id : string ?);
        """)
        if results['status'] != 'success':
            logger.error('Could not index chunk_id.')
            logger.error(f'JSON Dump: {results}')
            return False

        return True

    def perform_benchmark(self):
        if not self.do_indexes_exist(self.index_names, self.dataset_name):
            return

        if not self._index_chunk_id():
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
        logger.info('Removing the index on chunk_id for both ATOM and SARR.')
        results = self.execute_sqlpp(f"""
            USE ShopALot.ATOM;
            DROP INDEX {self.dataset_name.capitalize()}.{self.dataset_name.lower()}ChunkIdx;
            USE ShopALot.SARR;
            DROP INDEX {self.dataset_name.capitalize()}.{self.dataset_name.lower()}ChunkIdx;
        """)
        if results['status'] != 'success':
            logger.warning('Could not drop the index on chunk_id.')
            logger.warning(f'JSON Dump: {results}')
