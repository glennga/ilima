import __init__
import logging
import json
import abc
import random

from src.asterixdb.executor import AbstractBenchmarkRunnable
from src.datagen.shopalot import DatagenAbstractFactoryProvider
from src.datagen.shopalot import PrimaryKeyGeneratorFactory

logger = logging.getLogger(__name__)


class AbstractInsertUpsertDelete(AbstractBenchmarkRunnable, abc.ABC):
    DATASET_UPSERT_ALPHAS = [0.0, 0.25, 0.5, 0.75, 1]
    DATASET_INCREMENT_SIZE = 0.005
    DATASET_DECREMENT_SIZE = 0.005

    def __init__(self, **kwargs):
        self.index_names = kwargs['index_names']
        self.dataset_name = kwargs['dataset_name']
        self.primary_key = kwargs['primary_key']
        self.dataset_size = kwargs['dataset_size']
        self.chunk_size = kwargs['chunk_size']

        self.insert_epoch = kwargs['insert_epoch']
        self.upsert_epoch = kwargs['upsert_epoch']
        self.delete_epoch = kwargs['delete_epoch']

        # We need to remove primary key before invoking our factory.
        factory_args = kwargs
        del factory_args['primary_key']

        # Invoke our factory.
        datagen_factory = DatagenAbstractFactoryProvider.provide_memory_abstract_factory(kwargs['datagen_class'])
        self.datagen = datagen_factory(primary_key_generator=None, pk_zfill=len(str(self.dataset_size)), **factory_args)
        super().__init__()

    def _perform_insert_upsert(self, i, operation, text):
        # First, insert into our buffer dataset.
        buffer_results = self.execute_sqlpp(f"""
            INSERT INTO ShopALot.{self.dataverse.upper()}.{self.dataset_name}Buffer [{text}];
        """)
        if buffer_results['status'] != 'success':
            logger.error(f'Insert {i + 1} into buffer was not successful.')
            return False

        # Next, perform our UPSERT/INSERT operation.
        query_suffix = ';'  # 'RETURNING MISSING;'
        results = self.execute_sqlpp(f"""
            {operation.upper()} INTO ShopALot.{self.dataverse.upper()}.{self.dataset_name}
            SELECT VALUE B
            FROM ShopALot.{self.dataverse.upper()}.{self.dataset_name}Buffer B
            {query_suffix}
        """)
        if results['status'] != 'success':
            logger.error(f'{operation.capitalize()} {i + 1} was not successful.')
            return False

        # Finally, clear our buffer.
        buffer_results = self.execute_sqlpp(f"""
            DELETE FROM ShopALot.{self.dataverse.upper()}.{self.dataset_name}Buffer;
        """)
        if buffer_results['status'] != 'success':
            logger.error(f'Delete {i + 1} from buffer was not successful.')
            return False

        logger.debug(f'{operation.capitalize()} {i + 1} was successful. '
                     f'Execution time: {results["metrics"]["elapsedTime"]}')

        results['runNumber'] = i + 1
        if 'results' in results:
            del results['results']
        self.log_results(results)
        return True

    def _benchmark_insert(self):
        working_range = {'start': self.dataset_size, 'end': self.dataset_size + self.chunk_size}
        for i in range(self.insert_epoch):
            # Invoke our data generator.
            primary_key_generator = PrimaryKeyGeneratorFactory. \
                provide_range_generator(working_range['start'], working_range['end'])
            self.datagen.reset_generation(primary_key_generator)
            self.datagen.invoke()

            working_range['start'] = working_range['start'] + self.chunk_size
            working_range['end'] = working_range['end'] + self.chunk_size

            # Pull our insert chunk.
            if self.dataverse == self.ATOM_DATAVERSE:
                insert_chunk = self.datagen.atom_json
            else:
                insert_chunk = self.datagen.sarr_json

            # Perform the insert itself.
            insert_text = ',\n'.join([json.dumps(s) for s in insert_chunk])
            if not self._perform_insert_upsert(i, 'insert', insert_text):
                return False

        return True

    def _benchmark_upsert(self):
        for alpha in self.DATASET_UPSERT_ALPHAS:
            logger.info(f'Now using alpha value of {alpha}.')
            for i in range(self.upsert_epoch):
                # Invoke our data generator.
                primary_key_generator = PrimaryKeyGeneratorFactory. \
                    provide_rand_range_generator(0, self.dataset_size, self.chunk_size)
                self.datagen.reset_generation(primary_key_generator)
                self.datagen.invoke()

                # Pull our upsert chunk. Apply transformation as needed.
                if self.dataverse == self.ATOM_DATAVERSE:
                    upsert_chunk = self.datagen.atom_json
                    chunk_updater = self.datagen.atom_mapper
                else:
                    upsert_chunk = self.datagen.sarr_json
                    chunk_updater = lambda a: self.datagen.sarr_mapper(self.datagen.atom_mapper(a))

                for record in upsert_chunk[0:round(self.chunk_size * alpha)]:
                    old_pk = int(record[self.primary_key])
                    new_record = chunk_updater(old_pk + 1)
                    new_record[self.primary_key] = old_pk

                # Perform the upsert itself.
                upsert_text = ',\n'.join([json.dumps(s) for s in upsert_chunk])
                if not self._perform_insert_upsert(i, 'upsert', upsert_text):
                    return False

        return True

    def _benchmark_delete(self):
        delete_chunk_ids = set()
        for i in range(self.delete_epoch):
            chunk_id = random.randint(0, self.chunk_size)
            while chunk_id in delete_chunk_ids:
                chunk_id = random.randint(0, self.chunk_size)

            logger.debug(f'Deleting using chunk_id: {self.datagen.format_key(chunk_id)}')
            delete_chunk_ids.add(chunk_id)

            results = self.execute_sqlpp(f"""
                DELETE FROM ShopALot.{self.dataverse.upper()}.{self.dataset_name}
                WHERE chunk_id = "{self.datagen.format_key(chunk_id)}";
            """)
            if results['status'] != 'success':
                logger.error(f'Delete {i + 1} was not successful.')
                return False

            logger.debug(f'Delete {i + 1} was successful. Execution time: {results["metrics"]["elapsedTime"]}')
            results['runNumber'] = i + 1
            self.log_results(results)

        return True

    def _index_chunk_id(self):
        results = self.execute_sqlpp(f"""
            USE ShopALot.{self.dataverse.upper()};
            CREATE INDEX {self.dataset_name.capitalize()}ChunkIdx ON 
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

        logger.info(f'Executing insert_upsert_delete on {self.dataset_name} for {self.dataverse.upper()}.')
        logger.info(f'Running benchmark for {self.dataverse.upper()} inserts.')
        if not self._benchmark_insert():
            return

        self.restart_cluster()
        logger.info(f'Running benchmark for {self.dataverse.upper()} upserts.')
        if not self._benchmark_upsert():
            return

        self.restart_cluster()
        logger.info(f'Indexing chunk_id {self.dataverse.upper()}.')
        if not self._index_chunk_id():
            return

        logger.info(f'Running benchmark for {self.dataverse.upper()} deletes.')
        if not self._benchmark_delete():
            return

    def perform_post(self):
        logger.info(f'Removing the index on chunk_id for {self.dataverse.upper()}.')
        results = self.execute_sqlpp(f"""
            USE ShopALot.{self.dataverse.upper()};
            DROP INDEX {self.dataset_name.capitalize()}.{self.dataset_name.capitalize()}ChunkIdx IF EXISTS;
        """)
        if results['status'] != 'success':
            logger.warning('Could not drop the index on chunk_id.')
            logger.warning(f'JSON Dump: {results}')
