import __init__
import logging
import abc

from src.asterixdb.executor import AbstractBenchmarkRunnable
from src.datagen.shopalot import DatagenAbstractFactoryProvider
from src.datagen.shopalot import PrimaryKeyGeneratorFactory

logger = logging.getLogger(__name__)


class AbstractEqualityPredicateQuery(AbstractBenchmarkRunnable, abc.ABC):
    def __init__(self, **kwargs):
        self.index_names = kwargs['index_names']
        self.dataset_name = kwargs['dataset_name']
        self.dataset_size = kwargs['dataset_size']
        self.num_queries = kwargs['num_queries']
        self.chunk_size = kwargs['chunk_size']
        super().__init__()

        datagen_factory = DatagenAbstractFactoryProvider.provide_memory_abstract_factory(kwargs['datagen_class'])
        self.datagen = datagen_factory(primary_key_generator=None, pk_zfill=len(str(self.dataset_size)), **kwargs)

    def enable_index_only(self, is_index_only):
        if is_index_only:
            results = self.execute_sqlpp(""" SET `compiler.indexonly` "true"; """)
        else:
            results = self.execute_sqlpp(""" SET `compiler.indexonly` "false"; """)

        if results['status'] != 'success':
            logger.error("Could not set indexonly parameter.")
            return False
        else:
            return True

    @abc.abstractmethod
    def benchmark_atom(self, working_sample_objects, atom_num):
        pass

    @abc.abstractmethod
    def benchmark_sarr(self, working_sample_objects, sarr_num):
        pass

    def get_sample_data(self, dataverse):
        primary_key_generator = PrimaryKeyGeneratorFactory.\
            provide_rand_range_generator(0, self.dataset_size, self.num_queries)
        self.datagen.reset_generation(primary_key_generator)
        self.datagen.invoke()

        if dataverse == self.ATOM_DATAVERSE:
            return self.datagen.atom_json
        else:
            return self.datagen.sarr_json

    def perform_benchmark(self):
        if not self.do_indexes_exist(self.index_names, self.dataset_name):
            logger.warning(f'Indexes not found. Assuming that this is benchmarking non-indexed operations.')

        if self.dataverse == self.ATOM_DATAVERSE:
            logger.info(f'Executing equality_predicate_query on {self.dataset_name} for ATOM.')
            logger.info(f'Running benchmark for: ATOM, {self.dataset_name}, {str(self.index_names)}, index-only.')
            if not self.benchmark_atom(self.get_sample_data(self.dataverse), atom_num=1):
                return

            logger.info(f'Running benchmark for: ATOM, {self.dataset_name}, {str(self.index_names)}, not index-only.')
            if not self.benchmark_atom(self.get_sample_data(self.dataverse), atom_num=2):
                return

        else:
            logger.info(f'Executing equality_predicate_query on {self.dataset_name} for SARR.')
            logger.info(f'Running benchmark for SARR, {self.dataset_name}, {str(self.index_names)}, '
                        f'unnest-style query, no materialization.')
            if not self.benchmark_sarr(self.get_sample_data(self.dataverse), sarr_num=1):
                return

            logger.info(f'Running benchmark for SARR, {self.dataset_name}, {str(self.index_names)}, '
                        f'unnest-style query, with materialization.')
            if not self.benchmark_sarr(self.get_sample_data(self.dataverse), sarr_num=2):
                return

            logger.info(f'Running benchmark for SARR, {self.dataset_name}, {str(self.index_names)}, '
                        f'existential quantification query.')
            if not self.benchmark_sarr(self.get_sample_data(self.dataverse), sarr_num=3):
                return

            logger.info(f'Running benchmark for SARR, {self.dataset_name}, {str(self.index_names)}, '
                        f'universal quantification query.')
            if not self.benchmark_sarr(self.get_sample_data(self.dataverse), sarr_num=4):
                return
