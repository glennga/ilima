import __init__
import logging
import json
import abc

from src.asterixdb.executor import AbstractBenchmarkRunnable

logger = logging.getLogger(__name__)


class AbstractEqualityPredicateQuery(AbstractBenchmarkRunnable, abc.ABC):
    def __init__(self, **kwargs):
        self.index_name = kwargs['index_name']
        self.dataset_name = kwargs['dataset_name']
        super().__init__()

        logger.info('Loading ATOM sample data into memory.')
        self.sample_atom_data = []
        with open(self.config['resources'] + '/' + 'sample_data' + '/' + f'ATOM-{self.dataset_name}Sample.json') as f:
            for line in f:
                self.sample_atom_data.append(json.loads(line))

        logger.info('Loading SARR sample data into memory.')
        self.sample_sarr_data = []
        with open(self.config['resources'] + '/' + 'sample_data' + '/' + f'SARR-{self.dataset_name}Sample.json') as f:
            for line in f:
                self.sample_sarr_data.append(json.loads(line))

    def _do_indexes_exist(self):
        logger.info(f'Checking that the index "{self.index_name}" exists on ATOM.')
        results = self._execute_sqlpp(f"""
            SELECT *
            FROM `Metadata`.`Index`
            WHERE IndexName = "{self.index_name}" AND 
                  DataverseName = "ShopALot.ATOM" AND 
                  DatasetName = "{self.dataset_name}";
        """)
        if len(results['results']) == 0:
            logger.error(f'Index "{self.index_name}" on ATOM does not exist.')
            return False

        logger.info(f'Checking that the index "{self.index_name}" exists on SARR.')
        results = self._execute_sqlpp(f"""
            SELECT *
            FROM `Metadata`.`Index`
            WHERE IndexName = "{self.index_name}" AND 
                  DataverseName = "ShopALot.SARR" AND 
                  DatasetName = "{self.dataset_name}";
        """)
        if len(results['results']) == 0:
            logger.error(f'Index "{self.index_name}" on SARR does not exist.')
            return False

        return True

    def _enable_index_only(self, is_index_only):
        if is_index_only:
            results = self._execute_sqlpp(""" SET `compiler.indexonly` "true"; """)
        else:
            results = self._execute_sqlpp(""" SET `compiler.indexonly` "false"; """)

        if results['status'] != 'success':
            logger.error("Could not set indexonly parameter.")
            return False
        else:
            return True

    @abc.abstractmethod
    def _benchmark_atom(self, working_sample_objects, atom_num):
        pass

    @abc.abstractmethod
    def _benchmark_sarr(self, working_sample_objects, sarr_num):
        pass

    def _perform_benchmark(self):
        if not self._do_indexes_exist():
            return

        logger.info(f'Executing equality_predicate_query on {self.dataset_name} for ATOM.')
        logger.info(f'Running benchmark for: ATOM, {self.dataset_name}, {self.index_name}, not index-only.')
        working_sample_objects = self.sample_atom_data[:int(len(self.sample_atom_data) / 2)]
        if not self._benchmark_atom(working_sample_objects, atom_num=1):
            return

        logger.info(f'Running benchmark for: ATOM, {self.dataset_name}, {self.index_name}, index-only.')
        working_sample_objects = self.sample_atom_data[-int(len(self.sample_atom_data) / 2):]
        if not self._benchmark_atom(working_sample_objects, atom_num=2):
            return

        logger.info(f'Executing equality_predicate_query on {self.dataset_name} for SARR.')
        logger.info(f'Running benchmark for SARR, {self.dataset_name}, {self.index_name}, '
                    f'unnest-style query, no materialization.')
        working_sample_objects = self.sample_sarr_data[:int(len(self.sample_sarr_data) / 4)]
        if not self._benchmark_sarr(working_sample_objects, sarr_num=1):
            return

        logger.info(f'Running benchmark for SARR, {self.dataset_name}, {self.index_name}, '
                    f'unnest-style query, with materialization.')
        working_sample_objects = self.sample_sarr_data[int(len(self.sample_sarr_data) / 4):
                                                       int(len(self.sample_sarr_data) / 2)]
        if not self._benchmark_sarr(working_sample_objects, sarr_num=2):
            return

        logger.info(f'Running benchmark for SARR, {self.dataset_name}, {self.index_name}, '
                    f'existential quantification query.')
        working_sample_objects = self.sample_sarr_data[int(len(self.sample_sarr_data) / 2):
                                                       int(3 * len(self.sample_sarr_data) / 4)]
        if not self._benchmark_sarr(working_sample_objects, sarr_num=3):
            return

        logger.info(f'Running benchmark for SARR, {self.dataset_name}, {self.index_name}, '
                    f'universal quantification query.')
        working_sample_objects = self.sample_sarr_data[-int(len(self.sample_sarr_data) / 4):]
        if not self._benchmark_sarr(working_sample_objects, sarr_num=4):
            return

    def _perform_post(self):
        pass
