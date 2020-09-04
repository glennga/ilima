import __init__
import logging

from src.asterixdb.executor import AbstractBenchmarkRunnable


logger = logging.getLogger(__name__)


class LowerBoundQueryBenchmarkRunnable(AbstractBenchmarkRunnable):
    def _perform_benchmark(self):
        logger.info('Issuing DDL for lower_bound_query.')
        results = self._execute_sqlpp("""
            DROP DATAVERSE ForLowerBoundQuery IF EXISTS;
            CREATE DATAVERSE ForLowerBoundQuery;
            USE ForLowerBoundQuery;

            CREATE TYPE GenericType AS {
                _id: uuid
            };

            CREATE DATASET GenericDataset (GenericType) PRIMARY KEY _id AUTOGENERATED;

            INSERT INTO GenericDataset [{"a": 1}];
        """)
        self._log_results(results)

        logger.info('Now executing the lower_bound_query.')
        for i in range(1000):
            logger.debug(f'Executing run {i + 1} for lower_bound_query.')
            results = self._execute_sqlpp("""
                SELECT *
                FROM ForLowerBoundQuery.GenericDataset;
            """)
            results['runNumber'] = i + 1
            self._log_results(results)

    def _perform_post(self):
        logger.info('Removing testing dataverse.')
        results = self._execute_sqlpp(""" DROP DATAVERSE ForLowerBoundQuery; """)
        if results['status'] != 'success':
            logger.warning('Could not remove test dataverse.')


if __name__ == '__main__':
    LowerBoundQueryBenchmarkRunnable()
