import __init__
import logging

from src.asterixdb.executor import AbstractBenchmarkRunnable

logger = logging.getLogger(__name__)


class LowerBoundInsert(AbstractBenchmarkRunnable):
    NUMBER_OF_REPEATS = 1000

    def perform_benchmark(self):
        logger.info('Building dataverse and loading dataverse.')
        results = self.execute_sqlpp("""
            DROP DATAVERSE TestDataverse IF EXISTS;
            CREATE DATAVERSE TestDataverse;
            USE TestDataverse;

            CREATE TYPE GenericType AS { _id: uuid };
            CREATE DATASET GenericDataset (GenericType) PRIMARY KEY _id AUTOGENERATED;
            CREATE DATASET GenericDatasetBuffer (GenericType) PRIMARY KEY _id AUTOGENERATED;
            
            INSERT INTO GenericDatasetBuffer [{"a": 1}];
        """)
        self.log_results(results)

        logger.info('Now executing the lower bound statement.')
        for i in range(self.NUMBER_OF_REPEATS):
            logger.debug(f'Executing run {i + 1} for the lower bound statement.')
            results = self.execute_sqlpp("""
                USE TestDataverse;
                INSERT INTO GenericDataset 
                SELECT G.a
                FROM GenericDatasetBuffer G
                RETURNING MISSING;
            """)
            results['runNumber'] = i + 1
            self.log_results(results)

    def perform_post(self):
        logger.info('Removing test dataverse.')
        results = self.execute_sqlpp(""" DROP DATAVERSE TestDataverse; """)
        if results['status'] != 'success':
            logger.warning('Could not remove test dataverse.')
            logger.warning(f'JSON Dump: {results}')


if __name__ == '__main__':
    LowerBoundInsert().invoke()
