import logging

from src.asterixdb.lower_bound.executor import AbstractLowerBoundRunnable

logger = logging.getLogger(__name__)


class LowerBoundLoad(AbstractLowerBoundRunnable):
    NUMBER_OF_REPEATS = 1000

    def perform_benchmark(self):
        logger.info('Now executing the lower bound statement.')
        for i in range(self.NUMBER_OF_REPEATS):
            logger.debug(f'Executing run {i + 1} for the lower bound statement.')
            results = self.execute_sqlpp("""
                DROP DATAVERSE TestDataverse IF EXISTS;
                CREATE DATAVERSE TestDataverse;
                USE TestDataverse;

                CREATE TYPE GenericType AS { _id: uuid };
                CREATE DATASET GenericDataset (GenericType) PRIMARY KEY _id AUTOGENERATED;
            """)
            self.log_results(results)

            results = self.execute_sqlpp("""
                USE TestDataverse;
                LOAD DATASET GenericDataset USING localfs (
                    ("path"="%s"), ("format"="json")
                );
            """ % 'localhost:///resources/sample.json')
            results['runNumber'] = i + 1
            self.log_results(results)

    def perform_post(self):
        logger.info('Removing test dataverse.')
        results = self.execute_sqlpp(""" DROP DATAVERSE TestDataverse; """)
        if results['status'] != 'success':
            logger.warning('Could not remove test dataverse.')
            logger.warning(f'JSON Dump: {results}')


if __name__ == '__main__':
    LowerBoundLoad().invoke()
