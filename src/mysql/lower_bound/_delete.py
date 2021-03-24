import logging

from src.mysql.lower_bound.executor import AbstractLowerBoundRunnable

logger = logging.getLogger(__name__)


class LowerBoundDelete(AbstractLowerBoundRunnable):
    NUMBER_OF_REPEATS = 1000

    def perform_benchmark(self):
        logger.info('Building and loading dataset.')
        connection = self.connect()
        self.enable_profiling(connection.cursor())
        self.execute_sql(connection.cursor(), """
            DROP DATABASE IF EXISTS TestDatabase;
            CREATE DATABASE TestDatabase;
        """)
        connection.close()

        connection = self.connect('TestDatabase')
        self.enable_profiling(connection.cursor())
        cursor = connection.cursor()
        self.log_results(self.execute_sql(cursor, """
            CREATE TABLE TestTable (
                id VARCHAR(40) PRIMARY KEY,
                doc JSON
            );
            INSERT INTO TestTable VALUES ('1', '{"a": 1}');
        """))

        logger.info('Now executing the lower bound statement.')
        for i in range(self.NUMBER_OF_REPEATS):
            logger.debug(f'Executing run {i + 1} for the lower bound statement.')
            results = self.execute_sql(cursor, """
                DELETE FROM TestTable T
                WHERE JSON_CONTAINS(T.doc, '0', '$.a');
            """)
            results['runNumber'] = i + 1
            self.log_results(results)

    def perform_post(self):
        connection = self.connect()

        logger.info('Removing test database.')
        results = self.execute_sql(connection.cursor(), """ DROP DATABASE TestDatabase; """)
        if 'error' in results:
            logger.warning('Could not remove test database.')
            logger.warning(f'JSON Dump: {results}')


if __name__ == '__main__':
    LowerBoundDelete().invoke()
