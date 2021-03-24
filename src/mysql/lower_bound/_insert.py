import logging

from src.mysql.lower_bound.executor import AbstractLowerBoundRunnable

logger = logging.getLogger(__name__)


class LowerBoundInsert(AbstractLowerBoundRunnable):
    NUMBER_OF_REPEATS = 1000

    def perform_benchmark(self):
        logger.info('Building dataset and loading buffer.')
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
            CREATE TABLE TestTableBuffer (
                id VARCHAR(40) PRIMARY KEY,
                doc JSON
            );
            CREATE TABLE TestTable (
                id VARCHAR(40) PRIMARY KEY,
                doc JSON
            );
            INSERT INTO TestTableBuffer VALUES ('1', '{"a": 1}');
        """))

        logger.info('Now executing the lower bound statement.')
        for i in range(self.NUMBER_OF_REPEATS):
            logger.debug(f'Executing run {i + 1} for the lower bound statement.')
            results = self.execute_sql(cursor, """
                INSERT INTO TestTable
                SELECT * 
                FROM TestTableBuffer;
            """)
            results['runNumber'] = i + 1
            self.log_results(results)
            self.execute_sql(cursor, """ TRUNCATE TABLE TestTable; """)

    def perform_post(self):
        connection = self.connect()

        logger.info('Removing test database.')
        results = self.execute_sql(connection.cursor(), """ DROP DATABASE TestDatabase; """)
        if 'error' in results:
            logger.warning('Could not remove test database.')
            logger.warning(f'JSON Dump: {results}')


if __name__ == '__main__':
    LowerBoundInsert().invoke()
