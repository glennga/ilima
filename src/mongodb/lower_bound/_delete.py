import logging

from src.mongodb.lower_bound.executor import AbstractLowerBoundRunnable

logger = logging.getLogger(__name__)


class LowerBoundDelete(AbstractLowerBoundRunnable):
    NUMBER_OF_REPEATS = 1000

    def perform_benchmark(self):
        logger.info('Removing database.')
        self.drop_database()

        logger.info('Initializing test database and collection.')
        self.initialize_database()
        self.create_collection('Test')
        self.execute_insert('Test', [{"a": 1}])

        logger.info('Now executing the lower bound statement.')
        for i in range(self.NUMBER_OF_REPEATS):
            logger.debug(f'Executing run {i + 1} for the lower bound statement.')
            results = self.execute_delete('Test', {'a': 0})
            results['runNumber'] = i + 1
            self.log_results(results)

    def perform_post(self):
        logger.info('Removing database.')
        self.drop_database()


if __name__ == '__main__':
    LowerBoundDelete().invoke()
