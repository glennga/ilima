import logging

from src.mongodb.lower_bound.executor import AbstractLowerBoundRunnable

logger = logging.getLogger(__name__)


class LowerBoundUpsert(AbstractLowerBoundRunnable):
    NUMBER_OF_REPEATS = 1000

    def perform_benchmark(self):
        logger.info('Removing database.')
        self.drop_database()

        logger.info('Initializing test database and collection (+ buffer).')
        self.create_collection('TestBuffer')
        self.create_collection('Test')

        logger.info('Now executing the lower bound statement.')
        for i in range(self.NUMBER_OF_REPEATS):
            logger.debug(f'Executing run {i + 1} for the lower bound statement.')
            results = self.execute_update('Test', [{"a": 1}], is_upsert=True)
            results['runNumber'] = i + 1
            self.log_results(results)

    def perform_post(self):
        logger.info('Removing database.')
        self.drop_database()


if __name__ == '__main__':
    LowerBoundUpsert().invoke()
