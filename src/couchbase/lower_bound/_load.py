import logging

from src.couchbase.lower_bound.executor import AbstractLowerBoundRunnable

logger = logging.getLogger(__name__)


class LowerBoundLoad(AbstractLowerBoundRunnable):
    NUMBER_OF_REPEATS = 1000

    def perform_benchmark(self):
        logger.info('Building bucket.')
        self.initialize_bucket()

        logger.info('Now executing the lower bound statement.')
        for i in range(self.NUMBER_OF_REPEATS):
            logger.debug(f'Executing run {i + 1} for the lower bound statement.')
            results = self.execute_cbimport('file:///resources/sample.json')
            results['runNumber'] = i + 1
            self.log_results(results)

    def perform_post(self):
        logger.info('Removing bucket.')
        self.remove_bucket()


if __name__ == '__main__':
    LowerBoundLoad().invoke()
