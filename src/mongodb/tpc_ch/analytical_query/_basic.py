import logging

from src.mongodb.tpc_ch.analytical_query.executor import AbstractQueryRunnable

logger = logging.getLogger(__name__)


class BasicAnalyticalQuery(AbstractQueryRunnable):
    def _execute_and_log(self, query_f, query_number, run_number, timeout, **parameters):
        logger.info(f'Executing query {query_number}, run number {run_number}.')
        results = self.execute_select(timeout=timeout, **query_f(**parameters))

        if results['status'] != 'success':
            logger.error(f'Query execution not successful! Parameters: {parameters}')
            return False
        elif len(results['queryResults']) == 0:
            logger.warning(f'No results found... Execution time: {results["clientTime"]}')
            return True
        else:
            logger.debug(f'Query was successful. Execution time: {results["clientTime"]}')
            results.update({'runNumber': run_number, 'queryNumber': query_number, 'parameters': parameters})
            self.log_results(results)
            return True

    def __init__(self):
        super().__init__(num_queries=40)

    def perform_benchmark(self):
        logger.info('Executing basic analytical query suite.')

        for i in range(self.config['num_queries']):
            date_pair = self.config['tpc_ch']['parameters']['dateRange'] \
                [i % len(self.config['tpc_ch']['parameters']['dateRange'])]
            date_1, date_2 = date_pair['date1'], date_pair['date2']

            timeout = 3600 * 1000  # Execute all queries from query set 1.
            self._execute_and_log(self.query_1, 1, i + 1, timeout=timeout, date_1=date_1, date_2=date_2)
            self._execute_and_log(self.query_6, 6, i + 1, timeout=timeout, date_1=date_1, date_2=date_2)
            self._execute_and_log(self.query_12, 12, i + 1, timeout=timeout, date_1=date_1, date_2=date_2)
            self._execute_and_log(self.query_14, 14, i + 1, timeout=timeout, date_1=date_1, date_2=date_2)


if __name__ == '__main__':
    BasicAnalyticalQuery().invoke()