import logging

from src.asterixdb.tpc_ch.analytical_query.executor import AbstractQueryRunnable

logger = logging.getLogger(__name__)


class BasicAnalyticalQuery(AbstractQueryRunnable):
    def _execute_and_log(self, query_f, query_number, run_number, **parameters):
        logger.info(f'Executing query {query_number}, run number {run_number}.')
        results = self.execute_sqlpp('\nUSE TPC_CH;\n\n' + query_f(**parameters))

        if results['status'] != 'success':
            logger.error(f'Query execution not successful! Parameters: {parameters}')
            return False
        elif len(results['results']) == 0:
            logger.warning(f'No results found... Execution time: {results["metrics"]["elapsedTime"]}')
            return True
        else:
            logger.debug(f'Query was successful. Execution time: {results["metrics"]["elapsedTime"]}')
            results.update({'runNumber': run_number, 'queryNumber': query_number, 'parameters': parameters})
            self.log_results(results)
            return True

    def __init__(self):
        super().__init__(num_queries=50)

    def perform_benchmark(self):
        logger.info('Executing basic analytical query suite.')

        for i in range(self.config['num_queries']):
            # Queries 1, 12, 15, and 20 require a single parameter: a start date.
            queries = {1: self.query_1, 12: self.query_12, 15: self.query_15, 20: self.query_20}
            for number, query_f in queries.items():
                date_1 = self.config['tpc_ch']['parameters']['singleDate'] \
                    [i % len(self.config['tpc_ch']['parameters']['singleDate'])]['date1']

                if not self._execute_and_log(query_f, number, i + 1, date_1=date_1):
                    return

            # Queries 6, 7, 8, and 14 require two parameters: a start and end date.
            queries = {6: self.query_6, 7: self.query_7, 8: self.query_8, 14: self.query_14}
            for number, query_f in queries.items():
                date_pair = self.config['tpc_ch']['parameters']['pairDates'] \
                    [i % len(self.config['tpc_ch']['parameters']['pairDates'])]
                date_1, date_2 = date_pair['date1'], date_pair['date2']

                # Query 8 requires a hint to be specified.
                if number == 8:
                    if not self._execute_and_log(query_f, number, i + 1, date_1=date_1, date_2=date_2, hint=''):
                        return
                else:
                    if not self._execute_and_log(query_f, number, i + 1, date_1=date_1, date_2=date_2):
                        return


if __name__ == '__main__':
    BasicAnalyticalQuery().invoke()
