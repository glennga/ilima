import logging

from src.mongodb.tpc_ch.analytical_query._basic import BasicAnalyticalQuery

logger = logging.getLogger(__name__)


class IndexedAnalyticalQuery(BasicAnalyticalQuery):
    def perform_benchmark(self):
        logger.info('Executing indexed analytical query suite.')

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
    IndexedAnalyticalQuery().invoke()
