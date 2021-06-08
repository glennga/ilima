import logging

from src.couchbase.tpc_ch.analytical_query._basic import BasicAnalyticalQuery

logger = logging.getLogger(__name__)


class IndexedAnalyticalQuery(BasicAnalyticalQuery):
    def perform_benchmark(self):
        logger.info('Executing indexed analytical query suite.')
        self.connect_bucket()

        for i in range(self.config['num_queries']):
            date_pair = self.config['tpc_ch']['parameters']['dateRange'] \
                [i % len(self.config['tpc_ch']['parameters']['dateRange'])]
            date_1, date_2 = date_pair['date1'], date_pair['date2']

            # Execute all queries from query set 1 and all but one from query set 2.
            self._execute_and_log(self.query_1, 1, i + 1, timeout=1800, date_1=date_1, date_2=date_2)
            self._execute_and_log(self.query_6, 6, i + 1, timeout=1800, date_1=date_1, date_2=date_2)
            self._execute_and_log(self.query_12, 12, i + 1, timeout=1800, date_1=date_1, date_2=date_2)
            self._execute_and_log(self.query_14, 14, i + 1, timeout=1800, date_1=date_1, date_2=date_2)
            self._execute_and_log(self.query_15, 15, i + 1, timeout=1800, date_1=date_1, date_2=date_2)
            self._execute_and_log(self.query_20, 20, i + 1, timeout=1800, date_1=date_1, date_2=date_2)


if __name__ == '__main__':
    IndexedAnalyticalQuery().invoke()
