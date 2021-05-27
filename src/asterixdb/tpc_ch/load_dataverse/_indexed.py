import logging

from src.asterixdb.tpc_ch.load_dataverse.executor import AbstractLoadDataverseRunnable

logger = logging.getLogger(__name__)


class LoadIndexedDataverse(AbstractLoadDataverseRunnable):
    def _create_indexes(self):
        results = self.execute_sqlpp(f"""
            USE TPC_CH;
            
            CREATE INDEX orderlineDelivDateIdx ON Orders ( UNNEST o_orderline SELECT ol_delivery_d : string );
            CREATE INDEX orderlineItemIdx ON Orders ( UNNEST o_orderline SELECT ol_i_id : bigint );
        """)
        self.log_results(results)

    def perform_benchmark(self):
        logger.info('Building and loading dataverse TPC_CH.')
        self._create_dataverse()
        logger.info('Creating array indexes on TPC_CH.')
        self._create_indexes()


if __name__ == '__main__':
    LoadIndexedDataverse().invoke()