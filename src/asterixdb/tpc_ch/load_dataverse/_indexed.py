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
        results = self.execute_sqlpp(f"""
            FROM    `Metadata`.`Dataset` D
            WHERE   D.DataverseName = "TPC_CH" AND 
                    D.DatasetName = "Orders"
            SELECT  1;
        """)
        if len(results['results']) != 0:
            logger.info('Orders dataset found. Skipping the database load.')
            logger.info('Creating array indexes on TPC_CH.')
            self._create_indexes()

        else:
            logger.info('Building dataverse TPC_CH.')
            self.create_dataverse()
            logger.info('Creating array indexes on TPC_CH.')
            self._create_indexes()
            logger.info('Loading dataverse TPC_CH.')
            self.load_dataverse()


if __name__ == '__main__':
    LoadIndexedDataverse().invoke()
