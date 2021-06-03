import logging

from src.mongodb.tpc_ch.load_database.executor import AbstractLoadDatabaseRunnable

logger = logging.getLogger(__name__)


class LoadIndexedDatabase(AbstractLoadDatabaseRunnable):
    def _create_indexes(self):
        self.database['Orders'].create_index('o_orderline.ol_delivery_d', name='orderlineDelivDateIdx')
        self.database['Orders'].create_index('o_orderline.ol_i_id', name='orderlineItemIdx')
        self.log_results(self.format_strict(self.database['Orders'].index_information()))

    def perform_benchmark(self):
        if all(c in self.database.list_collection_names() for c in self.COLLECTION_NAMES):
            logger.info('All collections found. Skipping the database load.')
            logger.info('Creating indexes.')
            self._create_indexes()

        else:
            logger.info('Loading database.')
            self.load_collection()
            logger.info('Creating indexes.')
            self._create_indexes()


if __name__ == '__main__':
    LoadIndexedDatabase().invoke()
