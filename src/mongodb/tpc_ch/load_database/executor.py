import logging
import abc

from src.mongodb.tpc_ch.executor import AbstractTPCCHRunnable

logger = logging.getLogger(__name__)


class AbstractLoadDatabaseRunnable(AbstractTPCCHRunnable, abc.ABC):
    COLLECTION_NAMES = ['Customer', 'Nation', 'Orders', 'Stock', 'Item', 'Region', 'Supplier']

    def load_collection(self):
        logger.info('Removing database.')
        self.drop_database()
        self.initialize_database()

        # Create all collections.
        self.create_collection('Customer')
        self.create_collection('Nation')
        self.create_collection('Orders')
        self.create_collection('Stock')
        self.create_collection('Item')
        self.create_collection('Region')
        self.create_collection('Supplier')

        # Load each collection.
        data_path = self.config['tpc_ch']['dataPath']
        self.log_results(self.execute_mongoimport(f'{data_path}/customer.json', 'Customer'))
        self.log_results(self.execute_mongoimport(f'{data_path}/nation.json', 'Nation'))
        self.log_results(self.execute_mongoimport(f'{data_path}/orders.json', 'Orders'))
        self.log_results(self.execute_mongoimport(f'{data_path}/stock.json', 'Stock'))
        self.log_results(self.execute_mongoimport(f'{data_path}/item.json', 'Item'))
        self.log_results(self.execute_mongoimport(f'{data_path}/region.json', 'Region'))
        self.log_results(self.execute_mongoimport(f'{data_path}/supplier.json', 'Supplier'))
