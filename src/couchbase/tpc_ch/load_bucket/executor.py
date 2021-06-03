import logging
import abc

from couchbase.management.collections import CollectionSpec
from src.couchbase.tpc_ch.executor import AbstractTPCCHRunnable

logger = logging.getLogger(__name__)


class AbstractLoadBucketRunnable(AbstractTPCCHRunnable, abc.ABC):
    COLLECTION_NAMES = ['Customer', 'Nation', 'Orders', 'Stock', 'Item', 'Region', 'Supplier']

    def create_bucket(self):
        logger.info('Building bucket.')
        self.initialize_bucket()

        for collection in self.COLLECTION_NAMES:
            logger.info(f'Creating collection {collection}.')
            collection_manager = self.bucket.collections()
            collection_manager.create_collection(CollectionSpec(collection_name=collection))

    def load_bucket(self):
        data_path = 'file:///' + self.config['tpc_ch']['dataPath']

        self.log_results(self.execute_cbimport(f'{data_path}/customer.json', collection='Customer'))
        self.log_results(self.execute_cbimport(f'{data_path}/nation.json', collection='Nation'))
        self.log_results(self.execute_cbimport(f'{data_path}/orders.json', collection='Orders'))
        self.log_results(self.execute_cbimport(f'{data_path}/stock.json', collection='Stock'))
        self.log_results(self.execute_cbimport(f'{data_path}/item.json', collection='Item'))
        self.log_results(self.execute_cbimport(f'{data_path}/region.json', collection='Region'))
        self.log_results(self.execute_cbimport(f'{data_path}/supplier.json', collection='Supplier'))
