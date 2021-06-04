import logging
import abc
import time

from couchbase.management.collections import CollectionSpec
from src.couchbase.tpc_ch.executor import AbstractTPCCHRunnable

logger = logging.getLogger(__name__)


class AbstractLoadBucketRunnable(AbstractTPCCHRunnable, abc.ABC):
    COLLECTION_NAMES = ['Customer', 'Nation', 'Orders', 'Stock', 'Item', 'Region', 'Supplier']
    PRIMARY_KEYS = ['c_w_id, c_d_id, c_id', 'n_nationkey', 'o_w_id, o_d_id, o_id', 's_w_id, s_i_id',
                    'i_id', 'r_regionkey', 'su_suppkey']

    def _create_additional_indexes(self):
        logger.info(f'Creating additional index for collection Stock.')
        response = self.execute_n1ql(f"""
            CREATE INDEX StockIIDIdx ON `{self.bucket_name}`._default.Stock (s_i_id);
        """)
        if 'error' in response:
            logger.error('Could not build additional Stock index!')
            raise RuntimeError('Could not build additional Stock index!')
        else:
            self.log_results(response)

        logger.info(f'Creating additional index for collection Item.')
        response = self.execute_n1ql(f"""
            CREATE PRIMARY INDEX ItemPrimaryIndex ON `{self.bucket_name}`._default.Item USING GSI;
        """)
        if 'error' in response:
            logger.error('Could not build additional Item index!')
            raise RuntimeError('Could not build additional Item index!')
        else:
            self.log_results(response)

    def create_bucket(self):
        logger.info('Building bucket.')
        self.initialize_bucket()

        for collection, primary_key in zip(self.COLLECTION_NAMES, self.PRIMARY_KEYS):
            logger.info(f'Creating collection {collection}.')
            collection_manager = self.bucket.collections()
            collection_manager.create_collection(CollectionSpec(collection_name=collection))
            time.sleep(1)

            logger.info(f'Creating primary key index for collection {collection}.')
            response = self.execute_n1ql(f""" 
                CREATE INDEX {collection}PrimaryKeyIdx ON `{self.bucket_name}`._default.{collection} ({primary_key});
            """)
            if 'error' in response:
                logger.error('Could not build primary index!')
                raise RuntimeError('Could not build primary index!')
            else:
                self.log_results(response)

        # To satisfy query 20, two indexes need to be created. [ Stock (s_i_id), Item (primary) ].
        self._create_additional_indexes()

    def load_bucket(self):
        data_path = 'file:///' + self.config['tpc_ch']['dataPath']

        self.log_results(self.execute_cbimport(f'{data_path}/customer.json', collection='Customer'))
        self.log_results(self.execute_cbimport(f'{data_path}/nation.json', collection='Nation'))
        self.log_results(self.execute_cbimport(f'{data_path}/orders.json', collection='Orders'))
        self.log_results(self.execute_cbimport(f'{data_path}/stock.json', collection='Stock'))
        self.log_results(self.execute_cbimport(f'{data_path}/item.json', collection='Item'))
        self.log_results(self.execute_cbimport(f'{data_path}/region.json', collection='Region'))
        self.log_results(self.execute_cbimport(f'{data_path}/supplier.json', collection='Supplier'))
