import logging

from couchbase.exceptions import ProtocolException, CollectionAlreadyExistsException
from couchbase.management.collections import CollectionSpec
from src.couchbase.tpc_ch.load_bucket.executor import AbstractLoadBucketRunnable

logger = logging.getLogger(__name__)


class LoadIndexedBucket(AbstractLoadBucketRunnable):
    def _create_indexes(self):
        self.log_results(self.execute_n1ql(f"""
            CREATE INDEX orderlineDelivDateIdx ON {self.bucket_name}._default.Orders (
                DISTINCT ARRAY OL.ol_delivery_d
                FOR OL IN o_orderline 
                END
            );
        """))
        self.log_results(self.execute_n1ql(f"""
            CREATE INDEX orderlineItemIdx ON {self.bucket_name}._default.Orders (
                DISTINCT ARRAY OL.ol_i_id
                FOR OL IN o_orderline 
                END
            );
        """))

    def _build_and_load_bucket(self):
        logger.info('Building bucket for TPC_CH.')
        self.create_bucket()
        logger.info('Loading bucket for TPC_CH.')
        self.load_bucket()
        logger.info('Creating array indexes for TPC_CH.')
        self._create_indexes()

    def perform_benchmark(self):
        try:
            self.connect_bucket()

        except ProtocolException:
            logger.info('Bucket not found.')
            self._build_and_load_bucket()
            return

        for collection in self.COLLECTION_NAMES:
            try:
                logger.info(f'Checking if {collection} collection exists.')
                collection_manager = self.bucket.collections()
                collection_manager.create_collection(CollectionSpec(collection_name=collection))

                # If we make it this far, then rebuild the bucket.
                logger.info('Collection does not exist. Rebuilding bucket.')
                self._build_and_load_bucket()
                return

            except CollectionAlreadyExistsException:
                pass

        logger.info('All collections found.')
        logger.info('Creating array indexes for TPC_CH.')
        self._create_indexes()


if __name__ == '__main__':
    LoadIndexedBucket().invoke()
