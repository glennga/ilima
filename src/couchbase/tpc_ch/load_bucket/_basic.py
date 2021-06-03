import logging

from src.couchbase.tpc_ch.load_bucket.executor import AbstractLoadBucketRunnable

logger = logging.getLogger(__name__)


class LoadBasicBucket(AbstractLoadBucketRunnable):
    def perform_benchmark(self):
        logger.info('Building bucket for TPC_CH.')
        self.create_bucket()
        logger.info('Loading bucket for TPC_CH.')
        self.load_bucket()


if __name__ == '__main__':
    LoadBasicBucket().invoke()
