import logging

from src.asterixdb.tpc_ch.load_dataverse.executor import AbstractLoadDataverseRunnable

logger = logging.getLogger(__name__)


class LoadBasicDataverse(AbstractLoadDataverseRunnable):
    def perform_benchmark(self):
        logger.info('Building dataverse TPC_CH.')
        self.create_dataverse()
        logger.info('Loading dataverse TPC_CH.')
        self.load_dataverse()


if __name__ == '__main__':
    LoadBasicDataverse().invoke()
