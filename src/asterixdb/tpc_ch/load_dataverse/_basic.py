import logging

from src.asterixdb.tpc_ch.load_dataverse.executor import AbstractLoadDataverseRunnable

logger = logging.getLogger(__name__)


class LoadBasicDataverse(AbstractLoadDataverseRunnable):
    def perform_benchmark(self):
        logger.info('Building and loading dataverse TPC_CH.')
        self._create_dataverse()


if __name__ == '__main__':
    LoadBasicDataverse().invoke()
