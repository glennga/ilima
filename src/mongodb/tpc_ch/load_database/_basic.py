import logging

from src.mongodb.tpc_ch.load_database.executor import AbstractLoadDatabaseRunnable

logger = logging.getLogger(__name__)


class LoadBasicDatabase(AbstractLoadDatabaseRunnable):
    def perform_benchmark(self):
        logger.info('Loading database.')
        self.load_collection()


if __name__ == '__main__':
    LoadBasicDatabase().invoke()
