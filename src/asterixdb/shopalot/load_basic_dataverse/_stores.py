import logging

from src.asterixdb.shopalot.executor import AbstractShopALotRunnable

logger = logging.getLogger(__name__)


class LoadBasicStoresDataverse(AbstractShopALotRunnable):
    def __init__(self):
        super().__init__()

    def _benchmark_load_sarr(self):
        results = self.execute_sqlpp("""
            DROP DATAVERSE ShopALot.SARR IF EXISTS;
            CREATE DATAVERSE ShopALot.SARR;
            USE ShopALot.SARR;
            
            CREATE TYPE StoresType AS { store_id: string };
            CREATE DATASET Stores (StoresType) PRIMARY KEY store_id;
            CREATE DATASET StoresBuffer (StoresType) PRIMARY KEY store_id;
            LOAD DATASET ShopALot.SARR.Stores USING localfs (
                ("path"="%s"), ("format"="json")
            );
          """ % ('localhost:///' + self.config['shopalot']['stores']['sarrDataverse']['fullFilename']))
        self.log_results(results)

    def _benchmark_load_atom(self):
        results = self.execute_sqlpp("""
            DROP DATAVERSE ShopALot.ATOM IF EXISTS;
            CREATE DATAVERSE ShopALot.ATOM;
            USE ShopALot.ATOM;
            
            CREATE TYPE StoresType AS { store_id: string };
            CREATE DATASET Stores (StoresType) PRIMARY KEY store_id;
            CREATE DATASET StoresBuffer (StoresType) PRIMARY KEY store_id;
            LOAD DATASET ShopALot.ATOM.Stores USING localfs (
                ("path"="%s"), ("format"="json")
            );
          """ % ('localhost:///' + self.config['shopalot']['stores']['atomDataverse']['fullFilename']))
        self.log_results(results)

    def perform_benchmark(self):
        if self.dataverse == self.SARR_DATAVERSE:
            logger.info('Executing load_basic_dataverse on Stores for SARR.')
            self._benchmark_load_sarr()
        else:
            logger.info('Executing load_basic_dataverse on Stores for ATOM.')
            self._benchmark_load_atom()


if __name__ == '__main__':
    LoadBasicStoresDataverse().invoke()
