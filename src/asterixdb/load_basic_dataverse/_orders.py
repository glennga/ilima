import __init__
import logging

from src.asterixdb.executor import AbstractBenchmarkRunnable

logger = logging.getLogger(__name__)


class LoadBasicOrdersDataverse(AbstractBenchmarkRunnable):
    # PATH_PREFIX = "localhost:///Users/glenngalvizo/Documents/Projects/asterixdb/ilima-repo/resources/"
    # SARR_PATH = PATH_PREFIX + "SARR-OrdersSample.json"
    # ATOM_PATH = PATH_PREFIX + "ATOM-OrdersSample.json"
    PATH_PREFIX = "dbh-2074.ics.uci.edu:///home/ggalvizo/ilima/resources/"
    SARR_PATH = PATH_PREFIX + "SARR-OrdersFull.json"
    ATOM_PATH = PATH_PREFIX + "ATOM-OrdersFull.json"

    def __init__(self):
        super().__init__()

    def _benchmark_load_sarr(self):
        results = self.execute_sqlpp("""
            DROP DATAVERSE ShopALot.SARR IF EXISTS;
            CREATE DATAVERSE ShopALot.SARR;
            USE ShopALot.SARR;

            CREATE TYPE OrdersType AS { order_id: string };
            CREATE DATASET Orders (OrdersType) PRIMARY KEY order_id;
            CREATE DATASET OrdersBuffer (OrdersType) PRIMARY KEY order_id;
            LOAD DATASET ShopALot.SARR.Orders USING localfs (
                ("path"="%s"), ("format"="json")
            );
        """ % self.SARR_PATH)
        self.log_results(results)

    def _benchmark_load_atom(self):
        results = self.execute_sqlpp("""
            DROP DATAVERSE ShopALot.ATOM IF EXISTS;
            CREATE DATAVERSE ShopALot.ATOM;
            USE ShopALot.ATOM;

            CREATE TYPE OrdersType AS { order_id: string };
            CREATE DATASET Orders (OrdersType) PRIMARY KEY order_id;
            CREATE DATASET OrdersBuffer (OrdersType) PRIMARY KEY order_id;
            LOAD DATASET ShopALot.ATOM.Orders USING localfs (
                ("path"="%s"), ("format"="json")
            );
        """ % self.ATOM_PATH)
        self.log_results(results)

    def perform_benchmark(self):
        if self.dataverse == self.SARR_DATAVERSE:
            logger.info('Executing load_basic_dataverse on Orders for SARR.')
            self._benchmark_load_sarr()
        else:
            logger.info('Executing load_basic_dataverse on Orders for ATOM.')
            self._benchmark_load_atom()


if __name__ == '__main__':
    LoadBasicOrdersDataverse().invoke()
