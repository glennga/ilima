import __init__
import logging

from src.asterixdb.shopalot.load_basic_dataset.executor import AbstractLoadBasicDataset

logger = logging.getLogger(__name__)


class LoadBasicOrdersDataset(AbstractLoadBasicDataset):
    PATH_PREFIX = "dbh-2074.ics.uci.edu:///home/ggalvizo/ilima/resources/"
    SARR_PATH = PATH_PREFIX + "SARR-OrdersFull.json"
    ATOM_PATH = PATH_PREFIX + "ATOM-OrdersFull.json"

    def __init__(self):
        super().__init__(sarr_type_ddl="CREATE TYPE OrdersType AS { order_id: string };",
                         atom_type_ddl="CREATE TYPE OrdersType AS { order_id: string };")

    def benchmark_sarr(self, run_number):
        results = self.execute_sqlpp("""
            DROP DATASET ShopALot.SARR.Orders IF EXISTS;

            USE ShopALot.SARR;
            CREATE DATASET Orders (OrdersType) PRIMARY KEY order_id;
            
            LOAD DATASET ShopALot.SARR.Orders USING localfs (
                ("path"="%s"), ("format"="json")
            );
        """ % self.SARR_PATH)
        if results['status'] != 'success':
            logger.error(f'Result of bulk-loading was not success, but {results["status"]}.')
            return False

        logger.info(f'Run {run_number + 1} has finished executing.')
        results['runNumber'] = run_number + 1
        self.log_results(results)
        return True

    def benchmark_atom(self, run_number):
        results = self.execute_sqlpp("""
            DROP DATASET ShopALot.ATOM.Orders IF EXISTS;

            USE ShopALot.ATOM;
            CREATE DATASET Orders (OrdersType) PRIMARY KEY order_id;

            LOAD DATASET ShopALot.ATOM.Orders USING localfs (
                ("path"="%s"), ("format"="json")
            );
         """ % self.ATOM_PATH)
        if results['status'] != 'success':
            logger.error(f'Result of bulk-loading was not success, but {results["status"]}.')
            return False

        logger.info(f'Run {run_number + 1} has finished executing.')
        results['runNumber'] = run_number + 1
        self.log_results(results)
        return True


if __name__ == '__main__':
    LoadBasicOrdersDataset().invoke()
