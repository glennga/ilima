import __init__
import logging

from src.asterixdb.shopalot.load_basic_dataset.executor import AbstractLoadBasicDataset

logger = logging.getLogger(__name__)


class LoadBasicStoresDataset(AbstractLoadBasicDataset):
    PATH_PREFIX = "dbh-2074.ics.uci.edu:///home/ggalvizo/ilima/resources/"
    SARR_PATH = PATH_PREFIX + "SARR-StoresFull.json"
    ATOM_PATH = PATH_PREFIX + "ATOM-StoresFull.json"

    def __init__(self):
        super().__init__(sarr_type_ddl="CREATE TYPE StoresType AS { store_id: string };",
                         atom_type_ddl="CREATE TYPE StoresType AS { store_id: string };")

    def benchmark_sarr(self, run_number):
        results = self.execute_sqlpp("""
            DROP DATASET ShopALot.SARR.Stores IF EXISTS;

            USE ShopALot.SARR;
            CREATE DATASET Stores (StoresType) PRIMARY KEY store_id;

            LOAD DATASET ShopALot.SARR.Stores USING localfs (
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
             DROP DATASET ShopALot.ATOM.Stores IF EXISTS;

             USE ShopALot.ATOM;
             CREATE DATASET Stores (StoresType) PRIMARY KEY store_id;

             LOAD DATASET ShopALot.ATOM.Stores USING localfs (
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
    LoadBasicStoresDataset().invoke()
