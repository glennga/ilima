import __init__
import logging

from src.asterixdb.load_indexed_dataset.executor import AbstractLoadIndexedDataset

logger = logging.getLogger(__name__)


class LoadIndexedStoresDataset(AbstractLoadIndexedDataset):
    def __init__(self):
        self.sarr_json = "dbh-2074.ics.uci.edu:///home/ggalvizo/datagen/shopalot-output/SARR-StoresEighth.json"
        self.atom_json = "dbh-2074.ics.uci.edu:///home/ggalvizo/datagen/shopalot-output/ATOM-StoresEighth.json"
        super().__init__(**{
            'sarr_type_ddl': """
                CREATE TYPE StoresType AS {
                    store_id: string,
                    `name`: string,
                    address: {
                        city: string,
                        street: string,
                        zip_code: string
                    },
                    phone: string,
                    categories: [string]
                };
            """,
            'atom_type_ddl': """
                CREATE TYPE StoresType AS {
                    store_id: string,
                    `name`: string,
                    address: {
                        city: string,
                        street: string,
                        zip_code: string
                    },
                    phone: string,
                    category: string
                };
            """
        })

    def benchmark_sarr(self, run_number):
        results = self.execute_sqlpp("""
            DROP DATASET ShopALot.SARR.Stores IF EXISTS;

            USE ShopALot.SARR;
            CREATE DATASET Stores (StoresType) PRIMARY KEY store_id;
            CREATE INDEX storesCatIdx ON Stores (UNNEST categories);

            LOAD DATASET ShopALot.SARR.Stores USING localfs (
                ("path"="%s"), ("format"="json")
            );
        """ % self.sarr_json)
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
             CREATE INDEX storesCatIdx ON Stores (category);

             LOAD DATASET ShopALot.ATOM.Stores USING localfs (
                 ("path"="%s"), ("format"="json")
             );
         """ % self.atom_json)
        if results['status'] != 'success':
            logger.error(f'Result of bulk-loading was not success, but {results["status"]}.')
            return False

        logger.info(f'Run {run_number + 1} has finished executing.')
        results['runNumber'] = run_number + 1
        self.log_results(results)
        return True


if __name__ == '__main__':
    LoadIndexedStoresDataset()()
