import __init__
import logging

from src.asterixdb.load_indexed_dataset.executor import AbstractLoadIndexedDataset

logger = logging.getLogger(__name__)


class LoadIndexedOrdersDataset(AbstractLoadIndexedDataset):
    def __init__(self):
        self.sarr_json = "dbh-2074.ics.uci.edu:///home/ggalvizo/ilima/resources/full_data/SARR-OrdersEighth.json"
        self.atom_json = "dbh-2074.ics.uci.edu:///home/ggalvizo/ilima/resources/full_data/ATOM-OrdersEighth.json"
        super().__init__(sarr_type_ddl="CREATE TYPE OrdersType AS { order_id: string };",
                         atom_type_ddl="CREATE TYPE OrdersType AS { order_id: string };")

    def benchmark_sarr(self, run_number):
        results = self.execute_sqlpp("""
            DROP DATASET ShopALot.SARR.Orders IF EXISTS;

            USE ShopALot.SARR;
            CREATE DATASET Orders (OrdersType) PRIMARY KEY order_id;
            CREATE INDEX ordersItemQtyIdx ON Orders(UNNEST items SELECT qty : string ?);
            CREATE INDEX ordersItemProductIdx ON Orders(UNNEST items SELECT product_id : string ?);

            LOAD DATASET ShopALot.SARR.Orders USING localfs (
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
            DROP DATASET ShopALot.ATOM.Orders IF EXISTS;

            USE ShopALot.ATOM;
            CREATE DATASET Orders (OrdersType) PRIMARY KEY order_id;
            CREATE INDEX ordersItemQtyIdx ON Orders(item.qty : string ?);
            CREATE INDEX ordersItemProductIdx ON Orders(item.product_id : string ?);

            LOAD DATASET ShopALot.ATOM.Orders USING localfs (
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
    LoadIndexedOrdersDataset()()
