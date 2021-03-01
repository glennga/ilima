import logging

from src.asterixdb.shopalot.load_indexed_dataset.executor import AbstractLoadIndexedDataset

logger = logging.getLogger(__name__)


class LoadIndexedOrdersDataset(AbstractLoadIndexedDataset):
    def __init__(self):
        super().__init__(sarr_type_ddl="CREATE TYPE OrdersType AS { order_id: string };",
                         atom_type_ddl="CREATE TYPE OrdersType AS { order_id: string };")

    def benchmark_sarr(self, run_number):
        results = self.execute_sqlpp("""
            DROP DATASET ShopALot.SARR.Orders IF EXISTS;

            USE ShopALot.SARR;
            CREATE DATASET Orders (OrdersType) PRIMARY KEY order_id;
            CREATE INDEX ordersItemQtyProductIdx ON Orders(UNNEST items SELECT qty : int ?, product_id : string ?);
            CREATE INDEX ordersProductItemQtyIdx ON Orders(UNNEST items SELECT product_id : string ?, qty : int ?);

            LOAD DATASET ShopALot.SARR.Orders USING localfs (
                ("path"="%s"), ("format"="json")
            );
        """ % ('localhost:///' + self.config['shopalot']['orders']['sarrDataverse']['fullFilename']))
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
            CREATE INDEX ordersItemQtyProductIdx ON Orders(item.qty : int ?, item.product_id : string ?);
            CREATE INDEX ordersProductItemQtyIdx ON Orders(item.product_id : string ?, item.qty : int ?);

            LOAD DATASET ShopALot.ATOM.Orders USING localfs (
                ("path"="%s"), ("format"="json")
            );
        """ % ('localhost:///' + self.config['shopalot']['orders']['atomDataverse']['fullFilename']))
        if results['status'] != 'success':
            logger.error(f'Result of bulk-loading was not success, but {results["status"]}.')
            return False

        logger.info(f'Run {run_number + 1} has finished executing.')
        results['runNumber'] = run_number + 1
        self.log_results(results)
        return True


if __name__ == '__main__':
    LoadIndexedOrdersDataset().invoke()
