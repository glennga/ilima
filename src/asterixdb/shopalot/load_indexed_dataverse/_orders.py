import logging

from src.asterixdb.shopalot.executor import AbstractShopALotRunnable

logger = logging.getLogger(__name__)


class LoadIndexedOrdersDataverse(AbstractShopALotRunnable):
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

            CREATE INDEX ordersItemQtyIdx ON Orders(UNNEST items SELECT qty : int);
            CREATE INDEX ordersItemProductIdx ON Orders(UNNEST items SELECT product_id : string);
        """ % ('localhost:///' + self.config['shopalot']['orders']['sarrDataverse']['fullFilename']))
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
            
            CREATE INDEX ordersItemQtyIdx ON Orders(item.qty : int);
            CREATE INDEX ordersItemProductIdx ON Orders(item.product_id : string);
        """ % ('localhost:///' + self.config['shopalot']['orders']['atomDataverse']['fullFilename']))
        self.log_results(results)

    def perform_benchmark(self):
        if self.dataverse == self.SARR_DATAVERSE:
            logger.info('Executing load_indexed_dataverse on Orders for SARR.')
            self._benchmark_load_sarr()
        else:
            logger.info('Executing load_indexed_dataverse on Orders for ATOM.')
            self._benchmark_load_atom()


if __name__ == '__main__':
    LoadIndexedOrdersDataverse().invoke()
