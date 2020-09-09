import __init__
import logging

from src.asterixdb.executor import AbstractBenchmarkRunnable

logger = logging.getLogger(__name__)


class LoadIndexedOrdersDataverse(AbstractBenchmarkRunnable):
    def __init__(self):
        self.sarr_json = "dbh-2074.ics.uci.edu:///home/ggalvizo/datagen/shopalot-output/SARR-OrdersFull.json"
        self.atom_json = "dbh-2074.ics.uci.edu:///home/ggalvizo/datagen/shopalot-output/ATOM-OrdersFull.json"
        super().__init__()

    def _benchmark_load_sarr(self):
        results = self.execute_sqlpp("""
            DROP DATAVERSE ShopALot.SARR IF EXISTS;
            CREATE DATAVERSE ShopALot.SARR;
            USE ShopALot.SARR;

            CREATE TYPE OrdersType AS {
                order_id: string,
                total_price: float,
                time_placed: string,
                user_id: string,
                store_id: string,
                items: [{
                    item_id: string,
                    qty: integer,
                    price: float,
                    product_id: string
                }]
            };

            CREATE DATASET Orders (OrdersType) PRIMARY KEY order_id;

            LOAD DATASET ShopALot.SARR.Orders USING localfs (
                ("path"="%s"), ("format"="json")
            );

            CREATE INDEX ordersItemQtyIdx ON Orders(UNNEST items SELECT qty);
            CREATE INDEX ordersItemProductIdx ON Orders(UNNEST items SELECT product_id);
        """ % self.sarr_json)
        self.log_results(results)

    def _benchmark_load_atom(self):
        results = self.execute_sqlpp("""
            DROP DATAVERSE ShopALot.ATOM IF EXISTS;
            CREATE DATAVERSE ShopALot.ATOM;
            USE ShopALot.ATOM;

            CREATE TYPE OrdersType AS {
                order_id: string,
                total_price: float,
                time_placed: string,
                user_id: string,
                store_id: string,
                item: {
                    item_id: string,
                    qty: integer,
                    price: float,
                    product_id: string
                }
            };

            CREATE DATASET Orders (OrdersType) PRIMARY KEY order_id;

            LOAD DATASET ShopALot.ATOM.Orders USING localfs (
                ("path"="%s"), ("format"="json")
            );
            
            CREATE INDEX ordersItemQtyIdx ON Orders(item.qty);
            CREATE INDEX ordersItemProductIdx ON Orders(item.product_id);
        """ % self.atom_json)
        self.log_results(results)

    def perform_benchmark(self):
        logger.info('Executing load_indexed_dataverse on Orders for SARR.')
        self._benchmark_load_sarr()

        logger.info('Executing load_indexed_dataverse on Orders for ATOM.')
        self._benchmark_load_atom()

    def perform_post(self):
        pass


if __name__ == '__main__':
    LoadIndexedOrdersDataverse()()
