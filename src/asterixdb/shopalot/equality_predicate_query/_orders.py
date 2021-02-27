import logging
import json

from src.asterixdb.shopalot.equality_predicate_query.executor import AbstractEqualityPredicateQuery
from src.asterixdb.shopalot.datagen import AbstractOrdersDatagen

logger = logging.getLogger(__name__)


class OrdersEqualityPredicateQuery(AbstractEqualityPredicateQuery):
    def __init__(self):
        with open('config/shopalot.json') as config_file:
            config_json = json.load(config_file)  # Determine how large our dataset size is.
            logger.info(f'Using the ShopALot config file: {config_json}')

        super().__init__(**{
            'dataset_size': config_json['orders']['idRange']['end'] - config_json['orders']['idRange']['start'],
            'user_start_id': config_json['users']['idRange']['start'],
            'user_end_id': config_json['users']['idRange']['end'],
            'store_start_id': config_json['stores']['idRange']['start'],
            'store_end_id': config_json['stores']['idRange']['end'],
            'chunk_size': config_json['orders']['chunkSize'],
            'datagen_class': AbstractOrdersDatagen,
            'index_names': ['ordersItemQtyProductIdx', 'ordersProductItemQtyIdx'],
            'dataset_name': 'Orders',
            'num_queries': 500
        })

    def benchmark_atom(self, working_sample_objects, atom_num):
        if not self.enable_index_only(atom_num == 1):
            return False

        for i, sample_order in enumerate(working_sample_objects):
            sample_order_qty = sample_order['item']['qty']
            sample_order_product = sample_order['item']['product_id']
            results = self.execute_sqlpp(f"""
                SELECT O
                FROM ShopALot.ATOM.Orders O
                WHERE O.item.qty = {sample_order_qty} AND 
                      O.item.product_id = "{sample_order_product}";
            """)
            if len(results['results']) == 0:
                logger.error(f'Query {i + 1} was not successful! "[{sample_order_qty}, '
                             f'{sample_order_product}]" not found.')
                return False

            logger.debug(f'Query {i + 1} was successful. Execution time: {results["metrics"]["elapsedTime"]}')
            results['runNumber'] = i + 1
            self.log_results(results)

        return True

    def benchmark_sarr(self, working_sample_objects, sarr_num):
        for i, sample_order in enumerate(working_sample_objects):
            sample_order_qty = sample_order['items'][0]['qty']
            sample_order_product = sample_order['items'][0]['product_id']
            if sarr_num == 1:
                results = self.execute_sqlpp(f"""
                    SELECT O
                    FROM ShopALot.SARR.Orders O
                    UNNEST O.items OI
                    WHERE OI.qty = {sample_order_qty} AND 
                          OI.product_id = "{sample_order_product}"
                    LIMIT 10;
                """)
            elif sarr_num == 2:
                results = self.execute_sqlpp(f"""
                    SELECT DISTINCT O
                    FROM ShopALot.SARR.Orders O
                    UNNEST O.items OI
                    WHERE OI.qty = {sample_order_qty} AND 
                          OI.product_id = "{sample_order_product}"
                    LIMIT 10;
                """)
            elif sarr_num == 3:
                results = self.execute_sqlpp(f"""
                    SELECT O    
                    FROM ShopALot.SARR.Orders O
                    WHERE (SOME OI IN O.items SATISFIES OI.qty = {sample_order_qty} AND 
                           OI.product_id = "{sample_order_product}")
                    LIMIT 10;
                """)
            else:
                results = self.execute_sqlpp(f"""
                    SELECT O    
                    FROM ShopALot.SARR.Orders O
                    WHERE LEN(O.items) > 0 AND 
                          (EVERY OI IN O.items SATISFIES OI.qty = {sample_order_qty} AND 
                           OI.product_id = "{sample_order_product}")
                    LIMIT 10;
                """)

            if len(results['results']) == 0:
                logger.error(f'Query {i + 1} was not successful! "[{sample_order_qty}, '
                             f'{sample_order_product}]" not found.')
                return False

            logger.debug(f'Query {i + 1} was successful. Execution time: {results["metrics"]["elapsedTime"]}')
            results['runNumber'] = i + 1
            self.log_results(results)

        return True


if __name__ == '__main__':
    OrdersEqualityPredicateQuery().invoke()
