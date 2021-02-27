import logging
import json

from src.asterixdb.shopalot.equality_predicate_query.executor import AbstractEqualityPredicateQuery
from src.asterixdb.shopalot.datagen import AbstractStoresDatagen

logger = logging.getLogger(__name__)


class StoresEqualityPredicateQuery(AbstractEqualityPredicateQuery):
    def __init__(self):
        with open('config/shopalot.json') as config_file:
            config_json = json.load(config_file)  # Determine how large our dataset size is.
            logger.info(f'Using the ShopALot config file: {config_json}')

        super().__init__(**{
            'dataset_size': config_json['stores']['idRange']['end'] - config_json['stores']['idRange']['start'],
            'chunk_size': config_json['stores']['chunkSize'],
            'datagen_class': AbstractStoresDatagen,
            'index_names': ['storesCatIdx'],
            'dataset_name': 'Stores',
            'num_queries': 40
        })

    def benchmark_atom(self, working_sample_objects, atom_num):
        if not self.enable_index_only(atom_num == 1):
            return False

        for i, sample_store in enumerate(working_sample_objects):
            sample_category = sample_store['category']
            results = self.execute_sqlpp(f"""
                 SELECT S
                 FROM ShopALot.ATOM.Stores S
                 WHERE S.category = "{sample_category}"
                 LIMIT 10;
             """)
            if len(results['results']) == 0:
                logger.error(f'Query {i + 1} was not successful! "{sample_category}" not found.')
                return False

            logger.debug(f'Query {i + 1} was successful. Execution time: {results["metrics"]["elapsedTime"]}')
            results['runNumber'] = i + 1
            self.log_results(results)

        return True

    def benchmark_sarr(self, working_sample_objects, sarr_num):
        for i, sample_store in enumerate(working_sample_objects):
            sample_category = sample_store['categories'][0]
            if sarr_num == 1:
                results = self.execute_sqlpp(f"""
                    SELECT S
                    FROM ShopALot.SARR.Stores S
                    UNNEST S.categories SC 
                    WHERE SC = "{sample_category}"
                    LIMIT 10;
                """)
            elif sarr_num == 2:
                results = self.execute_sqlpp(f"""
                    SELECT DISTINCT S
                    FROM ShopALot.SARR.Stores S
                    UNNEST S.categories SC 
                    WHERE SC = "{sample_category}"
                    LIMIT 10;
                """)
            elif sarr_num == 3:
                results = self.execute_sqlpp(f"""
                    SELECT S
                    FROM ShopALot.SARR.Stores S
                    WHERE "{sample_category}" IN S.categories
                    LIMIT 10;
                """)
            else:
                results = self.execute_sqlpp(f"""
                    SELECT S
                    FROM ShopALot.SARR.Stores S
                    WHERE LEN(S.categories) > 0 AND
                         (EVERY SC IN S.categories SATISFIES SC = "{sample_category}")
                    LIMIT 10;
                """)

            if len(results['results']) == 0:
                logger.error(f'Query {i + 1} was not successful! "{sample_category}" not found.')
                return False

            logger.debug(f'Query {i + 1} was successful. Execution time: {results["metrics"]["elapsedTime"]}')
            results['runNumber'] = i + 1
            self.log_results(results)

        return True


if __name__ == '__main__':
    StoresEqualityPredicateQuery().invoke()
