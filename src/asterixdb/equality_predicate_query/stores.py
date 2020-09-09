import __init__
import logging

from src.asterixdb.equality_predicate_query.executor import AbstractEqualityPredicateQuery

logger = logging.getLogger(__name__)


class StoresEqualityPredicateQuery(AbstractEqualityPredicateQuery):
    def __init__(self):
        super().__init__(index_names=['storesCatIdx'], dataset_name='Stores')

    def benchmark_atom(self, working_sample_objects, atom_num):
        if not self._enable_index_only(atom_num == 1):
            return False

        for i, sample_store in enumerate(working_sample_objects):
            sample_category = sample_store['category']
            results = self.execute_sqlpp(f"""
                 SELECT S
                 FROM ShopALot.ATOM.Stores S
                 WHERE S.category = "{sample_category}";
             """)
            if len(results['results']) == 0:
                logger.error(f'Query {i + 1} was not successful! "{sample_category}" not found.')
                return False

            logger.debug(f'Query {i + 1} was successful. Execution time: {results["metrics"]["elapsedTime"]}')
            results['results'] = results['results'][:10]  # This is high-selectivity query, do not save all results.
            results['areResultsTruncated'] = True
            results['runNumber'] = i + 1
            self.log_results(results)

        return True

    def benchmark_sarr(self, working_sample_objects, sarr_num):
        for i, sample_store in enumerate(working_sample_objects):
            sample_category = sample_store['category'][0]
            if sarr_num == 1:
                results = self.execute_sqlpp(f"""
                    SELECT S
                    FROM ShopALot.SARR.Stores S
                    UNNEST S.categories SC 
                    WHERE SC = "{sample_category}";
                """)
            elif sarr_num == 2:
                results = self.execute_sqlpp(f"""
                    SELECT DISTINCT S
                    FROM ShopALot.SARR.Stores S
                    UNNEST S.categories SC 
                    WHERE SC = "{sample_category}";
                """)
            elif sarr_num == 3:
                results = self.execute_sqlpp(f"""
                    SELECT S
                    FROM ShopALot.SARR.Stores S
                    "{sample_category}" IN S.categories;
                """)
            else:
                results = self.execute_sqlpp(f"""
                    SELECT S
                    FROM ShopALot.SARR.Stores S
                    WHERE LEN(S.categories) > 0 AND
                         (EVERY SC IN S.categories SATISFIES SC = "{sample_category}");
                """)

            if len(results['results']) == 0:
                logger.error(f'Query {i + 1} was not successful! "{sample_category}" not found.')
                return False

            logger.debug(f'Query {i + 1} was successful. Execution time: {results["metrics"]["elapsedTime"]}')
            results['results'] = results['results'][:10]  # This is high-selectivity query, do not save all results.
            results['areResultsTruncated'] = True
            results['runNumber'] = i + 1
            self.log_results(results)

        return True


if __name__ == '__main__':
    StoresEqualityPredicateQuery()()
