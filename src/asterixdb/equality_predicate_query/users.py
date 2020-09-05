import __init__
import logging

from src.asterixdb.equality_predicate_query.executor import AbstractEqualityPredicateQuery

logger = logging.getLogger(__name__)


class UsersEqualityPredicateQuery(AbstractEqualityPredicateQuery):
    def __init__(self):
        super().__init__(index_name='usersNumberIdx', dataset_name='Users')

    def benchmark_atom(self, working_sample_objects, atom_num):
        if not self._enable_index_only(atom_num == 1):
            return False

        for i, sample_user in enumerate(working_sample_objects):
            sample_user_number = sample_user['phone']['number']
            results = self.execute_sqlpp(f"""
                 SELECT U
                 FROM ShopALot.ATOM.Users U
                 WHERE U.phone.number = "{sample_user_number}";
             """)
            if len(results['results']) == 0:
                logger.error(f'Query {i + 1} was not successful! "{sample_user_number}" not found.')
                return False

            logger.debug(f'Query {i + 1} was successful. Execution time: {results["metrics"]["elapsedTime"]}')
            results['runNumber'] = i + 1
            self.log_results(results)

        return True

    def benchmark_sarr(self, working_sample_objects, sarr_num):
        for i, sample_user in enumerate(working_sample_objects):
            sample_user_number = sample_user['phones'][0]['number']
            if sarr_num == 1:
                results = self.execute_sqlpp(f"""
                    SELECT U
                    FROM ShopALot.SARR.Users U
                    UNNEST U.phones UP
                    WHERE UP.number = "{sample_user_number}";
                """)
            elif sarr_num == 2:
                results = self.execute_sqlpp(f"""
                    SELECT DISTINCT U
                    FROM ShopALot.SARR.Users U
                    UNNEST U.phones UP
                    WHERE UP.number = "{sample_user_number}";
                """)
            elif sarr_num == 3:
                results = self.execute_sqlpp(f"""
                    SELECT U
                    FROM ShopALot.SARR.Users U
                    WHERE (SOME UP IN U.phones SATISFIES UP.number = "{sample_user_number}");
                """)
            else:
                results = self.execute_sqlpp(f"""
                    SELECT U
                    FROM ShopALot.SARR.Users U
                    WHERE LEN(U.phones) > 0 AND 
                         (EVERY UP IN U.phones SATISFIES UP.number = "{sample_user_number}");
                """)

            if len(results['results']) == 0:
                logger.error(f'Query {i + 1} was not successful! "{sample_user_number}" not found.')
                return False

            logger.debug(f'Query {i + 1} was successful. Execution time: {results["metrics"]["elapsedTime"]}')
            results['runNumber'] = i + 1
            self.log_results(results)

        return True


if __init__ == '__main__':
    UsersEqualityPredicateQuery()
