import __init__
import logging

from src.asterixdb.load_indexed_dataset.executor import AbstractLoadIndexedDataset

logger = logging.getLogger(__name__)


class LoadIndexedUsersDataset(AbstractLoadIndexedDataset):
    def __init__(self):
        self.sarr_json = "dbh-2074.ics.uci.edu:///home/ggalvizo/datagen/shopalot-output/SARR-UsersEighth.json"
        self.atom_json = "dbh-2074.ics.uci.edu:///home/ggalvizo/datagen/shopalot-output/ATOM-UsersEighth.json"
        super().__init__(**{
            'sarr_type_ddl': """
                CREATE TYPE UsersType AS {
                    user_id: string,
                    `name`: {
                        `first`: string,
                        `last`: string
                    },
                    phones: [{
                        `type`: string,
                        `number`: string
                    }]
                };
            """,
            'atom_type_ddl': """
                CREATE TYPE UsersType AS {
                    user_id: string,
                    `name`: {
                        `first`: string,
                        `last`: string
                    },
                    phone: {
                        `type`: string,
                        `number`: string
                    }
                };
            """
        })

    def benchmark_sarr(self, run_number):
        results = self.execute_sqlpp("""
            DROP DATASET ShopALot.SARR.Users IF EXISTS;

            USE ShopALot.SARR;
            CREATE DATASET Users (UsersType) PRIMARY KEY user_id;
            CREATE INDEX usersNumberIdx ON Users(UNNEST phones SELECT number);

            LOAD DATASET ShopALot.SARR.Users USING localfs (
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
             DROP DATASET ShopALot.ATOM.Users IF EXISTS;

             USE ShopALot.ATOM;
             CREATE DATASET Users (UsersType) PRIMARY KEY user_id;
             CREATE INDEX usersNumberIdx ON Users(phone.number);

             LOAD DATASET ShopALot.ATOM.Users USING localfs (
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
    LoadIndexedUsersDataset()()
