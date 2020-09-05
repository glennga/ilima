import __init__
import logging

from src.asterixdb.executor import AbstractBenchmarkRunnable


logger = logging.getLogger(__name__)


class LoadIndexedUsersDataverseRunnable(AbstractBenchmarkRunnable):
    def __init__(self):
        self.sarr_json = "dbh-2074.ics.uci.edu:///home/ggalvizo/datagen/shopalot-output/SARR-UsersFull.json"
        self.atom_json = "dbh-2074.ics.uci.edu:///home/ggalvizo/datagen/shopalot-output/ATOM-UsersFull.json"
        super().__init__()

    def _benchmark_load_sarr(self):
        results = self._execute_sqlpp("""
            DROP DATAVERSE ShopALot.SARR IF EXISTS;
            CREATE DATAVERSE ShopALot.SARR;
            USE ShopALot.SARR;

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

            CREATE DATASET Users (UsersType) PRIMARY KEY user_id;

            LOAD DATASET ShopALot.SARR.Users USING localfs (
                ("path"="%s"), ("format"="json")
            );

            CREATE INDEX usersNumberIdx ON Users(UNNEST phones SELECT number);
        """ % self.sarr_json)
        self._log_results(results)

    def _benchmark_load_atom(self):
        results = self._execute_sqlpp("""
            DROP DATAVERSE ShopALot.ATOM IF EXISTS;
            CREATE DATAVERSE ShopALot.ATOM;
            USE ShopALot.ATOM;

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

            CREATE DATASET Users (UsersType) PRIMARY KEY user_id;

            LOAD DATASET ShopALot.ATOM.Users USING localfs (
                ("path"="%s"), ("format"="json")
            );

            CREATE INDEX usersNumberIdx ON Users (phone.number);
        """ % self.atom_json)
        self._log_results(results)

    def _perform_benchmark(self):
        logger.info('Executing load_indexed_dataverse on Users for SARR.')
        self._benchmark_load_sarr()

        logger.info('Executing load_indexed_dataverse on Users for ATOM.')
        self._benchmark_load_atom()

    def _perform_post(self):
        pass


if __name__ == '__main__':
    LoadIndexedUsersDataverseRunnable()
