import __init__
import logging

from src.asterixdb.executor import AbstractBenchmarkRunnable

logger = logging.getLogger(__name__)


class LoadIndexedUsersDataverse(AbstractBenchmarkRunnable):
    def __init__(self):
        self.sarr_json = "dbh-2074.ics.uci.edu:///home/ggalvizo/ilima/resources/full_data/SARR-UsersFull.json"
        self.atom_json = "dbh-2074.ics.uci.edu:///home/ggalvizo/ilima/resources/full_data/ATOM-UsersFull.json"
        super().__init__()

    def _benchmark_load_sarr(self):
        results = self.execute_sqlpp("""
            DROP DATAVERSE ShopALot.SARR IF EXISTS;
            CREATE DATAVERSE ShopALot.SARR;
            USE ShopALot.SARR;

            CREATE TYPE UsersType AS { user_id: string };
            CREATE DATASET Users (UsersType) PRIMARY KEY user_id;
            LOAD DATASET ShopALot.SARR.Users USING localfs (
                ("path"="%s"), ("format"="json")
            );

            CREATE INDEX usersNumberIdx ON Users(UNNEST phones SELECT number : string ?);
        """ % self.sarr_json)
        self.log_results(results)

    def _benchmark_load_atom(self):
        results = self.execute_sqlpp("""
            DROP DATAVERSE ShopALot.ATOM IF EXISTS;
            CREATE DATAVERSE ShopALot.ATOM;
            USE ShopALot.ATOM;

            CREATE TYPE UsersType AS { user_id: string };
            CREATE DATASET Users (UsersType) PRIMARY KEY user_id;
            LOAD DATASET ShopALot.ATOM.Users USING localfs (
                ("path"="%s"), ("format"="json")
            );

            CREATE INDEX usersNumberIdx ON Users (phone.number : string ?);
        """ % self.atom_json)
        self.log_results(results)

    def perform_benchmark(self):
        logger.info('Executing load_indexed_dataverse on Users for SARR.')
        self._benchmark_load_sarr()

        logger.info('Executing load_indexed_dataverse on Users for ATOM.')
        self._benchmark_load_atom()

    def perform_post(self):
        pass


if __name__ == '__main__':
    LoadIndexedUsersDataverse()()
