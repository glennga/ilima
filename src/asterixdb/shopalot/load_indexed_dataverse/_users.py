import __init__
import logging

from src.asterixdb.shopalot.executor import AbstractShopALotRunnable

logger = logging.getLogger(__name__)


class LoadIndexedUsersDataverse(AbstractShopALotRunnable):
    PATH_PREFIX = "dbh-2074.ics.uci.edu:///home/ggalvizo/ilima/resources/"
    SARR_PATH = PATH_PREFIX + "SARR-UsersFull.json"
    ATOM_PATH = PATH_PREFIX + "ATOM-UsersFull.json"

    def _benchmark_load_sarr(self):
        results = self.execute_sqlpp("""
            DROP DATAVERSE ShopALot.SARR IF EXISTS;
            CREATE DATAVERSE ShopALot.SARR;
            USE ShopALot.SARR;

            CREATE TYPE UsersType AS { user_id: string };
            CREATE DATASET Users (UsersType) PRIMARY KEY user_id;
            CREATE DATASET UsersBuffer (UsersType) PRIMARY KEY user_id;
            LOAD DATASET ShopALot.SARR.Users USING localfs (
                ("path"="%s"), ("format"="json")
            );

            CREATE INDEX usersNumberIdx ON Users(UNNEST phones SELECT number : string ?);
        """ % self.SARR_PATH)
        self.log_results(results)

    def _benchmark_load_atom(self):
        results = self.execute_sqlpp("""
            DROP DATAVERSE ShopALot.ATOM IF EXISTS;
            CREATE DATAVERSE ShopALot.ATOM;
            USE ShopALot.ATOM;

            CREATE TYPE UsersType AS { user_id: string };
            CREATE DATASET Users (UsersType) PRIMARY KEY user_id;
            CREATE DATASET UsersBuffer (UsersType) PRIMARY KEY user_id;
            LOAD DATASET ShopALot.ATOM.Users USING localfs (
                ("path"="%s"), ("format"="json")
            );

            CREATE INDEX usersNumberIdx ON Users (phone.number : string ?);
        """ % self.ATOM_PATH)
        self.log_results(results)

    def perform_benchmark(self):
        if self.dataverse == self.SARR_DATAVERSE:
            logger.info('Executing load_indexed_dataverse on Users for SARR.')
            self._benchmark_load_sarr()
        else:
            logger.info('Executing load_indexed_dataverse on Users for ATOM.')
            self._benchmark_load_atom()


if __name__ == '__main__':
    LoadIndexedUsersDataverse().invoke()
