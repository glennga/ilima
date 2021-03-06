import logging
import abc

from src.asterixdb.shopalot.executor import AbstractShopALotRunnable

logger = logging.getLogger(__name__)


class AbstractLoadIndexedDataset(AbstractShopALotRunnable, abc.ABC):
    NUMBER_OF_REPEATS = 3

    def __init__(self, **kwargs):
        self.sarr_type_ddl = kwargs['sarr_type_ddl']
        self.atom_type_ddl = kwargs['atom_type_ddl']
        super().__init__()

    @abc.abstractmethod
    def benchmark_sarr(self, run_number):
        pass

    @abc.abstractmethod
    def benchmark_atom(self, run_number):
        pass

    def perform_benchmark(self):
        if self.dataverse == self.SARR_DATAVERSE:
            logger.info('Executing load_indexed_dataset on Users for SARR.')
            logger.info('Starting Algebricks-layer bulk loading for SARR.')
            logger.info('Creating empty dataverse for SARR.')
            results = self.execute_sqlpp(f"""
                DROP DATAVERSE ShopALot.SARR IF EXISTS;
                CREATE DATAVERSE ShopALot.SARR;
                USE ShopALot.SARR;
                {self.sarr_type_ddl}
                """)
            if results['status'] != 'success':
                logger.error(f'Result of SARR dataverse creation was not success, but {results["status"]}.')
                return
            self.log_results(results)

            for i in range(self.NUMBER_OF_REPEATS):
                if not self.benchmark_sarr(i):
                    return
        else:
            logger.info('Executing load_indexed_dataset on Users for ATOM.')
            logger.info('Starting Algebricks-layer bulk loading for ATOM.')
            logger.info('Creating empty dataverse for ATOM.')
            results = self.execute_sqlpp(f"""
                DROP DATAVERSE ShopALot.ATOM IF EXISTS;
                CREATE DATAVERSE ShopALot.ATOM;
                USE ShopALot.ATOM;
                {self.atom_type_ddl}
            """)
            if results['status'] != 'success':
                logger.error(f'Result of ATOM dataverse creation was not success, but {results["status"]}.')
                return
            self.log_results(results)

            for i in range(self.NUMBER_OF_REPEATS):
                if not self.benchmark_atom(i):
                    return

    def perform_post(self):
        logger.info('Dropping the SARR and ATOM dataverses.')
        results = self.execute_sqlpp("""
            DROP DATAVERSE ShopALot.ATOM IF EXISTS;
            DROP DATAVERSE ShopALot.SARR IF EXISTS;
        """)
        if results['status'] != 'success':
            logger.error(f'Result of ATOM + SARR dataverse deletion was not success, but {results["status"]}.')
            return
        self.log_results(results)
