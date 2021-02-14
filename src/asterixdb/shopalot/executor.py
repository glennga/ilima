import __init__
import logging
import argparse
import datetime
import json
import abc

from src.asterixdb.executor import AbstractAsterixDBRunnable

logger = logging.getLogger(__name__)


class AbstractShopALotRunnable(AbstractAsterixDBRunnable, abc.ABC):
    SARR_DATAVERSE = 'sarr'
    ATOM_DATAVERSE = 'atom'

    def _collect_config(self):
        parser = argparse.ArgumentParser(description='Benchmark ShopALot CRUD on an AsterixDB instance.')
        parser.add_argument('--config', type=str, default='config/asterixdb.json', help='Path to the config file.')
        parser.add_argument('--datagen', type=str, default='config/shopalot.json', help='Path to the datagen file.')
        parser_args = parser.parse_args()
        with open(parser_args.config) as config_file:
            config_json = json.load(config_file)

        config_json['shopalot'] = parser_args.datagen
        config_json['dataverse'] = parser_args.dataverse
        config_json['resultsDir'] = 'out/' + datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '-' + \
            self.__class__.__name__ + '-' + self.dataverse.upper() + '-A'

        return config_json

    def __init__(self, **kwargs):
        super().__init__(**{**self._collect_config(), **kwargs})
        self.dataverse = self.config['dataverse']

    def do_indexes_exist(self, index_names, dataset_name):
        for index_name in index_names:
            if self.dataverse == self.ATOM_DATAVERSE:
                logger.info(f'Checking that the index "{index_name}" exists on ATOM.')
                results = self.execute_sqlpp(f"""
                    SELECT *
                    FROM `Metadata`.`Index`
                    WHERE IndexName = "{index_name}" AND 
                          DataverseName = "ShopALot.ATOM" AND 
                          DatasetName = "{dataset_name}";
                """)
                if len(results['results']) == 0:
                    logger.error(f'Index "{index_name}" on ATOM does not exist.')
                    return False

            else:
                logger.info(f'Checking that the index "{index_name}" exists on SARR.')
                results = self.execute_sqlpp(f"""
                    SELECT *
                    FROM `Metadata`.`Index`
                    WHERE IndexName = "{index_name}" AND 
                          DataverseName = "ShopALot.SARR" AND 
                          DatasetName = "{dataset_name}";
                """)
                if len(results['results']) == 0:
                    logger.error(f'Index "{index_name}" on SARR does not exist.')
                    return False

        return True

    def log_results(self, results):
        results['dataverse'] = self.dataverse
        super(AbstractShopALotRunnable, self).log_results(results)

    def invoke(self):
        logger.info(f'Specified dataverse is: {self.dataverse}.')
        super(AbstractShopALotRunnable, self).invoke()
