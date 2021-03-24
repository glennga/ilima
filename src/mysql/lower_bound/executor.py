import logging
import argparse
import json
import datetime
import abc

from src.mysql.executor import AbstractMySQLRunnable

logger = logging.getLogger(__name__)


class AbstractLowerBoundRunnable(AbstractMySQLRunnable, abc.ABC):
    def _collect_config(self):
        parser = argparse.ArgumentParser(description='Benchmark lean document-based CRUD on a MySQL instance.')
        parser.add_argument('--config', type=str, default='config/mysql.json', help='Path to the config file.')
        parser_args = parser.parse_args()
        with open(parser_args.config) as config_file:
            config_json = json.load(config_file)

        config_json['resultsDir'] = 'out/' + datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '-' + \
            self.__class__.__name__ + '-Y'

        return config_json

    def __init__(self, **kwargs):
        super().__init__(**{**self._collect_config(), **kwargs})
