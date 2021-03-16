import logging
import argparse
import json
import datetime
import abc

from src.mongodb.executor import AbstractMongoDBRunnable

logger = logging.getLogger(__name__)


class AbstractLowerBoundRunnable(AbstractMongoDBRunnable, abc.ABC):
    def _collect_config(self):
        parser = argparse.ArgumentParser(description='Benchmark lean CRUD on a MongoDB instance.')
        parser.add_argument('--config', type=str, default='config/mongodb.json', help='Path to the config file.')
        parser_args = parser.parse_args()
        with open(parser_args.config) as config_file:
            config_json = json.load(config_file)

        config_json['resultsDir'] = 'out/' + datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '-' + \
            self.__class__.__name__ + '-M'

        return config_json

    def __init__(self, **kwargs):
        super().__init__(**{**self._collect_config(), **kwargs})
