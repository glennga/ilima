import logging
import argparse
import json
import datetime
import abc

from src.couchbase.executor import AbstractCouchbaseRunnable

logger = logging.getLogger(__name__)


class AbstractLowerBoundRunnable(AbstractCouchbaseRunnable, abc.ABC):
    def _collect_config(self):
        parser = argparse.ArgumentParser(description='Benchmark lean CRUD on an Couchbase instance.')
        parser.add_argument('--config', type=str, default='config/couchbase.json', help='Path to the config file.')
        parser_args = parser.parse_args()
        with open(parser_args.config) as config_file:
            config_json = json.load(config_file)

        config_json['resultsDir'] = 'out/' + datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '-' + \
            self.__class__.__name__ + '-C'

        return config_json

    def __init__(self, **kwargs):
        super().__init__(**{**self._collect_config(), **kwargs})
