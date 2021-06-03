import logging
import argparse
import datetime
import json
import abc

from src.couchbase.executor import AbstractCouchbaseRunnable

logger = logging.getLogger(__name__)


class AbstractTPCCHRunnable(AbstractCouchbaseRunnable, abc.ABC):
    def _collect_config(self, **kwargs):
        parser = argparse.ArgumentParser(description='Benchmark TPC_CH queries on a Couchbase instance.')
        parser.add_argument('--config', type=str, default='config/couchbase.json', help='Path to the config file.')
        parser.add_argument('--datagen', type=str, default='config/tpc_ch.json', help='Path to the datagen file.')
        parser_args = parser.parse_args()
        with open(parser_args.config) as config_file:
            config_json = json.load(config_file)
        with open(parser_args.datagen) as datagen_file:
            config_json['tpc_ch'] = json.load(datagen_file)

        config_json['resultsDir'] = 'out/' + datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '-' + \
            self.__class__.__name__ + '-C'

        return {**config_json, **kwargs}

    def __init__(self, **kwargs):
        super().__init__(**self._collect_config(**kwargs))
