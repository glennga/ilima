import __init__
import datetime
import argparse
import logging
import json
import socket
import uuid
import os
import subprocess
import time
import requests
import abc

logger = logging.getLogger(__name__)


class AbstractBenchmarkRunnable(abc.ABC):
    SARR_DATAVERSE = 'sarr'
    ATOM_DATAVERSE = 'atom'

    @staticmethod
    def _collect_config():
        parser = argparse.ArgumentParser(description='Benchmark a collection of queries on an AsterixDB instance.')
        parser.add_argument('dataverse', type=str, choices=['atom', 'sarr'], help='Dataverse to benchmark.')
        parser.add_argument('--config', type=str, default='config/asterixdb.json', help='Path to the config file.')
        parser.add_argument('--shopalot', type=str, default='config/shopalot.json', help='Path to the datagen file.')
        parser_args = parser.parse_args()
        with open(parser_args.config) as config_file:
            config_json = json.load(config_file)

        config_json['shopalot'] = parser_args.shopalot
        config_json['dataverse'] = parser_args.dataverse
        return config_json

    @staticmethod
    def _call_subprocess(command):
        subprocess_pipe = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True
        )
        for stdout_line in iter(subprocess_pipe.stdout.readline, ""):
            if stdout_line.strip() != '':
                logger.debug(stdout_line.strip())
        subprocess_pipe.stdout.close()

    def _generate_results_dir(self):
        results_dir = 'out/' + datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '-' + \
                      self.__class__.__name__ + '-' + self.dataverse.upper() + '-A'
        os.mkdir(os.getcwd() + '/' + results_dir)
        logger.info(f'Results will be stored in: {results_dir}')

        return results_dir

    def __init__(self, **kwargs):
        self.config = {**self._collect_config(), **kwargs}
        logger.info(f'Using the following configuration: {self.config}')

        self.nc_uri = self.config['benchmark']['nodeController']['address'] + ':' + \
                      str(self.config['benchmark']['nodeController']['port'])
        self.nc_uri = 'http://' + self.nc_uri + '/query/service'
        self.execution_id = str(uuid.uuid4())
        self.dataverse = self.config['dataverse']

        # Setup our benchmarking outputs (to analysis cluster, to file).
        self.results_dir = self._generate_results_dir()
        self.results_fp = open(self.results_dir + '/' + 'results.json', 'w')
        self.results_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.results_socket.connect((self.config['analysisCluster']['nodeController']['address'],
                                     self.config['analysisCluster']['feedSocketPort']))

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
        results['logTime'] = str(datetime.datetime.now())
        results['executionID'] = self.execution_id
        results['dataverse'] = self.dataverse
        results['configJSON'] = self.config

        # To the results file..
        logger.debug('Recording result to disk.')
        json.dump(results, self.results_fp)
        self.results_fp.write('\n')

        # To the analysis cluster. We must provide our own key.
        results['id'] = str(uuid.uuid4())
        logger.debug('Recording result to cluster through feed.')
        if not self.results_socket.sendall(json.dumps(results).encode('ascii')) is None:
            logger.warning('Analysis cluster did not accept record!')
        else:
            logger.info('Result successfully sent to cluster.')

    def execute_sqlpp(self, statement):
        lean_statement = ' '.join(statement.split())
        query_parameters = {
            'statement': lean_statement,
            'plan-format': 'STRING',
            'profile': 'timings',
            'rewritten-expression-tree': True,
            'optimized-logical-plan': True,
            'expression-tree': True,
            'logical-plan': True,
            'job': True
        }

        response_json = requests.post(self.nc_uri, query_parameters).json()
        if response_json['status'] != 'success':
            logger.warning(f'Status of executing statement {statement} not successful, '
                           f'but instead {response_json["status"]}.')
            logger.warning(f'JSON dump: {response_json}')

        # We get our job as a JSON string, but it would be beneficial to store this as an object.
        if 'plans' in response_json and 'job' in response_json['plans']:
            response_json['plans']['job'] = json.loads(response_json['plans']['job'])

        # Add the query to response.
        response_json['statement'] = lean_statement
        return response_json

    @abc.abstractmethod
    def perform_benchmark(self):
        pass

    def perform_post(self):
        pass

    def invoke(self):
        logger.info(f'Working with execution id: {self.execution_id}.')
        logger.info(f'Specified dataverse is: {self.dataverse}.')

        # Restart the cluster. For queries, this minimizes the chance that we access a cached page.
        logger.info('Running STOP command.')
        self._call_subprocess(self.config['benchmark']['stopCommand'])
        logger.info('Running START command.')
        self._call_subprocess(self.config['benchmark']['startCommand'])
        logger.info('Waiting for cluster to start...')
        while True:
            time.sleep(5)
            try:
                starting_response_json = requests.post(self.nc_uri, {
                    'statement': "SELECT 1;",
                    'client_context_id': str(uuid.uuid4()),
                }).json()
                if starting_response_json['status'] == 'success':
                    break
                else:
                    logger.info(f'Status from initial connection to cluster not success, '
                                f'but {starting_response_json["status"]}. Trying again in 5 seconds...')
                    logger.info(f'JSON dump: {starting_response_json}')

            except requests.exceptions.ConnectionError:
                logger.info('Connection refused, trying again in 5 seconds...')

        # Perform the benchmark.
        logger.debug('Executing the benchmark.')
        self.perform_benchmark()

        # Populate our results directory.
        logger.info('Running finalize command for copying config + log files.')
        [h.flush() for h in logger.handlers]
        self._call_subprocess([self.config['benchmark']['postCommand'], self.results_dir])

        self.perform_post()
        self.results_fp.close()
        self.results_socket.close()
        logger.info('Benchmark has finished executing.')

        # Scare me in the middle of the night :-) OSX specific.
        os.system('say "Benchmark has finished executing." -v Samantha')
