import logging.config
import datetime
import logging
import socket
import uuid
import os
import subprocess
import abc
import json


with open('config/logging.json') as logging_config_file:
    logging_json = json.load(logging_config_file)
    logging.config.dictConfig({k: v for k, v in logging_json.items() if k != 'analysisCluster'})
    logger = logging.getLogger(__name__)


class AbstractBenchmarkRunnable(abc.ABC):
    @staticmethod
    def call_subprocess(command, is_log=True):
        subprocess_pipe = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True
        )

        resultant = ''
        for stdout_line in iter(subprocess_pipe.stdout.readline, ""):
            if stdout_line.strip() != '':
                resultant += stdout_line
                for line in resultant.strip().split('\n'):
                    if is_log:
                        logger.debug(line)
        subprocess_pipe.stdout.close()
        return resultant

    def __init__(self, **kwargs):
        self.config = {**logging_json, **kwargs}
        logger.info(f'Using the following configuration: {self.config}')
        self.execution_id = str(uuid.uuid4())

        # Setup our benchmarking outputs (to analysis cluster, to file).
        if self.config['results']['isFile']:
            os.mkdir(os.getcwd() + '/' + self.config['resultsDir'])
            logger.info(f'Results will be stored in: {self.config["resultsDir"]}')
            self.results_fp = open(self.config['resultsDir'] + '/' + 'results.json', 'w')
        if self.config['results']['isSocket']:
            self.results_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.results_socket.connect((self.config['analysisCluster']['clusterController']['address'],
                                         self.config['analysisCluster']['feedSocketPort']))

    def log_results(self, results):
        results['logTime'] = str(datetime.datetime.now())
        results['executionID'] = self.execution_id
        results['configJSON'] = self.config

        # To the results file.
        if self.config['results']['isFile']:
            logger.debug('Recording result to disk.')
            json.dump(results, self.results_fp)
            self.results_fp.write('\n')

        # To the analysis cluster. We must provide our own key.
        if self.config['results']['isSocket']:
            results['id'] = str(uuid.uuid4())
            logger.debug('Recording result to cluster through feed.')
            if not self.results_socket.sendall(json.dumps(results).encode('ascii')) is None:
                logger.warning('Analysis cluster did not accept record!')
            else:
                logger.info('Result successfully sent to cluster.')

        # To the console.
        if self.config['results']['isConsole']:
            logger.debug('Writing result to console.')
            logger.debug(json.dumps(results))

    def restart_db(self):
        logger.info('Running STOP command.')
        self.call_subprocess(self.config['benchmark']['stopCommand'])
        logger.info('Running START command.')
        self.call_subprocess(self.config['benchmark']['startCommand'])
        logger.info('Waiting for database to start...')

    @abc.abstractmethod
    def perform_benchmark(self):
        pass

    def perform_post(self):
        pass

    def invoke(self):
        logger.info(f'Working with execution id: {self.execution_id}.')

        # Restart the cluster. For queries, this minimizes the chance that we access a cached page.
        self.restart_db()

        # Perform the benchmark.
        logger.debug('Executing the benchmark.')
        self.perform_benchmark()

        # Populate our results directory.
        logger.info('Running finalize command for copying config + log files.')
        [h.flush() for h in logger.handlers]
        self.call_subprocess([self.config['benchmark']['postCommand'], self.config['resultsDir']])

        # Perform any post action.
        self.perform_post()

        if self.config['results']['isFile']:
            self.results_fp.close()
        if self.config['results']['isSocket']:
            self.results_socket.close()
        logger.info('Benchmark has finished executing.')
