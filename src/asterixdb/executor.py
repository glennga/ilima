import logging
import json
import abc
import time
import requests
import uuid
import timeit

from src.executor import AbstractBenchmarkRunnable

logger = logging.getLogger(__name__)


class AbstractAsterixDBRunnable(AbstractBenchmarkRunnable, abc.ABC):
    def __init__(self, **kwargs):
        super().__init__(working_system='AsterixDB', **kwargs)
        self.nc_uri = self.config['benchmark']['clusterController']['address'] + ':' + \
            str(self.config['benchmark']['clusterController']['port'])
        self.nc_uri = 'http://' + self.nc_uri + '/query/service'

    def execute_sqlpp(self, statement, timeout=None):
        lean_statement = ' '.join(statement.split())
        query_parameters = {
            'statement': lean_statement,
            'plan-format': 'STRING',
            'optimized-logical-plan': True,
            'logical-plan': True,
            'job': True
        }

        # Retry the query until success.
        while True:
            try:
                t_before = timeit.default_timer()
                response_json = requests.post(self.nc_uri, query_parameters, timeout=timeout).json()
                response_json['clientTime'] = timeit.default_timer() - t_before
                break
            except requests.exceptions.RequestException as e:
                if timeout is not None and isinstance(e, requests.exceptions.ReadTimeout):
                    logger.warning(f'Statement {statement} has run longer than the specified timeout {timeout}.')
                    response_json = {'status': f'Timeout. Exception: {str(e)}'}
                    break
                else:
                    logger.warning(f'Exception caught: {str(e)}. Restarting the query in 5 seconds...')
                    time.sleep(5)

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

    def restart_db(self):
        super(AbstractAsterixDBRunnable, self).restart_db()
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
