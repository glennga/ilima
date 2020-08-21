import __init__
import argparse
import datetime
import logging
import json
import abc
import os
import subprocess
import requests
import time
import socket
import uuid
import natsort

logger = logging.getLogger(__name__)


def _call_subprocess_with_logger_output(command):
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


class _AbstractDBBenchmarkExecutor(abc.ABC):
    def __init__(self, results_dir, results_suffix, **kwargs):
        self.results_fp = open(results_dir + '/results.json', 'w')
        self.results_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.results_suffix = results_suffix
        self.kwargs = kwargs

        self.results_socket.connect((kwargs['analysisAsterixCluster']['nodeController']['address'],
                                     kwargs['analysisAsterixCluster']['feedSocketPort']))

    @staticmethod
    def _aggregate_by_group(benchmark_scripts):
        benchmarks_by_group = {}

        for benchmark_script in benchmark_scripts:
            with open(benchmark_script, 'r') as script_fp:
                group_line = script_fp.readline()
                group_line = group_line.split('GROUP:')[1].strip()

                if group_line not in benchmarks_by_group:
                    benchmarks_by_group[group_line] = []
                benchmarks_by_group[group_line].append(benchmark_script)

        return benchmarks_by_group

    def _log_result(self, result, **kwargs):
        result['queryExecuted'] = kwargs['query_executed']
        result['startTime'] = str(kwargs['start_time'])
        result['endTime'] = str(datetime.datetime.now())
        result['benchmarkGroup'] = kwargs['group_id']
        result['numberOfRuns'] = kwargs['number_of_runs']
        result['overviewFromScript'] = kwargs['overview_from_script']
        result['configJSON'] = self.kwargs

        # To the results file..
        logger.debug('Recording result to disk.')
        json.dump(result, self.results_fp)
        self.results_fp.write('\n')

        # To the analysis cluster. We must provide our own key.
        result['id'] = str(uuid.uuid4())
        logger.debug('Recording result to cluster through feed.')
        if not self.results_socket.sendall(json.dumps(result).encode('ascii')) is None:
            logger.warning('Analysis cluster did not accept record!')
        else:
            logger.info('Result successfully sent to cluster.')

    def _invoke_within_group(self, executor, group):
        with open(group[0], 'r') as script_fp:
            group_line = script_fp.readline()
            repeat_line = script_fp.readline()

        group_id = group_line.split('GROUP:')[1].strip()
        number_of_runs = int(repeat_line.split('REPEAT:')[1].strip()) + 1
        for run_number in range(number_of_runs):
            for benchmark_script in group:
                logger.info(f'Running {benchmark_script}, run number {run_number}.')
                with open(benchmark_script, 'r') as script_fp:

                    [script_fp.readline() for _ in range(2)]  # We do not send comments to our executor.
                    overview_from_script = script_fp.readline().split('OVERVIEW:')[1].strip()

                    # Execute the query.
                    time_issued = datetime.datetime.now()
                    query = script_fp.read()
                    result = executor(query, group_id)

                    # Only save the first 100 results (for those high selectivity queries).
                    if 'results' in result.keys():
                        result['results'] = result['results'][:100]
                    logger.debug('Result from execution: ' + str(result))
                    self._log_result(result, **{
                        'start_time': time_issued,
                        'group_id': group_id,
                        'number_of_runs': number_of_runs,
                        'overview_from_script': overview_from_script,
                        'query_executed': query[:1000]
                    })
                time.sleep(1)

    @abc.abstractmethod
    def _startup(self):
        pass

    @abc.abstractmethod
    def _finalize(self):
        pass

    @abc.abstractmethod
    def _invoke(self):
        pass

    def invoke(self):
        self._startup()
        self._invoke()
        self._finalize()

        self.results_fp.close()
        self.results_socket.close()


class _AsterixDBBenchmarkExecutor(_AbstractDBBenchmarkExecutor):
    def _generate_results_dir(self):
        os.mkdir(os.getcwd() + '/' + self.results_dir)
        logger.info(f'Results will be stored in: {self.results_dir}')

    def __init__(self, *args, **kwargs):
        self.node_controller_uri = kwargs['benchmark']['asterixDB']['nodeController']['address'] + ':' + \
                                   str(kwargs['benchmark']['asterixDB']['nodeController']['port'])
        self.node_controller_uri = 'http://' + self.node_controller_uri + '/query/service'

        results_suffix = datetime.datetime.now().strftime('A-%Y-%m-%d_%H-%M-%S')
        self.results_dir = args[1] + '/' + results_suffix
        self.benchmark_dir = args[0]
        self.kwargs = kwargs

        self._generate_results_dir()
        super().__init__(self.results_dir, results_suffix, **kwargs)

    def _startup(self):
        logger.info('Running STOP command.')
        _call_subprocess_with_logger_output(self.kwargs['benchmark']['asterixDB']['stopCommand'])
        logger.info('Running START command.')
        _call_subprocess_with_logger_output(self.kwargs['benchmark']['asterixDB']['startCommand'])
        logger.info('Waiting for cluster to start...')
        time.sleep(5)

    def _get_benchmark_scripts(self):
        scripts_to_exclude = set()
        for script in self.kwargs['benchmark']['asterixDB']['excludeScripts']:
            scripts_to_exclude.add(script)

        files_from_dir = natsort.natsorted(os.listdir(self.benchmark_dir))
        benchmark_scripts = []
        for dir_file in files_from_dir:
            if dir_file[-6:] == '.sqlpp' and dir_file not in scripts_to_exclude:
                benchmark_scripts.append(dir_file)

        logger.info(f'Excluding the scripts: [f{scripts_to_exclude}]')
        benchmark_scripts = [self.benchmark_dir + '/' + b for b in benchmark_scripts]
        logger.info(f'Benchmark scripts are: [{benchmark_scripts}].')

        return self._aggregate_by_group(benchmark_scripts)

    def _execute_benchmark(self, query, group_id):
        response = requests.post(self.node_controller_uri, {
            'statement': query,
            'client_context_id': str(group_id),
            'plan-format': 'STRING',
            'expression-tree': True,
            'rewritten-expression-tree': True,
            'logical-plan': True,
            'optimized-logical-plan': True,
            'job': True
        })
        return response.json()

    def _invoke(self):
        aggregated_scripts = self._get_benchmark_scripts()
        for _, group in aggregated_scripts.items():
            self._invoke_within_group(self._execute_benchmark, group)

        # Delegating the process of copying our config files + logs to a tool (external script).
        logger.info('Running copy-post command for copying config + log files.')
        _call_subprocess_with_logger_output(
            [self.kwargs['benchmark']['asterixDB']['copyPostCommand'], self.results_dir])

    def _finalize(self):
        logger.info('Running STOP command.')
        _call_subprocess_with_logger_output(self.kwargs['benchmark']['asterixDB']['stopCommand'])


def _benchmark_executor_factory(*args, **kwargs) -> _AbstractDBBenchmarkExecutor:
    selected_database = args[0]
    if selected_database == 'asterix':
        return _AsterixDBBenchmarkExecutor(*args[1:], **kwargs)

    else:
        e = NotImplementedError('Benchmarking not yet implemented for other databases!')
        logger.error(str(e))
        raise e


if __name__ == '__main__':
    logger.info('Running benchmark.py.')
    parser = argparse.ArgumentParser(description='Benchmark a collection of queries on a specified database platform.')
    parser.add_argument('benchmark', type=str, help='Path to the location of all benchmark scripts to execute.')
    parser.add_argument('--results', type=str, default='results', help='Path to the location of results directory.')
    parser.add_argument('--config', type=str, default='config.json', help='Path to the configuration file.')
    parser.add_argument('--database', type=str, default='asterix', choices=['asterix', 'mongo', 'couchbase'],
                        help='Which database to benchmark.')
    command_line_args = parser.parse_args()
    with open(command_line_args.config) as config_file:
        config_json = json.load(config_file)

    # In order of 'database', 'benchmark', 'results'.
    benchmark_args = [command_line_args.database, command_line_args.benchmark, command_line_args.results]
    benchmark_executor = _benchmark_executor_factory(*benchmark_args, **config_json)

    # noinspection PyBroadException
    try:
        benchmark_executor.invoke()
    except Exception as uncaught_exception:
        logger.error('Uncaught exception! ' + str(uncaught_exception))

    logger.info('Exiting benchmark.py.')
    os.system('say "benchmark.py has finished running."')
