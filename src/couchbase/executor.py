import logging
import abc
import time
import timeit
import datetime

from couchbase.cluster import Cluster, ClusterOptions
from couchbase.diagnostics import ClusterState
from couchbase.exceptions import BucketDoesNotExistException, ProtocolException
from couchbase.management.buckets import CreateBucketSettings, BucketType
from couchbase_core.cluster import PasswordAuthenticator
from src.executor import AbstractBenchmarkRunnable

logger = logging.getLogger(__name__)


class AbstractCouchbaseRunnable(AbstractBenchmarkRunnable, abc.ABC):
    def __init__(self, **kwargs):
        super().__init__(working_system='Couchbase', **kwargs)
        self.cluster_uri = 'couchbase://' + self.config['cluster']['address']
        self.bucket_name = self.config['cluster']['bucket']

        self.cluster = Cluster.connect(self.cluster_uri, ClusterOptions(PasswordAuthenticator(
            username=self.config['username'],
            password=self.config['password']
        )))
        self.bucket, self.collection = None, None

    def execute_cbimport(self, file_path, collection='_default', key="key::#UUID#"):
        cbimport_command = self.config['loadCommand'][:-1] + [
            self.config['loadCommand'][-1] + ' ' + ' '.join(f"""
                --cluster localhost
                --username "{self.config['username']}"
                --password "{self.config['password']}"
                --bucket "{self.bucket_name}"
                --dataset {file_path}
                --format lines
                --generate-key {key}
                --scope-collection-exp _default.{collection}
                # --verbose
            """.split())
        ]
        return {'response': self.call_subprocess(cbimport_command)}

    def execute_n1ql(self, statement, timeout=None):
        query_parameters = {} if timeout is None else {'timeout': datetime.timedelta(seconds=timeout)}
        lean_statement = ' '.join(statement.split())
        try:
            t_before = timeit.default_timer()
            response_iterable = self.cluster.query(lean_statement, **query_parameters)
            client_time = timeit.default_timer() - t_before
            response_json = {'statement': lean_statement, 'results': [], 'clientTime': client_time}
            response_json = {**response_json, **response_iterable.meta}
            for record in response_iterable:
                response_json['results'].append(record)

        except Exception as e:
            logger.warning(f'Status of executing statement {statement} not successful, but instead {e}.')
            response_json = {'statement': lean_statement, 'results': [], 'error': str(e)}

        return response_json

    def connect_bucket(self, max_retries=5):
        for r in range(max_retries):
            try:
                logger.info('Attempting to connect to our bucket.')
                self.bucket = self.cluster.bucket(self.bucket_name)
                self.collection = self.bucket.default_collection()
                return
            except ProtocolException as e:
                if r == max_retries - 1:
                    raise e

                logger.warning(f'Could not connect to bucket. Retrying in 3 seconds.')
                time.sleep(3)

    def initialize_bucket(self):
        self.remove_bucket()  # Start from a clean bucket.

        bucket_manager = self.cluster.buckets()
        logger.info(f'Creating bucket {self.bucket_name}.')
        bucket_manager.create_bucket(CreateBucketSettings(
            name=self.bucket_name,
            ram_quota_mb=self.config['cluster']['ramsize'],
            bucket_type=BucketType.COUCHBASE
        ))
        time.sleep(10)
        self.connect_bucket()

        response = self.execute_n1ql(f"CREATE PRIMARY INDEX naupakaPrimaryIdx ON `{self.bucket_name}` USING GSI;")
        if 'error' in response:
            logger.error('Could not build primary index!')
            raise RuntimeError('Could not build primary index!')
        else:
            while True:
                logger.info('Verifying that our primary index is online.')
                response = self.execute_n1ql(f"SELECT * FROM `{self.bucket_name}`;")
                if 'error' in response:
                    logger.warning(f'Could not issue SELECT query. Retrying in 3 seconds.')
                    time.sleep(3)
                else:
                    break

    def remove_bucket(self):
        bucket_manager = self.cluster.buckets()
        try:
            logger.info(f'Dropping bucket {self.bucket_name}.')
            bucket_manager.drop_bucket(self.bucket_name)

        except BucketDoesNotExistException:
            logger.info(f'Bucket not found... Swallowing exception.')
            pass

    def restart_db(self):
        super(AbstractCouchbaseRunnable, self).restart_db()
        while True:
            time.sleep(10)
            diagnostics = self.cluster.diagnostics()
            if diagnostics.state != ClusterState.Online:
                logger.warning(f'Cluster not currently online, but rather {diagnostics.state}. '
                               f'Trying again in 10 seconds...')

            else:
                break
