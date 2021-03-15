import logging
import uuid

from src.couchbase.lower_bound.executor import AbstractLowerBoundRunnable

logger = logging.getLogger(__name__)


class LowerBoundUpsert(AbstractLowerBoundRunnable):
    NUMBER_OF_REPEATS = 1000

    def perform_benchmark(self):
        logger.info('Building bucket.')
        self.initialize_bucket()

        logger.info('Initialize the buffer.')
        self.execute_n1ql(f"""
            INSERT INTO `{self.bucket_name}` (KEY, VALUE)
            VALUES ("{str(uuid.uuid4())}", {{ "type": "TestCollectionBuffer", "a": 1 }});
        """)

        logger.info('Now executing the lower bound statement.')
        for i in range(self.NUMBER_OF_REPEATS):
            logger.debug(f'Executing run {i + 1} for the lower bound statement.')
            results = self.execute_n1ql(f"""
                UPSERT INTO `{self.bucket_name}` (KEY UUID(), VALUE _document)
                SELECT _document
                FROM `{self.bucket_name}`
                WHERE `type` = "TestCollectionBuffer";
            """)
            results['runNumber'] = i + 1
            self.log_results(results)

    def perform_post(self):
        logger.info('Removing bucket.')
        self.remove_bucket()


if __name__ == '__main__':
    LowerBoundUpsert().invoke()
