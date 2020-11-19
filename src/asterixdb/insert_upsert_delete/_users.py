import __init__
import logging
import json

from src.asterixdb.insert_upsert_delete.executor import AbstractInsertUpsertDelete
from src.datagen.shopalot import AbstractUsersDatagen

logger = logging.getLogger(__name__)


class UsersInsertUpsertDelete(AbstractInsertUpsertDelete):
    def __init__(self):
        with open('config/shopalot.json') as config_file:
            config_json = json.load(config_file)  # Determine where to start our inserts.
            logger.info(f'Using the ShopALot config file: {config_json}')

        increment_size = AbstractInsertUpsertDelete.DATASET_INCREMENT_SIZE
        decrement_size = AbstractInsertUpsertDelete.DATASET_DECREMENT_SIZE
        dataset_size = config_json['users']['idRange']['end']
        chunk_size = config_json['users']['chunkSize']

        super().__init__(**{
            'insert_epoch': max(int((dataset_size * increment_size) / chunk_size), 1),
            'upsert_epoch': max(int((dataset_size * increment_size) / chunk_size), 1),
            'delete_epoch': max(int((dataset_size * decrement_size) / chunk_size), 1),
            'datagen_class': AbstractUsersDatagen,
            'dataset_size': dataset_size,
            'chunk_size': chunk_size,
            'index_names': ['usersNumberIdx'],
            'dataset_name': 'Users',
            'primary_key': 'user_id'
        })


if __name__ == '__main__':
    UsersInsertUpsertDelete().invoke()
