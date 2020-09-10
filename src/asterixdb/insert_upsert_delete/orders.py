import __init__
import logging
import json

from src.asterixdb.insert_upsert_delete.executor import AbstractInsertUpsertDelete
from src.datagen.shopalot import AbstractOrdersDatagen

logger = logging.getLogger(__name__)


class OrdersInsertUpsertDelete(AbstractInsertUpsertDelete):
    def __init__(self):
        with open('config/shopalot.json') as config_file:
            config_json = json.load(config_file)  # Determine where to start our inserts.
            logger.info(f'Using the ShopALot config file: {config_json}')

        increment_size = AbstractInsertUpsertDelete.DATASET_INCREMENT_SIZE
        decrement_size = AbstractInsertUpsertDelete.DATASET_DECREMENT_SIZE
        dataset_size = config_json['orders']['idRange']['end']
        chunk_size = config_json['orders']['chunkSize']

        super().__init__(**{
            'insert_epoch': int((dataset_size * increment_size) / chunk_size),
            'upsert_epoch': int((dataset_size * increment_size) / chunk_size),
            'delete_epoch': int((dataset_size * decrement_size) / chunk_size),
            'datagen_class': OrdersInsertUpsertDelete,
            'dataset_size': dataset_size,
            'chunk_size': chunk_size,
            'index_names': ['ordersItemQtyIdx', 'ordersItemProductIdx'],
            'dataset_name': 'Orders',
            'primary_key': 'order_id'
        })


if __name__ == '__main__':
    OrdersInsertUpsertDelete()()
