import __init__
import logging

from src.asterixdb.insert_upsert_delete.executor import AbstractInsertUpsertDelete
from src.datagen.shopalot_atom import generate_stores as atom_generate_stores
from src.datagen.shopalot_sarr import generate_stores as sarr_generate_stores

logger = logging.getLogger(__name__)


class StoresInsertUpsertDelete(AbstractInsertUpsertDelete):
    def __init__(self):
        super().__init__(index_name='storesCatIdx', dataset_name='Stores', primary_key='store_id')

    def generate_atom_dataset(self, insert_handler, starting_id, ending_id):
        return atom_generate_stores(insert_handler, self.faker_dategen, starting_id, ending_id)

    def generate_sarr_dataset(self, insert_handler, starting_id, ending_id):
        return sarr_generate_stores(insert_handler, self.faker_dategen, starting_id, ending_id)


if __name__ == '__main__':
    StoresInsertUpsertDelete()
