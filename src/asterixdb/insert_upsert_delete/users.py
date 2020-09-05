import __init__
import logging

from src.asterixdb.insert_upsert_delete.executor import AbstractInsertUpsertDelete
from src.datagen.shopalot_atom import generate_users as atom_generate_users
from src.datagen.shopalot_sarr import generate_users as sarr_generate_users

logger = logging.getLogger(__name__)


class UsersInsertUpsertDelete(AbstractInsertUpsertDelete):
    def __init__(self):
        super().__init__(index_name='usersNumberIdx', dataset_name='Users', primary_key='user_id')

    def generate_atom_dataset(self, insert_handler, starting_id, ending_id):
        return atom_generate_users(insert_handler, self.faker_dategen, starting_id, ending_id)

    def generate_sarr_dataset(self, insert_handler, starting_id, ending_id):
        return sarr_generate_users(insert_handler, self.faker_dategen, starting_id, ending_id)


if __name__ == '__main__':
    UsersInsertUpsertDelete()
