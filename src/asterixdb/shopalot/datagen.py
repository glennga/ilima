import datetime
import decimal
import random
import faker
import json
import argparse
import abc


class PrimaryKeyGeneratorFactory:
    @staticmethod
    def provide_range_generator(start_id, end_id):
        def _range_based_pk_generator():
            working_primary_key = start_id
            while working_primary_key < end_id:
                yield working_primary_key
                working_primary_key = working_primary_key + 1

        return _range_based_pk_generator

    @staticmethod
    def provide_rand_range_generator(start_id, end_id, dataset_size):
        # Note this is all in memory! No large ranges!
        def _random_range_based_pk_generator():
            for pk in random.sample(range(start_id, end_id), dataset_size):
                yield pk

        return _random_range_based_pk_generator


class _AbstractShopALotDatagen(abc.ABC):
    def __init__(self, **kwargs):
        self.faker_datagen = faker.Faker()
        self.primary_key_generator = kwargs['primary_key_generator']
        self.dataset_size = kwargs['dataset_size']
        self.chunk_size = kwargs['chunk_size']
        self.primary_key = kwargs['primary_key']
        self.pk_zfill = kwargs['pk_zfill']

    def format_key(self, text):
        return str(text).zfill(self.pk_zfill)

    @classmethod
    def chunk_id_mapper(cls, pk, chunk_size):
        return pk % chunk_size

    @abc.abstractmethod
    def atom_mapper(self, pk):
        pass

    @abc.abstractmethod
    def sarr_mapper(self, atom_json):
        pass

    @abc.abstractmethod
    def json_consumer(self, atom_json, sarr_json):
        pass

    def close_resources(self):
        pass

    def invoke(self):
        primary_key_iter = self.primary_key_generator()
        for pk in primary_key_iter:
            # We ensure that one PK always maps to a a single, unique record.
            random.seed(pk)
            faker.Faker.seed(pk)

            # First, create the ATOM JSON.
            atom_json = self.atom_mapper(pk)

            # Next create the SARR JSON from the ATOM JSON.
            sarr_json = self.sarr_mapper(atom_json)

            # Finally, consume the JSON.
            self.json_consumer(atom_json, sarr_json)

        self.close_resources()


class AbstractUsersDatagen(_AbstractShopALotDatagen, abc.ABC):
    PHONE_TYPES = ['HOME', 'OFFICE', 'MOBILE']

    def __init__(self, **kwargs):
        super().__init__(primary_key='user_id', **kwargs)

    def atom_mapper(self, pk):
        user_phone = {
            "type": random.choice(self.PHONE_TYPES),
            "number": self.faker_datagen.phone_number()
        }
        return {
            "user_id": self.format_key(pk),
            "chunk_id": self.format_key(self.chunk_id_mapper(pk, self.chunk_size)),
            "name": {
                "first": self.faker_datagen.first_name(),
                "last": self.faker_datagen.last_name()
            },
            "phone": user_phone
        }

    def sarr_mapper(self, atom_json):
        sarr_json = dict(atom_json)

        sarr_json['phones'] = [atom_json['phone']]
        del sarr_json['phone']
        return sarr_json


class AbstractStoresDatagen(_AbstractShopALotDatagen, abc.ABC):
    STORE_NAMES = [
        "Machias Mainway", "Casey's Cove Convenience Store", "Maay Convenient Inc", "Cefco", "Mapco Express",
        "Plaid Pantry", "Jackson Food Store", "Rutters Farm Store", "Irving Oil Corp", "Hillcrest", "Cubbys",
        "7-Eleven", "Service Champ", "Express Mart Stores", "Baum's Mercantile", "Border Station", "Country Market",
        "Golden Spike Travel Plaza", "Wesco Oil CO", "Cracker Barrel Stores Inc", "Elnemr Enterprises Inc", "Sheetz",
        "Pump-N-Pantry Of NY", "Spaceway Oil CO", "6-Twelve Convenient-Mart Inc", "Dandy Mini Mart", "Stripes Llc",
        "Plaid Pantries Inc", "Quick Chek Food Stores", "Victory Marketing Llc", "Beasley Enterprises Inc",
        "Lil' Champ", "Pit Row", "Popeye Shell Superstop", "Mapco", "Sunset Foods", "Simonson Market", "Fasmart",
        "Super Quik Inc", "Jim's Quick Stop"
    ]

    PRODUCT_CATEGORIES = [
        'Baby Care', 'Beverages', 'Bread & Bakery', 'Breakfast & Cereal', 'Canned Goods & Soups',
        'Condiments, Spice, & Bake', 'Cookies, Snacks, & Candy', 'Dairy, Eggs, & Cheese', 'Deli', 'Frozen Foods',
        'Fruits & Vegetables', 'Grains, Pasta, & Sides', 'Meat & Seafood', 'Paper, Cleaning, & Home',
        'Personal Care & Health', 'Pet Care'
    ]

    def __init__(self, **kwargs):
        super().__init__(primary_key='store_id', **kwargs)

    def atom_mapper(self, pk):
        store_name = random.choice(self.STORE_NAMES)
        store_category = random.choice(self.PRODUCT_CATEGORIES)
        return {
            "store_id": self.format_key(pk),
            "chunk_id": self.format_key(self.chunk_id_mapper(pk, self.chunk_size)),
            "name": store_name,
            "address": {
                "city": self.faker_datagen.city(),
                "street": self.faker_datagen.street_address(),
                "zip_code": self.faker_datagen.postcode()[:5].lstrip('0'),
            },
            "phone": self.faker_datagen.phone_number(),
            "category": store_category
        }

    def sarr_mapper(self, atom_json):
        sarr_json = dict(atom_json)

        sarr_json['categories'] = [atom_json['category']]
        del sarr_json['category']
        return sarr_json


class AbstractOrdersDatagen(_AbstractShopALotDatagen, abc.ABC):
    @staticmethod
    def _product_generator():
        while True:
            yield {
                "product_id": str(random.randint(0, 541)).zfill(3),
                "list_price": max(random.random() * 50, 0.99)
            }

    def __init__(self, **kwargs):
        self.user_id_start = kwargs['user_start_id']
        self.user_id_end = kwargs['user_end_id']
        self.user_id_zfill = len(str(self.user_id_end))

        self.store_id_start = kwargs['store_start_id']
        self.store_id_end = kwargs['store_end_id']
        self.store_id_zfill = len(str(self.store_id_end))

        self.product_iter = self._product_generator()
        super().__init__(primary_key='order_id', **kwargs)

    def atom_mapper(self, pk):
        product = next(self.product_iter)
        price = max(float(product['list_price']) + (float(product['list_price']) * random.random()) -
                    (float(product['list_price']) / 2.0), 0.99)
        order_item = {
            "item_id": self.faker_datagen.uuid4(),
            "qty": int(abs(random.normalvariate(1, 10))),
            "price": float(decimal.Decimal(price).quantize(decimal.Decimal('0.01'), decimal.ROUND_HALF_UP)),
            "product_id": product['product_id']
        }

        time_placed = self.faker_datagen.date_time_this_month()
        pickup_time = self.faker_datagen.date_time_between_dates(
            datetime_start=time_placed, datetime_end=time_placed + datetime.timedelta(hours=6))
        date_fulfilled_field = str(self.faker_datagen.date_time_between_dates(
            datetime_start=pickup_time, datetime_end=pickup_time + datetime.timedelta(hours=6)))
        user_id = str(random.randint(self.user_id_start, self.user_id_end)).zfill(self.user_id_zfill)
        store_id = str(random.randint(self.store_id_start, self.store_id_end)).zfill(self.store_id_zfill)

        return {
            "order_id": self.format_key(pk),
            "chunk_id": self.format_key(self.chunk_id_mapper(pk, self.chunk_size)),
            "time_placed": str(time_placed),
            "time_fulfilled": str(date_fulfilled_field),
            "user_id": user_id,
            "store_id": store_id,
            "item": order_item
        }

    def sarr_mapper(self, atom_json):
        sarr_json = dict(atom_json)

        sarr_json['items'] = [atom_json['item']]
        del sarr_json['item']
        return sarr_json


class DatagenAbstractFactoryProvider:
    @staticmethod
    def provide_memory_abstract_factory(datagen_class: _AbstractShopALotDatagen):
        class _ForMemoryDatagen(datagen_class):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self.atom_json, self.sarr_json = [], []

            def json_consumer(self, atom_json, sarr_json):
                self.atom_json.append(atom_json)
                self.sarr_json.append(sarr_json)

            def reset_generation(self, primary_key_generator):
                self.primary_key_generator = primary_key_generator
                self.atom_json = []
                self.sarr_json = []

        return _ForMemoryDatagen

    @staticmethod
    def provide_disk_abstract_factory(datagen_class: _AbstractShopALotDatagen):
        class _ToFileDatagen(datagen_class):
            def __init__(self, **kwargs):
                config = {
                    'chunk_size': kwargs['chunkSize'],
                    'dataset_size': kwargs['idRange']['end'] - kwargs['idRange']['start'],
                    'pk_zfill': len(str(kwargs['idRange']['end'])),
                    'user_start_id': main_config_json['users']['idRange']['start'],
                    'user_end_id': main_config_json['users']['idRange']['end'],
                    'store_start_id': main_config_json['stores']['idRange']['start'],
                    'store_end_id': main_config_json['stores']['idRange']['end']
                }
                primary_key_generator = PrimaryKeyGeneratorFactory.\
                    provide_range_generator(kwargs['idRange']['start'], kwargs['idRange']['end'])
                super().__init__(primary_key_generator=primary_key_generator, **config)

                self.atom_fps = {
                    'full': open(kwargs['atomDataverse']['fullFilename'], 'w'),
                    'eighth': open(kwargs['atomDataverse']['eighthFilename'], 'w')
                }
                self.sarr_fps = {
                    'full': open(kwargs['sarrDataverse']['fullFilename'], 'w'),
                    'eighth': open(kwargs['sarrDataverse']['eighthFilename'], 'w')
                }

                self.datagen_counter = kwargs['idRange']['start']

            @staticmethod
            def _write_to_file(out_fp, out_json):
                json.dump(out_json, out_fp)
                out_fp.write('\n')

            def json_consumer(self, atom_json, sarr_json):
                if self.datagen_counter % 8 == 0:
                    self._write_to_file(self.atom_fps['eighth'], atom_json)
                    self._write_to_file(self.sarr_fps['eighth'], sarr_json)

                self._write_to_file(self.atom_fps['full'], atom_json)
                self._write_to_file(self.sarr_fps['full'], sarr_json)
                self.datagen_counter = self.datagen_counter + 1

            def close_resources(self):
                [fp.close() for fp in self.atom_fps.values()]
                [fp.close() for fp in self.sarr_fps.values()]

        return _ToFileDatagen


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate ShopALot JSON data.')
    parser.add_argument('dataset', type=str, choices=['user', 'store', 'order'], help='Which dataset to generate.')
    parser.add_argument('--config', type=str, default='config/shopalot.json', help='Path to the config file.')
    command_line_args = parser.parse_args()
    with open(command_line_args.config) as config_file:
        main_config_json = json.load(config_file)

    # Determine our parent class, based on the given dataset.
    if command_line_args.dataset == 'user':
        datagen_parent = AbstractUsersDatagen
    elif command_line_args.dataset == 'store':
        datagen_parent = AbstractStoresDatagen
    else:
        datagen_parent = AbstractOrdersDatagen

    # Invoke our datagen.
    datagen_factory = DatagenAbstractFactoryProvider.provide_disk_abstract_factory(datagen_parent)
    datagen_instance = datagen_factory(**(main_config_json[command_line_args.dataset + 's']))
    datagen_instance.invoke()
