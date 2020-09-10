import datetime
import decimal
import random
import faker
import json
import numpy
import argparse
import abc


class _AbstractShopALotDatagen(abc.ABC):
    def __init__(self, **kwargs):
        random_seed = datetime.datetime.now()
        faker.Faker.seed(random_seed)
        random.seed(random_seed)
        self.faker_datagen = faker.Faker()

        self.start_id = kwargs['start_id']
        self.end_id = kwargs['end_id']
        self.chunk_size = kwargs['chunk_size']
        self.primary_key = kwargs['primary_key']
        self.pk_zfill = len(str(self.end_id))

    @abc.abstractmethod
    def atom_generator(self):
        pass

    @abc.abstractmethod
    def sarr_mapper(self, atom_json):
        pass

    @abc.abstractmethod
    def json_consumer(self, atom_json, sarr_json):
        pass

    def close_resources(self):
        pass

    def _chunk_id_generator(self):
        working_chunk_id = self.start_id
        while True:
            yield str(working_chunk_id).zfill(self.pk_zfill)
            if working_chunk_id < self.chunk_size - 1:
                working_chunk_id = working_chunk_id + 1
            else:
                working_chunk_id = self.start_id

    def _primary_key_generator(self):
        working_primary_key = self.start_id
        while working_primary_key < self.end_id:
            yield str(working_primary_key).zfill(self.pk_zfill)
            working_primary_key = working_primary_key + 1

    def __call__(self, *args, **kwargs):
        chunk_id_iter = self._chunk_id_generator()
        primary_key_iter = self._primary_key_generator()
        atom_json_iter = self.atom_generator()

        for _ in range(self.start_id, self.end_id):
            # First, generate the ATOM JSON.
            atom_json = next(atom_json_iter)
            atom_json[self.primary_key] = next(primary_key_iter)
            atom_json['chunk_id'] = next(chunk_id_iter)

            # Next generate the SARR JSON from the ATOM JSON.
            sarr_json = self.sarr_mapper(atom_json)

            # Finally, consume the JSON.
            self.json_consumer(atom_json, sarr_json)

        self.close_resources()


class AbstractUsersDatagen(_AbstractShopALotDatagen, abc.ABC):
    PHONE_TYPES = ['HOME', 'OFFICE', 'MOBILE']

    def __init__(self, **kwargs):
        super().__init__(primary_key='user_id', **kwargs)

    def atom_generator(self):
        for _ in range(self.start_id, self.end_id):
            user_phone = {
                "type": random.choice(self.PHONE_TYPES),
                "number": self.faker_datagen.phone_number()
            }
            yield {
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

    def atom_generator(self):
        for _ in range(self.start_id, self.end_id):
            store_name = random.choice(self.STORE_NAMES)
            store_category = random.choice(self.PRODUCT_CATEGORIES)
            store = {
                "name": store_name,
                "address": {
                    "city": self.faker_datagen.city(),
                    "street": self.faker_datagen.street_address(),
                    "zip_code": self.faker_datagen.postcode()[:5].lstrip('0'),
                },
                "phone": self.faker_datagen.phone_number(),
                "category": store_category
            }
            yield store

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

        super().__init__(primary_key='order_id', **kwargs)

    def atom_generator(self):
        product_iter = self._product_generator()

        for _ in range(self.start_id, self.end_id):
            product = next(product_iter)
            price = max(float(product['list_price']) + (float(product['list_price']) * random.random()) -
                        (float(product['list_price']) / 2.0), 0.99)
            order_item = {
                "item_id": self.faker_datagen.uuid4(),
                "qty": int(abs(numpy.random.normal(1, 10))),
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

            yield {
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


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate ShopALot JSON data.')
    parser.add_argument('dataset', type=str, choices=['user', 'store', 'order'], help='Which dataset to generate.')
    parser.add_argument('--config', type=str, default='config/shopalot.json', help='Path to the config file.')
    command_line_args = parser.parse_args()
    with open(command_line_args.config) as config_file:
        main_config_json = json.load(config_file)


    class ToFileDatagen({  # Determine our parent class.
                            'user': AbstractUsersDatagen,
                            'store': AbstractStoresDatagen,
                            'order': AbstractOrdersDatagen
                        }[command_line_args.dataset]):

        def __init__(self, **kwargs):
            config = {
                'start_id': kwargs['idRange']['start'],
                'end_id': kwargs['idRange']['end'],
                'chunk_size': kwargs['chunkSize'],
                'user_start_id': main_config_json['users']['idRange']['start'],
                'user_end_id': main_config_json['users']['idRange']['end'],
                'store_start_id': main_config_json['stores']['idRange']['start'],
                'store_end_id': main_config_json['stores']['idRange']['end']
            }
            super().__init__(**config)

            self.atom_fps = {
                'full': open(kwargs['atomDataverse']['fullFilename'], 'w'),
                'eighth': open(kwargs['atomDataverse']['eighthFilename'], 'w'),
                'sample': open(kwargs['atomDataverse']['sampleFilename'], 'w')
            }
            self.sarr_fps = {
                'full': open(kwargs['sarrDataverse']['fullFilename'], 'w'),
                'eighth': open(kwargs['sarrDataverse']['eighthFilename'], 'w'),
                'sample': open(kwargs['sarrDataverse']['sampleFilename'], 'w')
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

            if self.datagen_counter < 1000:
                self._write_to_file(self.atom_fps['sample'], atom_json)
                self._write_to_file(self.sarr_fps['sample'], sarr_json)

            self._write_to_file(self.atom_fps['full'], atom_json)
            self._write_to_file(self.sarr_fps['full'], sarr_json)
            self.datagen_counter = self.datagen_counter + 1

        def close_resources(self):
            [fp.close() for fp in self.atom_fps.values()]
            [fp.close() for fp in self.sarr_fps.values()]

    # Invoke our datagen.
    ToFileDatagen(**(main_config_json[command_line_args.dataset + 's']))()
