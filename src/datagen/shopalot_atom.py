#!/usr/local/bin/python3
import datetime
import decimal
import os
import random
import faker
import json
import numpy
import argparse

from src.datagen.shopalot_universe import *


def _file_handler(out_fp, out_json):
    json.dump(out_fp, out_json)
    out_fp.write('\n')


def generate_users(handler, fake_datagen, start_id, end_id):
    for u in range(start_id, end_id):
        generated_id = str(u)

        user_phone = {
            "type": random.choice(phone_types),
            "number": fake_datagen.phone_number()[:20]
        }
        user = {
            "user_id": generated_id,
            "name": {
                "first": fake_datagen.first_name(),
                "last": fake_datagen.last_name()
            },
            "phone": user_phone
        }
        handler(user)


def generate_stores(handler, fake_datagen, start_id, end_id):
    for s in range(start_id, end_id):
        store_id = str(s)

        store_name = random.choice(store_names)
        store_category = random.choice(product_categories)
        store = {
            "store_id": store_id,
            "name": store_name,
            "address": {
                "city": fake_datagen.city(),
                "street": fake_datagen.street_address(),
                "zip_code": fake_datagen.postcode()[:5].lstrip('0'),
            },
            "phone": fake_datagen.phone_number()[:20],
            "category": store_category
        }
        handler(store)


def generate_products(handler):
    current_id = 1
    for data_file in os.listdir('resources/products'):
        with open('resources/products/' + data_file) as working_file:
            working_json = json.load(working_file)

        for product in working_json['response']['docs']:
            product_id = str(current_id)
            category = products_filename_to_product_category(data_file)
            product_json = {
                "product_id": product_id,
                "category": category,
                "name": product['name'],
                "description": product['shelfName'],
                "list_price": max(product['basePrice'], 0.99)
            }
            handler(product_json)

            current_id = current_id + 1
            product_set.append((product_id, product['basePrice'], category,))


def generate_orders(handler, fake_datagen, start_id, end_id):
    # Order generator-specific constants below.
    USER_START_ID, USER_END_ID = 0, 20000000
    STORE_START_ID, STORE_END_ID = 0, 20000000
    PICKUP_TIME_NULL_PROB = 0.5

    for o in range(start_id, end_id):
        order_id = str(o)

        user_id = str(random.randint(USER_START_ID, USER_END_ID))
        store_id = str(random.randint(STORE_START_ID, STORE_END_ID))

        total_price = 0
        item_id = main_faker_datagen.uuid4()
        product = random.choice(product_set)
        price = max(float(product[1]) + (float(product[1]) * random.random()) - (float(product[1]) / 2.0), 0.99)
        price = float(decimal.Decimal(price).quantize(decimal.Decimal('0.01'), decimal.ROUND_HALF_UP))
        qty = int(abs(numpy.random.normal(1, 10)))

        total_price += price * qty
        time_placed = main_faker_datagen.date_time_this_month()
        pickup_time = main_faker_datagen.date_time_between_dates(
            datetime_start=time_placed, datetime_end=time_placed + datetime.timedelta(hours=6))
        date_fulfilled_field = str(main_faker_datagen.date_time_between_dates(
            datetime_start=pickup_time, datetime_end=pickup_time + datetime.timedelta(hours=6)))

        order_item = {
            "item_id": item_id,
            "qty": qty,
            "price": price,
            "product_id": product[0]
        }
        order = {
            "order_id": order_id,
            "total_price": total_price,
            "time_placed": str(time_placed),
            "pickup_time": random.choices([None, str(pickup_time)],
                                          weights=[PICKUP_TIME_NULL_PROB, 1 - PICKUP_TIME_NULL_PROB], k=1)[0],
            "user_id": user_id,
            "store_id": store_id,
            "time_fulfilled": str(date_fulfilled_field),
            "item": order_item
        }
        handler(order)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate atomic ShopALot JSON data.')
    parser.add_argument('dataset', type=str, choices=['user', 'store', 'order'], help='Which dataset to generate.')
    parser.add_argument('--filename', type=str, default=None, help='Name of the results file.')
    parser.add_argument('--idrange', type=int, nargs=2, default=[0, 10], help='Start and end of IDs (implicit count.')
    command_line_args = parser.parse_args()

    random_seed = datetime.datetime.now()
    faker.Faker.seed(random_seed)
    random.seed(random_seed)

    # Maintain some global state to pass between dataset creation.
    main_faker_datagen = faker.Faker()
    product_set = list()

    # Open the file to write the JSON data to.
    filename = command_line_args.filename if command_line_args.filename is not None else {
        'user': 'UsersOutput.json',
        'store': 'StoresOutput.json',
        'order': 'OrdersOutput.json'
    }[command_line_args.dataset]
    fp = open(command_line_args.filename, 'w')

    # We will always generate products.
    products_fp = open('ProductsOutput.json', 'w')
    generate_products(products_fp)
    products_fp.close()

    {  # Execute the data generator.
        'user': generate_users,
        'store': generate_stores,
        'order': generate_orders
    }[command_line_args.dataset](lambda a: _file_handler(fp, a), main_faker_datagen,
                                 command_line_args.idrange[0], command_line_args.idrange[1])

    fp.close()
