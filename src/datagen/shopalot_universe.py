#!/usr/local/bin/python3

store_names = [
    "Machias Mainway",
    "Casey's Cove Convenience Store",
    "Maay Convenient Inc",
    "Cefco",
    "Mapco Express",
    "Plaid Pantry",
    "Jackson Food Store",
    "Rutters Farm Store",
    "Irving Oil Corp",
    "Hillcrest",
    "Cubbys",
    "7-Eleven",
    "Service Champ",
    "Express Mart Stores",
    "Baum's Mercantile",
    "Border Station",
    "Country Market",
    "Golden Spike Travel Plaza",
    "Wesco Oil CO",
    "Cracker Barrel Stores Inc",
    "Elnemr Enterprises Inc",
    "Sheetz",
    "Pump-N-Pantry Of NY",
    "Spaceway Oil CO",
    "6-Twelve Convenient-Mart Inc",
    "Dandy Mini Mart",
    "Stripes Llc",
    "Plaid Pantries Inc",
    "Quick Chek Food Stores",
    "Victory Marketing Llc",
    "Beasley Enterprises Inc",
    "Lil' Champ",
    "Pit Row",
    "Popeye Shell Superstop",
    "Mapco",
    "Sunset Foods",
    "Simonson Market",
    "Fasmart",
    "Super Quik Inc",
    "Jim's Quick Stop"
]

product_categories = [
    'Baby Care', 'Beverages', 'Bread & Bakery', 'Breakfast & Cereal', 'Canned Goods & Soups',
    'Condiments, Spice, & Bake', 'Cookies, Snacks, & Candy', 'Dairy, Eggs, & Cheese', 'Deli', 'Frozen Foods',
    'Fruits & Vegetables', 'Grains, Pasta, & Sides', 'Meat & Seafood', 'Paper, Cleaning, & Home',
    'Personal Care & Health', 'Pet Care'
]

phone_types = ['HOME', 'OFFICE', 'MOBILE']


def products_filename_to_product_category(filename):
    return {
        "baby-care.json": "Baby Care",
        "beverages.json": "Beverages",
        "bread-bakery.json": "Bread & Bakery",
        "breakfast-cereal.json": "Breakfast & Cereal",
        "canned-goods-soups.json": "Canned Goods & Soups",
        "condiments-spice-bake.json": "Condiments, Spice, & Bake",
        "cookies-snacks-candy.json": "Cookies, Snacks, & Candy",
        "dairy-eggs-cheese.json": "Dairy, Eggs, & Cheese",
        "deli.json": "Deli",
        "frozen-foods.json": "Frozen Foods",
        "fruits-vegetables.json": "Fruits & Vegetables",
        "grains-pasta-sides.json": "Grains, Pasta, & Sides",
        "meat-seafood.json": "Meat & Seafood",
        "paper-cleaning-home.json": "Paper, Cleaning, & Home",
        "personal-care-health.json": "Personal Care & Health",
        "pet-care.json": "Pet Care"
    }[filename]
