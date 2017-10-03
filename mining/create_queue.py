#!/usr/bin/env python3
# ========================
# BHHT Create Queue Script
# ========================
#
# Script loading the necessary pages from the ES index and initializing the
# MongoDB queue.
#
from config import DATA, MONGODB
import pandas as pd
from progressbar import ProgressBar
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

# Constants
LANGUAGES_COLUMNS = [
    ('english_link', 'en'),
    ('french_link', 'fr'),
    ('spanish_link', 'es'),
    ('portuguese_link', 'pt'),
    ('german_link', 'de'),
    ('italian_link', 'it'),
    ('swedish_link', 'sv')
]

# Hasher
hasher = lambda lang, name: '%s§%s' % (lang, name)

# Mongo connection
mongo_client = MongoClient(MONGODB['host'], MONGODB['port'])
mongo_client.drop_database('bhht')
db = mongo_client.bhht
people_collection = db.people
location_collection = db.location

# Ensuring indices
people_collection.create_index('lang')
people_collection.create_index('done')

location_collection.create_index('done')

# Read the people file
df = pd.read_csv(DATA['people'], usecols=range(9), dtype={i: str for i in range(9)}, engine='c')

print('People file parsed!')

bar = ProgressBar(max_value=len(df))
duplicates = 0

for i, row in bar(df.iterrows()):
    items = []

    for column, prop in LANGUAGES_COLUMNS:
        if pd.notnull(row[column]):
            items.append({
                '_id': hasher(prop, row[column]),
                'lang': prop,
                'name': row[column],
                'done': False
            })

    try:
        people_collection.insert_many(items, ordered=False)
    except BulkWriteError as e:
        duplicates += 1

print('People inserted into MongoDB queue! (Found %i duplicates)' % duplicates)

# Read the location file
df = pd.read_csv(DATA['location'], usecols=[0], dtype={0: str}, engine='c')

print('Location file parsed!')

# TODO: the file has duplicate values
already_done = set()

bar = ProgressBar(max_value=len(df))

for i, row in bar(df.iterrows()):

    if row['location'] in already_done:
        continue

    location_collection.insert({
        '_id': row['location'],
        'name': row['location'],
        'done': False
    })

    already_done.add(row['location'])

print('Locations inserted into MongoDB queue!')
