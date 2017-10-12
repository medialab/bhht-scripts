#!/usr/bin/env python3
# ===============================
# BHHT Create People Queue Script
# ===============================
#
# Script loading the necessary pages from the people CSV file and initializing
# the MongoDB queue.
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
db = mongo_client.bhht
db.drop_collection('people')
people_collection = db.people

# Ensuring indices
people_collection.create_index('lang')
people_collection.create_index('done')
people_collection.create_index('notFound')
people_collection.create_index('badRequest')

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
