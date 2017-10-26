#!/usr/bin/env python3
# ==============================
# BHHT Patch People Queue Script
# ==============================
#
# Script patching the people queue by adding some missing items.
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
people_collection = db.people

# Read the people file
column_range = list(range(9)) + [14, 15]
df = pd.read_csv(DATA['people'], usecols=column_range, dtype={i: str for i in column_range}, engine='c')

print('People file parsed!')

bar = ProgressBar(max_value=len(df))

for i, row in bar(df.iterrows()):
    update = {}

    if pd.notnull(row['estimated_birth']):
        update['estimatedBirthDate'] = int(row['estimated_birth'])

    if pd.notnull(row['estimated_death']):
        update['estimatedDeathDate'] = int(row['estimated_death'])

    if len(update) == 0:
        continue

    update = {'$set': update}

    for column, prop in LANGUAGES_COLUMNS:
        if pd.notnull(row[column]):
            data = {
                '_id': hasher(prop, row[column])
            }

        people_collection.update_one(data, update)

print('People patched!')
