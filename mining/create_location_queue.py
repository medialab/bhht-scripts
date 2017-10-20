#!/usr/bin/env python3
# =================================
# BHHT Create Location Queue Script
# =================================
#
# Script loading the necessary pages from the location CSV file and
# initializing the MongoDB queue.
#
from config import DATA, MONGODB
import pandas as pd
from progressbar import ProgressBar
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

# Hasher
hasher = lambda lang, name: '%s§%s' % (lang, name)

# Mongo connection
mongo_client = MongoClient(MONGODB['host'], MONGODB['port'])
db = mongo_client.bhht
# db.drop_collection('location')
location_collection = db.location

# Ensuring indices
location_collection.create_index('done')
location_collection.create_index('notFound')
location_collection.create_index('badRequest')

# Read the location file
df = pd.read_csv(DATA['location'], usecols=[0, 3], dtype={0: str, 3: str}, engine='c')

print('Location file parsed!')

bar = ProgressBar(max_value=len(df))

# Iterating over a shuffled version of the frame to generate entropy for the crawls
for i, row in bar(df.sample(frac=1).iterrows()):
    _id = hasher(row['lang'], row['location'])

    # NOTE: this should not be a replace_one here. It erased the html pages!
    location_collection.replace_one({'_id': _id}, {
        '_id': _id,
        'name': row['location'],
        'lang': row['lang'],
        'done': False
    }, upsert=True)

print('Locations inserted into MongoDB queue!')
