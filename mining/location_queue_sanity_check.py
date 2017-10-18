#!/usr/bin/env python3
# =================================
# BHHT Create Location Queue Script
# =================================
#
# Script loading the necessary pages from the location CSV file and
# initializing the MongoDB queue.
#
import json
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
location_collection = db.location

# Read the location file
df = pd.read_csv(DATA['location'], usecols=[0, 3], dtype={0: str, 3: str}, engine='c')

print('Location file parsed!')

bar = ProgressBar(max_value=len(df))

FILE_LOCATIONS = set()

for i, row in df.iterrows():
    FILE_LOCATIONS.add(hasher(row['lang'], row['location']))

DISAPPEARED_LOCATIONS = []

for doc in location_collection.find({}, {'html': 0}):
    if hasher(doc['lang'], doc['name']) not in FILE_LOCATIONS:
        DISAPPEARED_LOCATIONS.append((doc['lang'], doc['name']))

print(json.dumps(sorted(DISAPPEARED_LOCATIONS), ensure_ascii=False))
