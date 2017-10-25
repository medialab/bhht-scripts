#!/usr/bin/env python3
# =================================
# BHHT Create Entities Queue Script
# =================================
#
# Script loading unique entities from the location database to create the
# entities queue.
#
from config import DATA, MONGODB
import pandas as pd
from progressbar import ProgressBar
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

# Mongo connection
mongo_client = MongoClient(MONGODB['host'], MONGODB['port'])
db = mongo_client.bhht
location_collection = db.location
entities_collection = db.entities

# Ensuring indices
entities_collection.create_index('done')

# Finding unique entities
ENTITIES = set()

bar = ProgressBar()

for location in bar(location_collection.find({}, {'wikidata': 1})):
    if 'wikidata' not in location or not location['wikidata']:
        continue

    for prop in location['wikidata']:
        if prop == 'coordinates' or prop == 'aliases' or prop == 'id':
            continue

        for entity in location['wikidata'][prop]:
            if entity in ENTITIES:
                continue

            if len(entity) < 2 or not entity.startswith('Q'):
                raise Exception('Bad entity %s' % entity)

            ENTITIES.add(entity)

            entities_collection.insert_one({
                '_id': entity,
                'done': False
            })

print('Found %i entities!' % len(ENTITIES))
