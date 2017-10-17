#!/usr/bin/env python3
# ====================
# BHHT Wikidata Script
# ====================
#
# Script attempting to retrieve wikidata information for our locations.
#
import wptools
from progressbar import ProgressBar
from config import MONGODB
from pymongo import MongoClient

# Mongo connection
mongo_client = MongoClient(MONGODB['host'], MONGODB['port'])
db = mongo_client.bhht
collection = db.location

for doc in collection.find({'done': True, 'wikidata': {'$exists': False}}, {'html': 0}):
    page = wptools.page(doc['name'], lang=doc['lang'])

    try:
        data = page.get_wikidata().data
    except LookupError:
        print('Could not lookup %s' % doc['_id'])
        continue

    payload = {'wikidata': data['wikidata']}

    if 'aliases' in data:
        payload['aliases'] = data['aliases']

    collection.update_one({'_id': doc['_id']}, {'$set': payload})
