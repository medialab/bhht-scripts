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
import json
from pymongo import MongoClient

# Mongo connection
mongo_client = MongoClient(MONGODB['host'], MONGODB['port'])
db = mongo_client.bhht
collection = db.location

for doc in collection.find({'_id': 'en§Amsterdam', 'done': False}, {'html': 0}):
    print(doc)
    # if 'wikidata' in doc and doc['wikidata'] is None:
    #     continue

    page = wptools.page(doc['name'], lang=doc['lang'])

    try:
        data = page.get_wikidata().data
        print(json.dumps(data, ensure_ascii=False))
    except:
        pass
