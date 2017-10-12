#!/usr/bin/env python3
# ======================
# BHHT Dump Links Script
# ======================
#
# Script dumping in CSV files the location links found in the people pages.
#
import os
import csv
import sys
from progressbar import ProgressBar
from config import MONGODB
from pymongo import MongoClient

# Arguments
if len(sys.argv) < 2:
    raise Exception('$1: [output-folder]')

output = sys.argv[1]

# Mongo connection
mongo_client = MongoClient(MONGODB['host'], MONGODB['port'])
db = mongo_client.bhht
collection = db.people

LINK_PATH = os.path.join(output, 'links.csv')

# Dumping 404
print('Dumping links...')
with open(LINK_PATH, 'w') as file:
    writer = csv.DictWriter(file, fieldnames=['lang', 'name', 'links'])
    writer.writeheader()

    bar = ProgressBar(max_value=collection.count({'done': True}))

    for doc in bar(collection.find({'done': True}, {'html': 0})):

        if 'links' not in doc:
            continue

        writer.writerow({
            'lang': doc['lang'],
            'name': doc['name'],
            'links': '§'.join(doc['links'])
        })
