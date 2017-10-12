#!/usr/bin/env python3
# ======================
# BHHT Dump Error Script
# ======================
#
# Script dumping in CSV files the pages not found & the one yielding a
# bad request.
#
import os
import csv
import sys
from config import MONGODB
from pymongo import MongoClient

# Arguments
if len(sys.argv) < 3:
    raise Exception('$1 $2: [model] [output-folder]')

model = sys.argv[1]
output = sys.argv[2]

# Mongo connection
mongo_client = MongoClient(MONGODB['host'], MONGODB['port'])
db = mongo_client.bhht
collection = db[model]

NOT_FOUND_PATH = os.path.join(output, '%s-not-found.csv' % model)
BAD_REQUEST_PATH = os.path.join(output, '%s-bad-request.csv' % model)

# Dumping 404
print('Dumping `not found`...')
with open(NOT_FOUND_PATH, 'w') as file:
    writer = csv.DictWriter(file, fieldnames=['lang', 'name'])
    writer.writeheader()

    for doc in collection.find({'notFound': True}):
        writer.writerow({
            'lang': doc['lang'],
            'name': doc['name']
        })

print('Dumping `bad request`...')
with open(BAD_REQUEST_PATH, 'w') as file:
    writer = csv.DictWriter(file, fieldnames=['lang', 'name'])
    writer.writeheader()

    for doc in collection.find({'badRequest': True}):
        writer.writerow({
            'lang': doc['lang'],
            'name': doc['name']
        })
