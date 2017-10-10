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
if len(sys.argv) < 2:
    raise Exception('$1: expecting an output folder.')

output = sys.argv[1]

# Mongo connection
mongo_client = MongoClient(MONGODB['host'], MONGODB['port'])
db = mongo_client.bhht
collection = db.people

NOT_FOUND_PATH = os.path.join(output, 'not-found.csv')
BAD_REQUEST_PATH = os.path.join(output, 'bad-request.csv')

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
