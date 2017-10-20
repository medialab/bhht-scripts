#!/usr/bin/env python3
# ==============================
# BHHT Dump Location Sets Script
# ==============================
#
# Script dumping the sets of existing location per lang from the MongoDB.
#
import os
import sys
import msgpack
from collections import defaultdict
from progressbar import ProgressBar
from config import MONGODB
from pymongo import MongoClient

# Arguments
if len(sys.argv) < 2:
    raise Exception('$1: [output-folder]')

output = sys.argv[1]

mongo_client = MongoClient(MONGODB['host'], MONGODB['port'])
db = mongo_client.bhht
collection = db.location

SETS_PATH = os.path.join(output, 'location-sets.bin')
SETS = defaultdict(list)

bar = ProgressBar(max_value=collection.count({}))

for doc in bar(collection.find({}, {'lang': 1, 'name': 1})):
    SETS[doc['lang']].append(doc['name'])

print('Dumping binary file')
with open(SETS_PATH, 'wb') as f:
    f.write(msgpack.packb(SETS, use_bin_type=True, encoding='utf-8'))
