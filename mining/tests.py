#!/usr/bin/env python3
# ========================
# BHHT Create Tests Script
# ========================
#
# Simple tests checking data coherence etc.
#
import zlib
from pymongo import MongoClient
from config import MONGODB

mongo_client = MongoClient(MONGODB['host'], MONGODB['port'])
db = mongo_client.bhht
collection = db.people

docs = collection.find({'html': {'$exists': True}}, limit=10)

# compressed = doc['html']
# uncompressed = zlib.decompress(doc['html']).decode('utf-8')

for doc in docs:
    print(doc['lang'], doc['name'])
    # print(zlib.decompress(doc['html']).decode('utf-8'))
