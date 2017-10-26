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
import networkx as nx
from collections import defaultdict
from progressbar import ProgressBar
from config import MONGODB
from pymongo import MongoClient

# Hasher
hasher = lambda lang, name: '%s§%s' % (lang, name)

def flatten_aliases(aliases):
    result = []

    for lang in aliases:
        result.extend(aliases[lang])

    return result

# Arguments
if len(sys.argv) < 2:
    raise Exception('$1: [output-folder]')

output = sys.argv[1]

mongo_client = MongoClient(MONGODB['host'], MONGODB['port'])
db = mongo_client.bhht
location_collection = db.location
people_collection = db.people
entities_collection = db.entities

BASE2_PATH = os.path.join(output, 'base2_locations_mined.csv')

ALIASES_INDEX = nx.Graph()

LOCATION_QUERY = {'done': True}

bar = ProgressBar(max_value=location_collection.count(LOCATION_QUERY))

print('Processing locations...')
for location in bar(location_collection.find(LOCATION_QUERY, {'html': 0})):

    if 'wikidata' not in location or not location['wikidata']:
        continue

    if 'aliases' in location['wikidata']:
        aliases = set([location['name']] + flatten_aliases(location['wikidata']['aliases']))

        if location['name'] in ALIASES_INDEX:
            neighbors = set([location['name']] + list(ALIASES_INDEX.neighbors(location['name'])))

            if aliases != neighbors:
                print('Error with "%s" aliases:' % location['name'])

                print('This:')
                for alias in aliases:
                    print('   %s' % alias)

                print('Vs. that:')
                for neighbor in neighbors:
                    print('   %s' % neighbor)

        aliases = list(aliases)

        for i, source in enumerate(aliases):
            for j in range(i + 1, len(aliases)):
                target = aliases[j]

                ALIASES_INDEX.add_edge(source, target)

# nx.write_gexf(ALIASES_INDEX, './aliases.gexf')

# TODO: URI encode the aliases
# TODO: verify that they are not also people
