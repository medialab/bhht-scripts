#!/usr/bin/env python3
# ==============================
# BHHT Dump Location Sets Script
# ==============================
#
# Script dumping the sets of existing location per lang from the MongoDB.
#
import os
import sys
import csv
import msgpack
import urllib.parse as urllib
import networkx as nx
from collections import defaultdict
from progressbar import ProgressBar
from config import MONGODB
from pymongo import MongoClient

# Hasher
hasher = lambda lang, name: '%s§%s' % (lang, name)

encodeURIComponent = lambda string: urllib.quote(string, safe='~()*!.\'')

def flatten_aliases(aliases):
    result = []

    for lang in aliases:
        result.extend([encodeURIComponent(alias) for alias in aliases[lang]])

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
LOCATIONS_INDEX = []

LOCATION_QUERY = {'done': True}

bar = ProgressBar(max_value=location_collection.count(LOCATION_QUERY))

print('Processing locations...')
for location in bar(location_collection.find(LOCATION_QUERY, {'html': 0})):

    # Filtering bad apples
    if 'notFound' in location and location['notFound']:
        continue

    if 'badRequest' in location and location['badRequest']:
        continue

    # Filtering pages that are also people
    if people_collection.count({'_id': hasher(location['lang'], location['name'])}):
        continue

    has_wikidata = 'wikidata' in location and location['wikidata']

    # Matching aliases
    aliases = [location['name']]

    if has_wikidata and 'aliases' in location['wikidata']:
        aliases = set([location['name']] + flatten_aliases(location['wikidata']['aliases']))
        aliases = list(aliases)

    matching_alias = next((alias for alias in aliases if alias in ALIASES_INDEX), None)

    component = ALIASES_INDEX.node[matching_alias]['component'] if matching_alias else len(LOCATIONS_INDEX)

    if matching_alias is None:
        data = {
            'langs': [location['lang']]
        }

        if has_wikidata and 'coordinates' in location['wikidata']:
            data['coordinates'] = location['wikidata']['coordinates']

        LOCATIONS_INDEX.append(data)
    else:
        data = LOCATIONS_INDEX[component]
        data['langs'].append(location['lang'])

        if 'coordinates' not in data and has_wikidata and 'coordinates' in location['wikidata']:
            data['coordinates'] = location['wikidata']['coordinates']

    for alias in aliases:
        ALIASES_INDEX.add_node(alias, component=component)

    if len(aliases) < 2:
        continue

    for i, source in enumerate(aliases):
        for j in range(i + 1, len(aliases)):
            target = aliases[j]
            ALIASES_INDEX.add_edge(source, target)

print('Collapsing location aliases...')
for nodes in nx.connected_components(ALIASES_INDEX):
    nodes = list(nodes)
    component = ALIASES_INDEX.node[nodes[0]]['component']
    data = LOCATIONS_INDEX[component]
    data['aliases'] = nodes

print('Writing location file')
with open(BASE2_PATH, 'w') as f:
    writer = csv.DictWriter(f, fieldnames=['langs', 'aliases', 'lat', 'lon'])
    writer.writeheader()

    for component in LOCATIONS_INDEX:
        coordinates = component.get('coordinates', None)

        writer.writerow({
            'langs': '§'.join(component['langs']),
            'aliases': '§'.join(component['aliases']),
            'lat': coordinates['lat'] if coordinates else '',
            'lon': coordinates['lon'] if coordinates else ''
        })
