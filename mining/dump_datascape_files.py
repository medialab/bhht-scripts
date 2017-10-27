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
from config import DATA, MONGODB
from pymongo import MongoClient

LANGS = [
    'en',
    'de',
    'fr',
    'es',
    'it',
    'pt',
    'sv'
]

LABEL_TO_LANG = {
    'english': 'en',
    'french': 'fr',
    'german': 'de',
    'italian': 'it',
    'portuguese': 'pt',
    'spanish': 'es',
    'swedish': 'sv'
}

LANG_TO_COLUMN = {
    'en': 'english_link',
    'fr': 'french_link',
    'es': 'spanish_link',
    'pt': 'portuguese_link',
    'de': 'german_link',
    'it': 'italian_link',
    'sv': 'swedish_link'
}

# Hasher
hasher = lambda lang, name: '%s§%s' % (lang, name)

encodeURIComponent = lambda string: urllib.quote(string, safe='~()*!.\'')

def flatten_aliases(aliases):
    result = []

    for lang in aliases:
        result.extend([encodeURIComponent(alias) for alias in aliases[lang]])

    return result

def collect_entities(index, entities):
    return [index[entity] for entity in entities if entity in index]

def encode_links(year, links):
    return '§'.join([link + '@' + year for link in links])

# Arguments
if len(sys.argv) < 2:
    raise Exception('$1: [output-folder]')

output = sys.argv[1]

mongo_client = MongoClient(MONGODB['host'], MONGODB['port'])
db = mongo_client.bhht
location_collection = db.location
people_collection = db.people
entities_collection = db.entities

BASE1_PATH = DATA['people']
BASE2_MINED_PATH = os.path.join(output, 'base2_locations_mined.csv')
BASE3_MINED_PATH = os.path.join(output, 'base3_trajectoires_mined.csv')

ALIASES_INDEX = nx.Graph()
LOCATIONS_INDEX = []
ENTITIES_INDEX = {}

LOCATION_QUERY = {'done': True}
ENTITIES_QUERY = {'done': True, 'labels': {'$exists': True}}
PEOPLE_QUERY = {'done': True}

entities_bar = ProgressBar(max_value=entities_collection.count(ENTITIES_QUERY))

print('Indexing entities...')
for entity in entities_bar(entities_collection.find(ENTITIES_QUERY)):
    label = None

    # We try to find a suitable label in lang order
    for lang in LANGS:
        if lang in entity['labels']:
            label = entity['labels'][lang]
            break

    if label:
        ENTITIES_INDEX[entity['_id']] = label

location_bar = ProgressBar(max_value=location_collection.count(LOCATION_QUERY))

print('Processing locations...')
for location in location_bar(location_collection.find(LOCATION_QUERY, {'html': 0})):

    # Filtering bad apples
    if 'notFound' in location and location['notFound']:
        continue

    if 'badRequest' in location and location['badRequest']:
        continue

    # Filtering pages that are also people
    if people_collection.count({'_id': hasher(location['lang'], location['name'])}):
        continue

    wikidata = location.get('wikidata')

    # Matching aliases
    aliases = [location['name']]

    if wikidata and 'aliases' in wikidata:
        aliases = set([location['name']] + flatten_aliases(wikidata['aliases']))
        aliases = list(aliases)

    matching_alias = next((alias for alias in aliases if alias in ALIASES_INDEX), None)

    component = ALIASES_INDEX.node[matching_alias]['component'] if matching_alias else len(LOCATIONS_INDEX)

    if matching_alias is None:
        data = {
            'langs': [location['lang']],
            'instance': set()
        }

        if wikidata and 'coordinates' in wikidata:
            data['coordinates'] = wikidata['coordinates']

        if wikidata and 'instance' in wikidata:
            data['instance'].update(collect_entities(ENTITIES_INDEX, wikidata['instance']))

        LOCATIONS_INDEX.append(data)
    else:
        data = LOCATIONS_INDEX[component]
        data['langs'].append(location['lang'])

        if 'coordinates' not in data and wikidata and 'coordinates' in wikidata:
            data['coordinates'] = wikidata['coordinates']

        if wikidata and 'instance' in wikidata:
            data['instance'].update(collect_entities(ENTITIES_INDEX, wikidata['instance']))

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
with open(BASE2_MINED_PATH, 'w') as f:
    writer = csv.DictWriter(f, fieldnames=['langs', 'aliases', 'lat', 'lon', 'instance'])
    writer.writeheader()

    for component in LOCATIONS_INDEX:
        coordinates = component.get('coordinates')
        instance = component.get('instance')

        writer.writerow({
            'langs': '§'.join(component['langs']),
            'aliases': '§'.join(component['aliases']),
            'lat': coordinates['lat'] if coordinates else '',
            'lon': coordinates['lon'] if coordinates else '',
            'instance': '§'.join(list(instance))
        })

with open(BASE3_MINED_PATH, 'w') as mf, open(BASE1_PATH, 'r') as pf:
    reader = csv.DictReader(pf)
    writer = csv.DictWriter(mf, fieldnames=['name', 'links'])
    writer.writeheader()

    people_bar = ProgressBar()

    for row in people_bar(reader):
        main_lang = LABEL_TO_LANG[row['language']]
        name = row[LANG_TO_COLUMN[main_lang]]

        if not row['estimated_birth'] or row['estimated_birth'] == '.':
            continue

        _id = hasher(main_lang, name)

        people_doc = people_collection.find_one({'_id': _id})

        if not people_doc:
            raise Exception('Could not find %s' % _id)

        if 'links' not in people_doc:
            continue

        writer.writerow({
            'name': row['name'],
            'links': encode_links(row['estimated_birth'], people_doc['links'])
        })
