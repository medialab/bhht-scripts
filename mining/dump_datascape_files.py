#!/usr/bin/env python3
# ==============================
# BHHT Dump Location Sets Script
# ==============================
#
# Script dumping the sets of existing location per lang from the MongoDB.
# Regex to find empty components: /(?:^|§)(?:en|fr|it|es|de|sv|pt),,/
#
import os
import sys
import csv
import msgpack
import urllib.parse as urllib
import networkx as nx
from math import radians, cos, sin, asin, sqrt
from statistics import mean
from collections import defaultdict, Counter
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

def score_aliases_set(index, aliases):
    # scores = [index[alias] for alias in aliases]

    # return mean(scores)

    score = 0

    for alias in aliases:
        score += index[alias]

    return score

def encode_links(year, links):
    return '§'.join([link + '|' + year for link in links])

def haversine(lon1, lat1, lon2, lat2):
    if lon1 == lon2 and lat1 == lat2:
        return 0

    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon/2) ** 2
    c = 2 * asin(sqrt(a))
    # Radius of earth in kilometers is 6371
    km = 6371 * c
    return km

# Arguments
if len(sys.argv) < 2:
    raise Exception('$1: [output-folder]')

output = sys.argv[1]
only_location = len(sys.argv) > 2

mongo_client = MongoClient(MONGODB['host'], MONGODB['port'])
db = mongo_client.bhht
location_collection = db.location
people_collection = db.people
entities_collection = db.entities

BASE1_PATH = DATA['people']
BASE2_PATH = DATA['location']
BASE2_MINED_PATH = os.path.join(output, 'base2_locations_mined.csv')
BASE3_MINED_PATH = os.path.join(output, 'base3_trajectoires_mined.csv')

ALIASES_INDEX = nx.Graph()
LOCATIONS_INDEX = []
ENTITIES_INDEX = {}
COORDINATES_INDEX = {}
SCORES_INDEX = Counter()

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

coordinates_bar = ProgressBar()

print('Indexing coordinates & scores...')
with open(BASE2_PATH) as f:
    reader = csv.DictReader(f)

    for line in coordinates_bar(reader):
        COORDINATES_INDEX[hasher(line['lang'], line['location'])] = {
            'lat': float(line['lat_href']),
            'lon': float(line['lon_href'])
        }

        SCORES_INDEX[line['location']] += 1

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
        aliases = set(aliases + flatten_aliases(wikidata['aliases']))
        aliases = list(aliases)

    matching_alias = next((alias.lower() for alias in aliases if alias.lower() in ALIASES_INDEX), None)
    component = ALIASES_INDEX.node[matching_alias]['component'] if matching_alias else len(LOCATIONS_INDEX)

    # Ensuring coordinates consistency
    if matching_alias:
        coordinates = COORDINATES_INDEX.get(location['_id'])

        if not coordinates and wikidata and 'coordinates' in wikidata:
            coordinates = wikidata['coordinates']

        data = LOCATIONS_INDEX[component]

        if coordinates and 'coordinates' in data:
            d = haversine(coordinates['lon'], coordinates['lat'], data['coordinates']['lon'], data['coordinates']['lat'])

            # If distance between both points is over 10km, we split
            if d > 10:

                # Solving conflict
                existing_aliases = LOCATIONS_INDEX[ALIASES_INDEX.node[matching_alias]['component']]['aliases']

                added_score = score_aliases_set(SCORES_INDEX, aliases)
                existing_score = score_aliases_set(SCORES_INDEX, existing_aliases)

                if added_score > existing_score:

                    # Added wins, we need to erase the existing aliases from the graph and current set
                    existing_aliases -= set(aliases)

                    for alias in aliases:
                        alias = alias.lower()

                        if alias in ALIASES_INDEX:
                            ALIASES_INDEX.remove_node(alias)

                else:

                    # Existing wins, we cull the new aliases
                    aliases = list(set(aliases) - existing_aliases)

                if len(aliases) == 0:
                    continue

                matching_alias = None
        else:

            # Better safe than sorry. If we don't have coordinates, we have separate components
            existing_aliases = LOCATIONS_INDEX[ALIASES_INDEX.node[matching_alias]['component']]['aliases']

            added_score = score_aliases_set(SCORES_INDEX, aliases)
            existing_score = score_aliases_set(SCORES_INDEX, existing_aliases)

            if added_score > existing_score:

                # Added wins, we need to erase the existing aliases from the graph and current set
                existing_aliases -= set(aliases)

                for alias in aliases:
                    alias = alias.lower()

                    if alias in ALIASES_INDEX:
                        ALIASES_INDEX.remove_node(alias)

            else:

                # Existing wins, we cull the new aliases
                aliases = list(set(aliases) - existing_aliases)

            if len(aliases) == 0:
                continue

            matching_alias = None

    # Handling merge normally
    if matching_alias is None:

        # TODO: due to file discrepancy (I don't have the correct file yet)
        # it's possible we don't have the coordinates...
        coordinates = COORDINATES_INDEX.get(location['_id'])

        if not coordinates and wikidata and 'coordinates' in wikidata:
            coordinates = wikidata['coordinates']

        data = {
            'aliases': set(aliases),
            'langs': set([location['lang']]),
            'instance': set()
        }

        if coordinates:
            data['coordinates'] = coordinates

        if wikidata and 'instance' in wikidata:
            data['instance'].update(collect_entities(ENTITIES_INDEX, wikidata['instance']))

        component = len(LOCATIONS_INDEX)
        LOCATIONS_INDEX.append(data)
    else:
        component = ALIASES_INDEX.node[matching_alias]['component']

        data = LOCATIONS_INDEX[component]
        data['aliases'].update(aliases)
        data['langs'].add(location['lang'])

        if wikidata and 'instance' in wikidata:
            data['instance'].update(collect_entities(ENTITIES_INDEX, wikidata['instance']))

    aliases = [alias.lower() for alias in aliases]

    for alias in aliases:
        ALIASES_INDEX.add_node(alias, component=component)

    if len(aliases) < 2:
        continue

    for i, source in enumerate(aliases):
        for j in range(i + 1, len(aliases)):
            target = aliases[j]
            ALIASES_INDEX.add_edge(source, target)

print('Writing location file')
with open(BASE2_MINED_PATH, 'w') as f:
    writer = csv.DictWriter(f, fieldnames=['langs', 'aliases', 'lat', 'lon', 'instance'])
    writer.writeheader()

    for component in LOCATIONS_INDEX:
        coordinates = component.get('coordinates')
        instance = component.get('instance')
        aliases = component['aliases']

        if len(aliases) == 0:
            continue

        writer.writerow({
            'langs': '§'.join(list(component['langs'])),
            'aliases': '§'.join(list(aliases)),
            'lat': coordinates['lat'] if coordinates else '',
            'lon': coordinates['lon'] if coordinates else '',
            'instance': '§'.join(list(instance))
        })

if only_location:
    sys.exit(0)

print('Writing path file')
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

        people_doc = people_collection.find_one({'_id': _id}, {'html': 0})

        if not people_doc:
            raise Exception('Could not find %s' % _id)

        # If we do not have links
        if 'links' not in people_doc or len(people_doc['links']) == 0:
            continue

        writer.writerow({
            'name': row['name'],
            'links': encode_links(row['estimated_birth'], people_doc['links'])
        })
