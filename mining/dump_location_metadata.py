#!/usr/bin/env python3
# ===========================
# BHHT Location Metadata Dump
# ===========================
#
# Script dumping the location metadata.
#
import csv
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

FIELDNAMES = [
    'id',
    'wikidata_id',
    'lang',
    'name',
    'not_found',
    'bad_request',
    'lat',
    'lon',
    'precision',
    'altitude',
    'instance',
    'instance_entities',
    'category',
    'category_entities',
    'country',
    'country_entities'
]

for lang in LANGS:
    FIELDNAMES.append('aliases_' + lang)

OUTPUT_PATH = './locations.csv'
ENTITIES_PATH = './entities.csv'

LOCATION_QUERY = {'done': True}
ENTITIES_QUERY = {'done': True, 'labels': {'$exists': True}}

ENTITIES_INDEX = {}

def collect_entities(index, entities):
    return [index[entity] for entity in entities if entity in index]

mongo_client = MongoClient(MONGODB['host'], MONGODB['port'])
db = mongo_client.bhht
location_collection = db.location
entities_collection = db.entities

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
output_file = open(OUTPUT_PATH, 'w')
writer = csv.DictWriter(output_file, fieldnames=FIELDNAMES)
writer.writeheader()

print('Dumping locations...')
for location in location_bar(location_collection.find(LOCATION_QUERY, {'html': 0})):

    row = {
        'id': location['_id'],
        'lang': location['lang'],
        'name': location['name']
    }

    # Filtering bad apples
    if 'notFound' in location and location['notFound']:
        row['not_found'] = 'yes'
        writer.writerow(row)
        continue

    if 'badRequest' in location and location['badRequest']:
        row['bad_request'] = 'yes'
        writer.writerow(row)
        continue

    wikidata = location.get('wikidata')

    if wikidata:

        row['id'] = wikidata.get('id', '')

        position = wikidata.get('location')

        if position:
            row['lat'] = position.get('lat', '')
            row['lon'] = position.get('lon', '')
            row['precision'] = position.get('precision', '')
            row['altitude'] = position.get('altitude', '')

        aliases = wikidata.get('aliases')

        if aliases:
            for lang in LANGS:
                a = aliases.get(lang)

                if a:
                    row['aliases_' + lang] = '|'.join(a)

        country = wikidata.get('country')

        if country:
            row['country'] = '|'.join(collect_entities(ENTITIES_INDEX, country))
            row['country_entities'] = '|'.join(country)

        category = wikidata.get('category')

        if category:
            row['category'] = '|'.join(collect_entities(ENTITIES_INDEX, category))
            row['category_entities'] = '|'.join(category)

        instance = wikidata.get('instance')

        if instance:
            row['instance'] = '|'.join(collect_entities(ENTITIES_INDEX, instance))
            row['instance_entities'] = '|'.join(instance)

    writer.writerow(row)

output_file.close()
