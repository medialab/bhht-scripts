#!/usr/bin/env python3
# ===========================
# BHHT Location Metadata Dump
# ===========================
#
# Script dumping the location metadata.
#
import csv
import networkx as nx
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
CATEGORY_GRAPH_PATH = './category-graph.gexf'
INSTANCE_GRAPH_PATH = './instance-graph.gexf'

LOCATION_QUERY = {'done': True}
ENTITIES_QUERY = {'done': True, 'labels': {'$exists': True}}

ENTITIES_INDEX = {}
INSTANCE_GRAPH = nx.Graph()
CATEGORY_GRAPH = nx.Graph()

def add_clique(g, clique, labels):
    for i in range(len(clique)):
        A = clique[i]
        A_label = labels[i]

        g.add_node(A, label=A_label)

        for j in range(i + 1, len(clique)):
            B = clique[j]
            B_label = labels[j]

            g.add_node(B, label=B_label)

            if g.has_edge(i, j):
                g[i][j]['weight'] += 1
            else:
                g.add_edge(i, j, weight=1)

def collect_entities(index, entities):
    return [index.get(entity, entity) for entity in entities]

mongo_client = MongoClient(MONGODB['host'], MONGODB['port'])
db = mongo_client.bhht
location_collection = db.location
entities_collection = db.entities

entities_bar = ProgressBar(max_value=entities_collection.count(ENTITIES_QUERY))

print('Indexing entities...')
with open(ENTITIES_PATH, 'w') as f:
    ew = csv.DictWriter(f, fieldnames=['id', 'label'])
    ew.writeheader()

    for entity in entities_bar(entities_collection.find(ENTITIES_QUERY)):
        label = None

        # We try to find a suitable label in lang order
        for lang in LANGS:
            if lang in entity['labels']:
                label = entity['labels'][lang]
                break

        if label:
            ENTITIES_INDEX[entity['_id']] = label
            ew.writerow({'id': entity['_id'], 'label': label})

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
            country_labels = collect_entities(ENTITIES_INDEX, country)
            row['country'] = '|'.join(country_labels)
            row['country_entities'] = '|'.join(country)

        category = wikidata.get('category')

        if category:
            category_labels = collect_entities(ENTITIES_INDEX, category)
            row['category'] = '|'.join(category_labels)
            row['category_entities'] = '|'.join(category)

            add_clique(CATEGORY_GRAPH, category, category_labels)

        instance = wikidata.get('instance')

        if instance:
            instance_labels = collect_entities(ENTITIES_INDEX, instance)
            row['instance'] = '|'.join(instance_labels)
            row['instance_entities'] = '|'.join(instance)

            add_clique(INSTANCE_GRAPH, instance, instance_labels)

    writer.writerow(row)

output_file.close()

print('Dumping graphs...')
nx.write_gexf(CATEGORY_GRAPH, CATEGORY_GRAPH_PATH)
nx.write_gexf(INSTANCE_GRAPH, INSTANCE_GRAPH_PATH)
