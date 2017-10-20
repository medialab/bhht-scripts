#!/usr/bin/env python3
# =========================
# BHHT Extract Links Script
# =========================
#
# Parallel script extracting the links from the page's html.
#
import sys
import signal
import zlib
import msgpack
from collections import defaultdict
from bs4 import BeautifulSoup
from progressbar import ProgressBar
from multiprocessing import Pool
from config import DEBUG, DATA, PROCESSORS, MONGODB
from pymongo import MongoClient

# Tests:
#   en§Alexander_the_Great (iconography)
#   en§Julius_Caesar (gallery)

# Arguments
if len(sys.argv) < 2:
    raise Exception('$1: [locations-sets-path]')

LOCATION_SETS_PATH = sys.argv[1]
LOCATIONS = None

# Building the location set
with open(LOCATION_SETS_PATH, 'rb') as f:
    LOCATIONS = msgpack.unpackb(f.read(), use_list=False, encoding='utf-8')

for lang in LOCATIONS:
    LOCATIONS[lang] = frozenset(LOCATIONS[lang])

# Mongo connection
mongo_client = MongoClient(MONGODB['host'], MONGODB['port'], connect=False)
db = mongo_client.bhht
collection = db.people

# Predicates
QUERY = {'done': True}

def is_bad_parent(parent):

    # Handling references
    if parent.name == 'cite':
        return True

    # Handling some more references
    if parent.name == 'li':
        parent_id = parent.get('id')

        if not parent_id:
            return False

        if parent_id.startswith('cite_note-'):
            return True

    # Handling only some tag names
    if parent.name != 'div' and parent.name != 'ul':
        return False

    # Filtering navs
    if parent.get('role') == 'navigation':
        return True

    classes = parent.get('class')
    has_class = classes is not None

    # Filtering thumbs and iconography
    if has_class and 'thumbinner' in classes:
        return True

    # Filtering galleries
    if has_class and 'gallery' in classes:
        return True

    return False

# Process
def extract_links(_id):

    doc = collection.find_one({'_id': _id}, {'links': 0})

    # TODO: add a links filter here for good measure
    if (
        ('notFound' in doc and doc['notFound']) or
        ('badRequest' in doc and doc['badRequest'])
    ):
        return False

    html = zlib.decompress(doc['html']).decode('utf-8')
    soup = BeautifulSoup(html, 'lxml')

    content = soup.find(id='content')
    all_links = content.find_all('a')

    relevant_links = set()
    relevant_locations = LOCATIONS[doc['lang']]

    for link in all_links:
        href = link.get('href')

        if not href:
            continue

        # Keeping only wiki links
        if not href.startswith('/wiki/'):
            continue

        href = href.split('/wiki/')[1].strip()

        # Dropping one letter href (often bugs, or not a location anyway...)
        if len(href) <= 1:
            continue

        # Dropping if ':' (categories & meta...)
        if ':' in href:
            continue

        # Keeping only locations
        if href not in relevant_locations:
            continue

        # Avoiding navs
        if any(is_bad_parent(parent) for parent in link.parents):
            continue

        relevant_links.add(href)

    # Updating the document
    collection.update_one(
        {'_id': doc['_id']},
        {'$set': {'links': list(relevant_links)}}
    )

    if not DEBUG:
        return True

    # Debug dump
    with open(u'.log/%s§%s.txt' % (doc['lang'], doc['name']), 'w') as file:
        for link in sorted(relevant_links):
            file.write(link + '\n')

    return True

# Master
if __name__ == '__main__':

    with Pool(processes=PROCESSORS) as pool:

        nb_docs = collection.count(QUERY)

        cursor = collection.find(QUERY, {'_id': 1}, no_cursor_timeout=True)

        # Cleanup
        def sigint_handler(signal, frame):
            print('Closing the cursor...')
            cursor.close()
            pool.terminate()
            sys.exit(0)

        signal.signal(signal.SIGINT, sigint_handler)

        bar = ProgressBar(max_value=nb_docs)

        id_iterator = (doc['_id'] for doc in cursor)

        for doc in bar(pool.imap_unordered(extract_links, id_iterator, chunksize=10)):
            pass

        print('Closing the cursor...')
        cursor.close()
