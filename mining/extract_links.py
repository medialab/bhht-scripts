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
import pandas as pd
from bs4 import BeautifulSoup
from progressbar import ProgressBar
from multiprocessing import Pool
from config import DATA, PROCESSORS, MONGODB
from pymongo import MongoClient

# TODO: césar (gallery), Alexander_the_Great (iconography)

# Building the location set
# df = pd.read_csv(DATA['location'], usecols=[0], dtype={0: str}, engine='c')

LOCATIONS = set()

# for _, row in df.iterrows():
#     LOCATIONS.add(row['location'])

# Mongo connection
mongo_client = MongoClient(MONGODB['host'], MONGODB['port'])
db = mongo_client.bhht
collection = db.people

# Predicates
QUERY = {'done': True, '_id': 'fr§Louis_XVI'}

# Process
def extract_links(doc):

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

    for link in all_links:
        href = link.get('href')

        if not href:
            continue

        # Keeping only wiki links
        if not href.startswith('/wiki/'):
            continue

        href = href.split('/wiki/')[1].strip()

        # Keeping only locations
        # if href not in LOCATIONS:
        #     continue

        relevant_links.add(href)

    for link in relevant_links:
        print(link)

    return True

# Master
if __name__ == '__main__':

    with Pool(processes=PROCESSORS) as pool:

        # TODO: predicate
        nb_docs = collection.count(QUERY)

        cursor = collection.find(QUERY, no_cursor_timeout=True)

        # Cleanup
        def sigint_handler(signal, frame):
            print('Closing the cursor...')
            cursor.close()
            pool.terminate()
            sys.exit(0)

        signal.signal(signal.SIGINT, sigint_handler)

        bar = ProgressBar(max_value=nb_docs)

        for doc in bar(pool.imap_unordered(extract_links, cursor)):
            pass

        print('Closing the cursor...')
        cursor.close()
