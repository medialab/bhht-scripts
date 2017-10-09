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
from bs4 import BeautifulSoup
from progressbar import ProgressBar
from multiprocessing import Pool
from config import PROCESSORS, MONGODB
from pymongo import MongoClient

# Mongo connection
mongo_client = MongoClient(MONGODB['host'], MONGODB['port'])
db = mongo_client.bhht
collection = db.people

# Predicates
QUERY = {'done': True}

# Process
def extract_links(doc):
    if (
        ('notFound' in doc and doc['notFound']) or
        ('badRequest' in doc and doc['badRequest'])
    ):
        return False

    html = zlib.decompress(doc['html']).decode('utf-8')
    soup = BeautifulSoup(html, 'lxml')

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
