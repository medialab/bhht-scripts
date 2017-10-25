import os
import scrapy
import logging
import json
from pymongo import MongoClient
from config import MONGODB, PROXY

if PROXY:
    os.environ['http_proxy'] = 'http://' + PROXY
    os.environ['https_proxy'] = 'https://' + PROXY

client = MongoClient(MONGODB['host'], MONGODB['port'])
db = client.bhht
collection = db.entities

QUERY = {
    'done': False
}

LANGS = [
    'en',
    'de',
    'fr',
    'it',
    'pt',
    'es',
    'sv'
]

BASE_URL = 'https://www.wikidata.org/w/api.php?action=wbgetentities&format=json&props=labels'


def wikidata_url(_id):
    return '%s&ids=%s&languages=%s' % (BASE_URL, _id, '|'.join(LANGS))

def parse_response(_id, response):
    try:
        data = json.loads(response.body_as_unicode())

        labels_data = data['entities'][_id]['labels']

        return {lang: item['value'] for lang, item in labels_data.items()}
    except json.decoder.JSONDecodeError:
        return None


class LocationSpider(scrapy.Spider):
    name = 'entities'

    def start_requests(self):
        self.cursor = collection.find(QUERY, {'_id': 1}, no_cursor_timeout=True)

        try:
            for doc in self.cursor:
                url = wikidata_url(doc['_id'])

                yield scrapy.Request(
                    url=url,
                    meta={
                        '_id': doc['_id']
                    },
                    callback=self.parse
                )
        except:
            logging.info('Closing MongoDB cusor.')
            self.cursor.close()
            raise

        logging.info('Closing MongoDB cusor.')
        self.cursor.close()

    def closed(self, reason):

        if self.cursor:
            logging.info('Closing MongoDB cusor.')
            self.cursor.close()

    def parse(self, response):
        logging.info(u'%i, %s' % (response.status, response.meta['_id']))

        yield {
            'model': 'entities',
            '_id': response.meta['_id'],
            'labels': parse_response(response.meta['_id'], response)
        }
