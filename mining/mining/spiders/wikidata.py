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
collection = db.location

QUERY = {
    'done': True,
    'wikidata': {
        '$exists': False
    }
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

BASE_URL = 'https://www.wikidata.org/w/api.php?action=wbgetentities&format=json'


def wikidata_url(lang, name):
    return u'%(url)s&sites=%(lang)swiki&titles=%(name)s&props=aliases|claims&languages=%(languages)s' % {
        'url': BASE_URL,
        'lang': lang,
        'name': name,
        'languages': '|'.join(LANGS)
    }


def parse_response(response):
    try:
        wikidata = {}

        data = json.loads(response.body_as_unicode())
        entity = next(iter(data['entities']))

        if entity == '-1':
            return None

        entity_data = data['entities'][entity]

        if 'aliases' in entity_data and len(entity_data['aliases']) > 0:
            wikidata['aliases'] = entity_data['aliases']

        if len(wikidata) < 1:
            return None

        return wikidata
    except json.decoder.JSONDecodeError:
        return None


class LocationSpider(scrapy.Spider):
    name = 'wikidata'
    handle_httpstatus_list = [404]

    def start_requests(self):
        self.cursor = collection.find(QUERY, {'lang': 1, 'name': 1}, limit=30, skip=1000, no_cursor_timeout=True)

        try:
            for doc in self.cursor:
                url = wikidata_url(doc['lang'], doc['name'])

                yield scrapy.Request(
                    url=url,
                    meta={
                        'name': doc['name'],
                        'lang': doc['lang']
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
        logging.info(u'%i, %s§%s' % (response.status, response.meta['lang'], response.meta['name']))

        yield {
            'model': 'location',
            'type': 'wikidata',
            'lang': response.meta['lang'],
            'name': response.meta['name'],
            'wikidata': None if response.status == 404 else parse_response(response)
        }
