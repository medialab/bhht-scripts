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

LABELS = {
    'P17': 'country',
    'P30': 'continent',
    'P31': 'instance',

    'P276': 'location',
    'P279': 'subclass',

    'P625': 'coordinates',
    'P910': 'category'
}

BASE_URL = 'https://www.wikidata.org/w/api.php?action=wbgetentities&format=json'


def wikidata_url(lang, name):
    return u'%(url)s&sites=%(lang)swiki&titles=%(name)s&props=aliases|claims&languages=%(languages)s' % {
        'url': BASE_URL,
        'lang': lang,
        'name': name,
        'languages': '|'.join(LANGS)
    }


def collect_claims(claims, prop):
    if prop != 'P625':
        return [claim['mainsnak']['datavalue']['value']['id'] for claim in claims[prop] if claim['mainsnak']['snaktype'] != 'novalue']
    else:
        first_claim = claims[prop][0]['mainsnak']

        if first_claim['snaktype'] == 'novalue':
            return None

        data = first_claim['datavalue']['value']

        return {
            'lat': data['latitude'],
            'lon': data['longitude'],
            'precision': data['precision'],
            'altitude': data['altitude']
        }

def parse_response(response):
    try:
        wikidata = {}

        data = json.loads(response.body_as_unicode())
        entity = next(iter(data['entities']))

        if entity == '-1':
            return None

        entity_data = data['entities'][entity]
        wikidata['id'] = entity_data['id']

        if 'aliases' in entity_data and len(entity_data['aliases']) > 0:
            wikidata['aliases'] = {lang: [alias['value'] for alias in aliases] for lang, aliases in entity_data['aliases'].items()}

        for prop in entity_data['claims']:
            if prop not in LABELS:
                continue

            claim_data = collect_claims(entity_data['claims'], prop)

            if claim_data and len(claim_data) > 0:
                wikidata[LABELS[prop]] = claim_data

        if len(wikidata) < 1:
            return None

        return wikidata
    except json.decoder.JSONDecodeError:
        return None


class LocationSpider(scrapy.Spider):
    name = 'wikidata'
    handle_httpstatus_list = [404]

    def start_requests(self):
        self.cursor = collection.find(QUERY, {'lang': 1, 'name': 1}, no_cursor_timeout=True)

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
