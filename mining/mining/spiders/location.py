import os
import scrapy
import logging
from pymongo import MongoClient
from config import MONGODB, PROXY

if PROXY:
    os.environ['http_proxy'] = 'http://' + PROXY
    os.environ['https_proxy'] = 'https://' + PROXY

client = MongoClient(MONGODB['host'], MONGODB['port'])
db = client.bhht
collection = db.location

QUERY = {
    '$and': [
        {'done': False},
        {
            '$or': [
                {'notFound': {'$exists': False}},
                {'notFound': False}
            ]
        }
    ]
}


def wikipedia_url(lang, name):
    return u'https://%s.wikipedia.org/wiki/%s' % (lang, name)


class LocationSpider(scrapy.Spider):
    name = 'location'
    handle_httpstatus_list = [400, 404]

    def start_requests(self):
        self.cursor = collection.find(QUERY, {'html': 0}, limit=1000 * 1000, no_cursor_timeout=True)

        try:
            for doc in self.cursor:
                url = wikipedia_url(doc['lang'], doc['name'])

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
        logging.info(u'%i, %s' % (response.status, response.url))

        if response.status == 404:
            yield {
                'model': 'location',
                'name': response.meta['name'],
                'lang': response.meta['lang'],
                'notFound': True,
                'badRequest': False
            }
        elif response.status == 400:
            yield {
                'model': 'location',
                'name': response.meta['name'],
                'lang': response.meta['lang'],
                'notFound': False,
                'badRequest': True
            }
        else:
            yield {
                'model': 'location',
                'name': response.meta['name'],
                'lang': response.meta['lang'],
                'html': response.body,
                'notFound': False,
                'badRequest': False
            }
