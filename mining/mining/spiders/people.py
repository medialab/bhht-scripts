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
collection = db.people

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


class PeopleSpider(scrapy.Spider):
    name = 'people'
    handle_httpstatus_list = [404]

    def start_requests(self):
        self.cursor = collection.find(QUERY, limit=1000 * 1000, no_cursor_timeout=True)

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
        cursor.close()

    def closed(self, reason):

        if self.cursor:
            logging.info('Closing MongoDB cusor.')
            self.cursor.close()

    def parse(self, response):
        logging.info(u'%i, %s' % (response.status, response.url))

        if response.status == 404:
            yield {
                'model': 'people',
                'name': response.meta['name'],
                'lang': response.meta['lang'],
                'notFound': True
            }
        else:
            yield {
                'model': 'people',
                'name': response.meta['name'],
                'lang': response.meta['lang'],
                'html': response.body,
                'notFound': False
            }
