import scrapy
import logging
from pymongo import MongoClient
from config import MONGODB

client = MongoClient(MONGODB['host'], MONGODB['port'])
db = client.bhht
collection = db.people

QUERY = {
    '$and': [
        {'done': False},
        {
            '$or': [
                {'notFound': {'$exists': False}},
                {'notFound': True}
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
        for doc in collection.find(QUERY, limit=10 * 1000):
            url = wikipedia_url(doc['lang'], doc['name'])

            yield scrapy.Request(
                url=url,
                meta={
                    'name': doc['name'],
                    'lang': doc['lang']
                },
                callback=self.parse
            )

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
