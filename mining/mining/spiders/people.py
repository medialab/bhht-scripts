import scrapy
import logging
from pymongo import MongoClient
from config import MONGODB

client = MongoClient(MONGODB['host'], MONGODB['port'])
db = client.bhht
collection = db.people


def wikipedia_url(lang, name):
    return u'https://%s.wikipedia.org/wiki/%s' % (lang, name)


class PeopleSpider(scrapy.Spider):
    name = 'people'

    def start_requests(self):
        for doc in collection.find({'done': False}, limit=10 * 1000):
            url = wikipedia_url(doc['lang'], doc['name'])

            logging.info(url)

            yield scrapy.Request(
                url=url,
                meta={
                    'name': doc['name'],
                    'lang': doc['lang']
                },
                callback=self.parse
            )

    def parse(self, response):
        yield {
            'model': 'people',
            'name': response.meta['name'],
            'lang': response.meta['lang'],
            'html': response.body
        }
