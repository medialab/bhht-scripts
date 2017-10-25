# -*- coding: utf-8 -*-
import zlib
from config import MONGODB
from pymongo import MongoClient

try:
    from pymongo.binary import Binary
except:
    from bson.binary import Binary

client = MongoClient(MONGODB['host'], MONGODB['port'])
db = client.bhtt


hasher = lambda lang, name: u'%s§%s' % (lang, name)


class MongoPipeline(object):
    def __init__(self):
        self.client = MongoClient(MONGODB['host'], MONGODB['port'])
        self.db = self.client.bhht

        self.collections = {
            'people': self.db.people,
            'location': self.db.location
        }

    def process_item(self, item, spider):
        collection = self.collections[item['model']]

        _id = hasher(item['lang'], item['name'])

        # Wikidata
        if 'type' in item and item['type'] == 'wikidata':

            collection.update_one(
                {'_id': _id},
                {'$set': {'wikidata': item['wikidata']}}
            )

            return item

        # Not Found
        if item['notFound']:
            collection.update_one(
                {'_id': _id},
                {'$set': {'notFound': True, 'done': True}}
            )

            return item

        # Bad Request
        if item['badRequest']:
            collection.update_one(
                {'_id': _id},
                {'$set': {'badRequest': True, 'done': True}}
            )

            return item

        # HTML
        collection.update_one(
            {'_id': _id},
            {'$set': {'html': Binary(zlib.compress(item['html'])), 'done': True}}
        )

        return item
