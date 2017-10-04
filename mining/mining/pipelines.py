# -*- coding: utf-8 -*-
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

        # TODO: update done
        collection.update_one(
            {'_id': hasher(item['lang'], item['name'])},
            {'$set': {'html': Binary(item['html'].encode('zip'))}}
        )

        return item
