from pymongo import MongoClient
import time
import uuid

# client = MongoClient("mongodb://user:pass@localhost:27017")
# db = client.get_database('ztbd_db')

class MongoManager:
    _client = None

    def __init__(self):
        self._client = MongoClient("mongodb://user:pass@localhost:27017")
        self._db = self._client.get_database('ztbd_db')
        self._collection = self._db['ratings_data']

    def select(self, rows_number=None):

        if rows_number is not None:
            query = f'self._collection.find().limit({rows_number})'
        else:
            query = 'self._collection.find()'

        return self._execute(query)

    def insert(self, rows_number=1):
        document = {
            'movie_id': uuid.uuid4().int & 0xFFFFFFFF,
            'rating_id': uuid.uuid4().int & 0xFFFFFFFF,
            'rating_url': 'http://mubi.com/films/pavee-lackeen-the-traveller-girl/ratings/15610495',
            'rating_score': 3.0,
            'rating_timestamp_utc': '2017-06-10 12:38:33',
            'critic': 'Some comment',
            'critic_likes': 0,
            'critic_comments': 0,
            'user_id': uuid.uuid4().int & 0xFFFFFFFF,
            'user_trialist': False,
            'user_subscriber': False,
            'user_eligible_for_trial': True,
            'user_has_payment_method': False
        }

        documents = [document] * rows_number
        query = f'self._collection.insert_many({documents})'
        print(query)
        return self._execute(query)

    def update(self, rows_number=1):
        user_ids = [doc['user_id'] for doc in self._collection.find().limit(rows_number)]
        query = 'self._collection.update_many({"user_id": {"$in": %s}},{"$set": {"rating_score": 5.0}})' % user_ids
        return self._execute(query)

    def delete(self, rows_number=1):
        user_ids = [doc['user_id'] for doc in self._collection.find().limit(rows_number)]
        query = 'self._collection.delete_many({"user_id": {"$in": %s}})' % user_ids
        return self._execute(query)

    def avg(self):
        pipeline = '[{"$group": {"_id": None, "avg": {"$avg": "$rating_score"}}}]'
        query = f'self._collection.aggregate({pipeline})'
        # average_value = result[0]['avg']
        return self._execute(query)

    def median(self):
        '''
        There is no easy way to calculate median in MongoDB. There is a trick instead.
        '''
        query = 'self._collection.find().sort("rating_score", 1).skip(int(self._collection.count_documents({}) / 2 - 1)).limit(1)'
        return self._execute(query)

    def count(self):
        query = 'self._collection.count_documents({})'
        return self._execute(query)

    def min(self):
        query = 'self._collection.find_one({}, {"ratings_score": 1}, sort=[("ratings_score", 1)])'
        return self._execute(query)

    def max(self):
        query = 'self._collection.find_one({}, {"ratings_score": -1}, sort=[("ratings_score", 1)])'
        return self._execute(query)

    def count_by_word(self):
        pipeline = '''[
            {"$project": {"words": {"$split": ["$critic", " "]}}},
            {"$unwind": "$words"},
            {"$group": {"_id": "$words", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]'''

        query = f'self._collection.aggregate({pipeline})'
        return self._execute(query)

    def _execute(self, query):
        start_time = time.time()
        eval(query)
        end_time = time.time()
        # self._client.close()
        return end_time - start_time

    def __del__(self):
        self._client.close()


manager = MongoManager()
print(manager.count_by_word())
