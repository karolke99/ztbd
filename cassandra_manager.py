from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
import time
import uuid


class CassandraManager:
    table_name = None

    def __init__(self):
        self.cluster = Cluster(["0.0.0.0"], port=9042)
        keyspace = 'mubi'
        self.session = self.cluster.connect(keyspace)

    def select(self, rows_number=None):
        query = f"SELECT * FROM {self.table_name}"
        if rows_number is not None:
            query = query + "limit " + str(rows_number)
        return self._execute(query)

    def _generate_insert_query(self):
        values = (uuid.uuid4().int & 0xFFFFFFFF, uuid.uuid4().int & 0xFFFFFFFF,
                  'http://mubi.com/films/pavee-lackeen-the-traveller-girl/ratings/15610495', 3.0,
                  '2017-06-10 12:38:33', 'Content', 0, 0, uuid.uuid4().int & 0xFFFFFFFF, False, False, True, False)

        query = "INSERT INTO mubi_ratings_data (movie_id, rating_id, rating_url, rating_score, " \
                "rating_timestamp_utc, critic, critic_likes, critic_comments, user_id, user_trialist, " \
                "user_subscriber, user_eligible_for_trial, user_has_payment_method) " \
                f"VALUES ({values})"

        return self.session.prepare(query)

    def insert(self, rows_number):
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        for _ in range(rows_number):
            query = self._generate_insert_query()
            batch.add(query)
        return self._execute(batch)

    def update(self, rows_number):
        query = f"UPDATE mubi_ratings_data SET rating_url = NULL WHERE user_id IN " \
                f"( SELECT user_id FROM mubi_ratings_data LIMIT {rows_number});"
        return self._execute(query)

    def _execute(self, query):
        start_time = time.time()
        self.session.execute(query)
        return time.time() - start_time

    def delete(self, rows_number):
        query = f"DELETE from mubi_ratings_data WHERE user_id IN ( SELECT user_id FROM mubi_ratings_data LIMIT {rows_number});"
        return self._execute(query)

    def avg(self):
        query = "SELECT AVG(user_id) FROM mubi_ratings_data;"
        return self._execute(query)

    def median(self):
        query = 'SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY user_id) AS median FROM mubi_ratings_data;'
        return self._execute(query)

    def count(self):
        query = 'SELECT count(*) FROM mubi_ratings_data'
        return self._execute(query)

    def min(self):
        query = "SELECT min(user_id) FROM mubi_ratings_data;"
        return self._execute(query)

    def max(self):
        query = "SELECT max(user_id) FROM mubi_ratings_data;"
        return self._execute(query)

    def count_by_word(self):
        query = f"SELECT tokenCount(tokenize(critics, ' ')) AS word_count FROM mubi_ratings_data;"
        return self._execute(query)
