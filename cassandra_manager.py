from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
import time
import uuid


class CassandraManager:
    table_name = None

    def __init__(self):
        self.cluster = Cluster(["0.0.0.0"], port=9042)
        keyspace = 'ztbd'
        self.session = self.cluster.connect(keyspace)

    def select(self, rows_number=None):
        query = f"SELECT * FROM ratings_data"
        if rows_number is not None:
            query = query + " limit " + str(rows_number)

        print(query)
        return self._execute(query)

    def _generate_insert_query(self):
        values = ('12345', '12345',
                  'http://mubi.com/films/pavee-lackeen-the-traveller-girl/ratings/15610495', 3.0,
                  '2017-06-10 12:38:33', 'Content', '0', '0', '123456676', 'False', 'False', 'True', 'False')

        query = "INSERT INTO ratings_data (movie_id, rating_id, rating_url, rating_scoree, " \
                "rating_timestamp_utc, critic, critic_likes, critic_comments, user_id, user_trialist, " \
                "user_subscriber, user_eligible_for_trial, user_has_payment_method) " \
                f"VALUES {values}"

        return self.session.prepare(query)

    def insert(self, rows_number=1):
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        for _ in range(rows_number):
            query = self._generate_insert_query()
            batch.add(query)
        return self._execute(batch)

    def update(self, rows_number=1):
        
        query = f"SELECT rating_id FROM ratings_data LIMIT {rows_number}"
        rows = self.session.execute(query)
        ids = []
        for row in rows:
            ids.append(f'\'{row.rating_id}\'')

        query = f'UPDATE ratings_data SET rating_url = \'someth\' WHERE rating_id IN ' \
                f'({", ".join(ids)});'
        
        return self._execute(query)

    def _execute(self, query):
        start_time = time.time()
        self.session.execute(query)
        return time.time() - start_time

    def delete(self, rows_number=1):
        query = f"SELECT rating_id FROM ratings_data LIMIT {rows_number}"
        rows = self.session.execute(query)
        ids = []
        for row in rows:
            ids.append(f'\'{row.rating_id}\'')
        
        query = f"DELETE from ratings_data WHERE rating_id IN " \
                f'({", ".join(ids)});'
        return self._execute(query)

    def avg(self):
        query = "SELECT AVG(user_id) FROM lists_data;"
        return self._execute(query)

    def median(self):
        # query = 'SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY user_id) AS median FROM lists_data;'
        query = "SELECT AVG(user_id) FROM lists_data;"
        return self._execute(query)

    def count(self):
        query = 'SELECT count(*) FROM lists_data'
        return self._execute(query)

    def min(self):
        query = "SELECT min(user_id) FROM lists_data;"
        return self._execute(query)

    def max(self):
        query = "SELECT max(user_id) FROM lists_data;"
        return self._execute(query)

    # def count_by_word(self):
    #     query = f"SELECT tokenCount(tokenize(critic, ' ')) AS word_count FROM ratings_data;"
    #     return self._execute(query)
