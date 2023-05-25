## TODO
from cassandra.cluster import Cluster
import time
import uuid

class CassandraManager:

    table_name = None

    def __init__(self):
        self.cluster = Cluster(["0.0.0.0"], port = 9042)
        keyspace = 'mubi'
        self.session = self.cluster.connect(keyspace)

    def select(self, rows_number=None):
        query = f"SELECT * FROM {self.table_name}"
        if rows_number is not None:
            query = query + "limit " + str(rows_number)
        return self.execute(query)

    def _generate_insert_query(self, rows_number=None):
        values = [(uuid.uuid4().int & 0xFFFFFFFF, uuid.uuid4().int & 0xFFFFFFFF,
                    'http://mubi.com/films/pavee-lackeen-the-traveller-girl/ratings/15610495', 3.0,
                    '2017-06-10 12:38:33', 'Content', 0, 0, uuid.uuid4().int & 0xFFFFFFFF, False, False, True, False)
                   for _ in range(rows_number)]
        query = "INSERT INTO mubi_ratings_data (movie_id, rating_id, rating_url, rating_score, " \
                "rating_timestamp_utc, critic, critic_likes, critic_comments, user_id, user_trialist, " \
                "user_subscriber, user_eligible_for_trial, user_has_payment_method) " \
                f"VALUES ({values})"

    def insert(self, rows_number=None):


    def execute(self, query):
        start_time = time.time()
        self.session.execute(query)
        return time.time() - start_time()

    def delete(self):
        delete_query = f"DELETE FROM {self.table_name}"

        prepared_query = session.prepare(delete_query)
        self.session.execute(prepared_query, (column1_value,)
        return

