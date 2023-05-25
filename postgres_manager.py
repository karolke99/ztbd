
import psycopg2
import time
import uuid


class PostgresManager:

    _conn = None

    def __init__(self):
        self._conn = psycopg2.connect(
            host='localhost',
            port='5432',
            database='postgres',
            user='postgres',
            password='postgres'
        )

    def select(self, rows_number=None):
        query = "SELECT * FROM mubi_ratings_data "

        if rows_number is not None:
            query = query + "limit " + str(rows_number)

        return self._execute(query)

    def insert(self, rows_number):
        new_row = [(uuid.uuid4().int & 0xFFFFFFFF, uuid.uuid4().int & 0xFFFFFFFF,
                    'http://mubi.com/films/pavee-lackeen-the-traveller-girl/ratings/15610495', 3.0,
                    '2017-06-10 12:38:33', 'Content', 0, 0, uuid.uuid4().int & 0xFFFFFFFF, False, False, True, False)
                   for _ in range(rows_number)]
        insert_query = "INSERT INTO mubi_ratings_data (movie_id, rating_id, rating_url, rating_score, rating_timestamp_utc, critic, critic_likes, critic_comments, user_id, user_trialist, user_subscriber, user_eligible_for_trial, user_has_payment_method) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        return self._execute_many(insert_query, new_row)

    def update(self, rows_number):
        query = f"UPDATE mubi_ratings_data SET rating_url = NULL WHERE user_id IN ( SELECT user_id FROM mubi_ratings_data LIMIT {rows_number});"

        return self._execute(query)

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
        query = "SELECT COUNT(word) AS word_count FROM ( SELECT regexp_split_to_table(critic, E'\\s+') AS word FROM mubi_ratings_data) subquery;"
        return self._execute(query)

    def _execute_many(self, query, rows):
        cursor = self._conn.cursor()

        start_time = time.time()
        cursor.executemany(query, rows)
        self._conn.commit()
        end_time = time.time()

        cursor.close()
        return end_time - start_time

    def _execute(self, query):
        cursor = self._conn.cursor()

        start_time = time.time()
        cursor.execute(query)
        self._conn.commit()
        end_time = time.time()

        cursor.close()
        return end_time - start_time

    def __del__(self):
        self._conn.close()
