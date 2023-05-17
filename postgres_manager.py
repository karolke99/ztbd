
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

    def select(self, rows_number = None):
        
        query = "SELECT * FROM mubi_ratings_user_data "

        if rows_number != None:
            query = query + "limit " + str(rows_number)
        
        return self._execute(query)
    
    def insert(self, rows_number):

        new_row =[(uuid.uuid4().int & 0xFFFFFFFF, None, None, None, None, None, None, None) for _ in range (rows_number)]
        insert_query = "INSERT INTO mubi_ratings_user_data (user_id,rating_date_utc,user_trialist,user_subscriber,user_avatar_image_url,user_cover_image_url,user_eligible_for_trial,user_has_payment_method) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

        return self._execute_many(insert_query, new_row)
    
    def update(self, rows_number):

        query = f"UPDATE mubi_ratings_user_data SET user_cover_image_url = NULL WHERE user_id IN ( SELECT user_id FROM mubi_ratings_user_data LIMIT {rows_number});"

        return self._execute(query)
    

    def delete(self, rows_number):

        query = f"DELETE from mubi_ratings_user_data WHERE user_id IN ( SELECT user_id FROM mubi_ratings_user_data LIMIT {rows_number});"

        return self._execute(query)
    
    def avg(self):
        query = "SELECT AVG(user_id) FROM mubi_ratings_user_data;"
        return self._execute(query)
    
    def median(self):
        query = 'SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY user_id) AS median FROM mubi_ratings_user_data;'
        return self._execute(query)
    
    def count(self):
        query = 'SELECT count(*) FROM mubi_ratings_user_data'
        return self._execute(query)
    
    def min(self):
        query = "SELECT min(user_id) FROM mubi_ratings_user_data;"
        return self._execute(query)
    
    def max(self):
        query = "SELECT max(user_id) FROM mubi_ratings_user_data;"
        return self._execute(query)
    
    def count_by_word(self):
        query = "SELECT count(*) FROM mubi_ratings_user_data where user_cover_image_url = '%assets%';"
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

