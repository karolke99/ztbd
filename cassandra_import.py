## TODO
from cassandra.cluster import Cluster
import csv
import pandas as pd

cluster = Cluster(['localhost'], port = 9042)

keyspace_name = 'mubi'
replication_options = {
    'class': 'SimpleStrategy',
    'replication_factor': 3
}
query = f"CREATE KEYSPACE IF NOT EXISTS {keyspace_name} WITH REPLICATION = {replication_options}"

session = cluster.connect()
session.execute(query)
session.set_keyspace(keyspace_name)

table_name = 'ratings'
# insert_query = f"INSERT INTO {table_name} (movie_id, movie_title, movie_release_year, movie_url, movie_title_language,"\
#                f" movie_popularity, movie_image_url, director_id, director_name, director_url) " \
#                f"VALUES (?,?,?,?,?,?,?,?,?,?)"

insert_query = f"INSERT INTO {table_name} (movie_id, rating_id, rating_url, raitng_score, rating_timestamp_utc, critic," \
               f" critic_likes, critic_comments, user_id, user_trialist, user_subscriber, user_eligible_for_trial," \
               f" user_has_payment_method) " \
               f"VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"

df = pd.read_csv('./data/mubi_ratings_data.csv')
data = df.values.tolist()
session.execute(insert_query, data)
...

























session.execute("CREATE KEYSPACE IF NOT EXISTS my_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}")
session.execute("USE my_keyspace")
session.execute("CREATE TABLE IF NOT EXISTS my_table (column1 datatype1, column2 datatype2, ...)")

with open('path/to/your/csv_file.csv', 'r') as file:
    csv_data = csv.reader(file)
    next(csv_data)  # Skip the header row if present
    for row in csv_data:
        query = "INSERT INTO my_table (column1, column2, ...) VALUES (%s, %s, ...)"
        session.execute(query, row)

session.shutdown()
cluster.shutdown()