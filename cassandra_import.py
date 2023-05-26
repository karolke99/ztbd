from cassandra.cluster import Cluster
import csv
import pandas as pd

cluster = Cluster(['localhost'], port = 9042)
keyspace_name = 'ztbd'

session = cluster.connect()
session.set_keyspace(keyspace_name)

# CREATE TABLE lists_data (
#     user_id int,
#     list_id int,
#     list_title text,
#     list_movie_number text,
#     list_update_timestamp_utc text,
#     list_creation_timestamp_utc text,
#     list_followers text,
#     list_url text,
#     list_comments text,
#     list_description text,
#     list_cover_image_url text,
#     list_first_image_url text,
#     list_second_image_url text,
#     list_third_image_url text,
#     PRIMARY KEY (list_id)
# )

# CREATE TABLE lists_user_data (
#     user_id text,
#     list_id text,
#     list_update_date_utc text,
#     list_creation_date_utc text,
#     user_trialist text,
#     user_subscriber text,
#     user_avatar_image_url text,
#     user_cover_image_url text,
#     user_eligible_for_trial text,
#     user_has_payment_method text,
#     PRIMARY KEY(user_id)
# )

# CREATE TABLE movie_data(
#     movie_id text,
#     movie_title text,
#     movie_release_year text,
#     movie_url text,
#     movie_title_language text,
#     movie_popularity text,
#     movie_image_url text,
#     director_id text,
#     director_name text,
#     director_url text,
#     PRIMARY KEY(movie_id)
# )

# CREATE TABLE ratings_data (
#     movie_id text,
#     rating_id text,
#     rating_url text,
#     rating_score text,
#     rating_timestamp_utc text,
#     critic text,
#     critic_likes text,
#     critic_comments text,
#     user_id text,
#     user_trialist text,
#     user_subscriber text,
#     user_eligible_for_trial text,
#     user_has_payment_method text,
#     PRIMARY KEY(rating_id)
# )

# CREATE TABLE ratings_user_data (
#     user_id text,
#     rating_date_utc text,
#     user_trialist text,
#     user_subscriber text,
#     user_avatar_image_url text,
#     user_cover_image_url text,
#     user_eligible_for_trial text,
#     user_has_payment_method text,
#     PRIMARY KEY(user_id)
# )

with open('archive/mubi_lists_data.csv') as csvfile:
    csvreader = csv.reader(csvfile)

    next(csvreader)

    for row in csvreader:
        session.execute(
            f"""
            INSERT INTO lists_data (user_id, 
                list_id,
                list_title, 
                list_movie_number, 
                list_update_timestamp_utc, 
                list_creation_timestamp_utc, 
                list_followers, 
                list_url,
                list_comments,
                list_description,
                list_cover_image_url,
                list_first_image_url,
                list_second_image_url,
                list_third_image_url
                )
            VALUES ({int(row[0])} ,{int(row[1])} , %s, %s, %s ,%s , %s, %s, %s ,%s , %s, %s, %s, %s)
            """,
            (row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13])
        )

with open('archive/mubi_lists_user_data.csv') as csvfile:
    csvreader = csv.reader(csvfile)

    next(csvreader)

    for row in csvreader:
        session.execute(
            """
            INSERT INTO lists_user_data (
                user_id,
                list_id,
                list_update_date_utc,
                list_creation_date_utc,
                user_trialist,
                user_subscriber,
                user_avatar_image_url,
                user_cover_image_url,
                user_eligible_for_trial,
                user_has_payment_method
                )
            VALUES (%s ,%s , %s, %s, %s ,%s , %s, %s, %s ,%s)
            """,
            (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9])
        )

with open('archive/mubi_movie_data.csv') as csvfile:
    csvreader = csv.reader(csvfile)

    next(csvreader)

    for row in csvreader:
        session.execute(
            """
            INSERT INTO movie_data (
                movie_id,
                movie_title,
                movie_release_year,
                movie_url,
                movie_title_language,
                movie_popularity,
                movie_image_url,
                director_id,
                director_name,
                director_url
                )
            VALUES (%s ,%s , %s, %s, %s ,%s , %s, %s, %s ,%s)
            """,
            (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9])
        )

with open('archive/mubi_ratings_data.csv') as csvfile:
    csvreader = csv.reader(csvfile)

    next(csvreader)

    for row in csvreader:
        session.execute(
            """
            INSERT INTO ratings_data (
                movie_id,
                rating_id,
                rating_url,
                rating_score,
                rating_timestamp_utc,
                critic,
                critic_likes,
                critic_comments,
                user_id,
                user_trialist,
                user_subscriber,
                user_eligible_for_trial,
                user_has_payment_method
                )
            VALUES (%s ,%s , %s, %s, %s ,%s , %s, %s, %s , %s, %s, %s, %s)
            """,
            (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12])
        )

with open('archive/mubi_ratings_user_data.csv') as csvfile:
    csvreader = csv.reader(csvfile)

    next(csvreader)

    for row in csvreader:
        session.execute(
            """
            INSERT INTO ratings_user_data (
                user_id,
                rating_date_utc,
                user_trialist,
                user_subscriber,
                user_avatar_image_url,
                user_cover_image_url,
                user_eligible_for_trial,
                user_has_payment_method
                )
            VALUES (%s ,%s , %s, %s, %s ,%s , %s, %s)
            """,
            (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7])
        )

session.shutdown()
cluster.shutdown()