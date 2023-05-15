import pandas as pd
from pymongo import MongoClient
import json

client = MongoClient("mongodb://user:pass@localhost:27017")
db = client.get_database('ztbd_db')

def mongoimport(csv_path, collection_name):
    collection = db[collection_name]
    data = pd.read_csv(csv_path, sep=',')
    payload = json.loads(data.to_json(orient='records'))
    collection.delete_many({})
    collection.insert_many(payload)
    return collection.count_documents({})


print(f'Importing mubi_lists_data.csv ...')
print(mongoimport('./archive/mubi_lists_data.csv', 'lists_data'))

print('Importing mubi_lists_user_data.csv ...')
print(mongoimport('./archive/mubi_lists_user_data.csv', 'lists_user_data'))

print('Importing mubi_movie_data.csv ...')
print(mongoimport('./archive/mubi_movie_data.csv', 'movie_data'))

print('Importing mubi_ratings_data.csv ...')
print(mongoimport('./archive/mubi_ratings_data.csv', 'ratings_data'))

print('Importing mubi_ratings_user_data.csv ...')
print(mongoimport('./archive/mubi_ratings_user_data.csv', 'ratings_user_data'))
