import pandas as pd
from pymongo import MongoClient
import json

client = MongoClient("mongodb://user:pass@localhost:27017")
db = client.get_database('ztbd_db')

## TODO

class Mongomanager(client, db):

    def select(num_rows):
        pass
        ## pobrać num_rows z tabeli
        ## zmierzyć czas modułem time
        ## retur execution_time

    def avg():
        pass

    def me():
        pass

    def word_count():
        pass

    
