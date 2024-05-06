from pymongo import MongoClient

from file_manager import *
from utils_nosql import get_games_with_embedded_data


def insert_collection(df, name):
    collection = db[name]
    collection.drop()
    df.rename(columns={df.columns[0]: "_id"}, inplace=True)
    collection.insert_many(df.to_dict(orient='records'))
    print(f"Imported {name}")


client = MongoClient('mongodb://localhost:27017/')
db = client['boardgameDB']

games = get_games_with_embedded_data()

insert_collection(games, 'games')
insert_collection(read_data('ARTISTS'), 'artists')
insert_collection(read_data('DESIGNERS'), 'designers')
insert_collection(read_data('PUBLISHERS'), 'publishers')
insert_collection(read_data('THEMES'), 'themes')
insert_collection(read_data('MECHANICS'), 'mechanics')
insert_collection(read_data('SUBCATEGORIES'), 'subcategories')
# insert_collection(read_data('USER_RATINGS'), 'userRatings')
