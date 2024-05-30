from pymongo import MongoClient

from file_manager import read_data
from utils_nosql import get_games_with_embedded_data, serialize


def insert_collection(db, df, name):
    collection = db[name]
    collection.drop()
    collection.insert_many(transform(df))
    print(f"Imported {name}")


def transform(df):
    df.rename(columns={df.columns[0]: "_id"}, inplace=True)
    data = df.to_dict(orient='records')
    for row in data:
        for key, value in row.items():
            row[key] = serialize(value)
    return data


def import_mongodb():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['boardgameDB']

    games = get_games_with_embedded_data()

    insert_collection(db, games, 'games')
    insert_collection(db, read_data('ARTISTS'), 'artists')
    insert_collection(db, read_data('DESIGNERS'), 'designers')
    insert_collection(db, read_data('PUBLISHERS'), 'publishers')
    insert_collection(db, read_data('THEMES'), 'themes')
    insert_collection(db, read_data('MECHANICS'), 'mechanics')
    insert_collection(db, read_data('SUBCATEGORIES'), 'subcategories')


if __name__ == "__main__":
    import_mongodb()
