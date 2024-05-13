import json

import redis

from file_manager import *
from utils_nosql import get_games_with_embedded_data


def serialize(value):
    return json.dumps(value) if isinstance(value, (list, dict)) else value


def insert_set(r, df, name):
    df.set_index(df.columns[0], inplace=True)
    for index, row in df.iterrows():
        mapping = {key: serialize(value) for key, value in row.to_dict().items()}
        r.hset(f"{name}:{index}", mapping=mapping)
    print(f"Imported {name}")


def import_redis():
    r = redis.Redis(host='localhost', port=6379, db=0)

    games = get_games_with_embedded_data()

    insert_set(r, games, 'game')
    insert_set(r, read_data('ARTISTS'), 'artist')
    insert_set(r, read_data('DESIGNERS'), 'designer')
    insert_set(r, read_data('PUBLISHERS'), 'publisher')
    insert_set(r, read_data('THEMES'), 'theme')
    insert_set(r, read_data('MECHANICS'), 'mechanic')
    insert_set(r, read_data('SUBCATEGORIES'), 'subcategory')


if __name__ == "__main__":
    import_redis()
