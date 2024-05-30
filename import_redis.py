import redis
from redis.commands.json.path import Path
from redis.commands.search.field import TextField, NumericField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType

from file_manager import *
from utils_nosql import get_games_with_embedded_data, serialize


def insert(r0, r1, df, name):
    df.set_index(df.columns[0], inplace=True)
    df.fillna("", inplace=True)
    create_index(df, name, r0)

    for index, row in df.iterrows():
        mapping = {key: serialize(value) for key, value in row.to_dict().items()}
        r0.json().set(f"{name}:{index}", Path.root_path(), mapping)
        r1.hset(f"{name}:{index}", mapping=mapping)
    print(f"Imported {name}")


def create_index(df, name, r):
    rs = r.ft(f"idx:{name}")
    remove_existing_index(rs)
    rs.create_index(generate_schema(df), definition=IndexDefinition(prefix=[f"{name}:"], index_type=IndexType.JSON))


def remove_existing_index(rs):
    try:
        rs.dropindex(delete_documents=True)
    except redis.exceptions.ResponseError:
        pass


def generate_schema(df):
    schema = []
    for column in df.columns:
        if df[column].dtype == 'object':
            schema.append(TextField(f"$..{column}", as_name=column))
        elif pd.api.types.is_numeric_dtype(df[column]):
            schema.append(NumericField(f"$..{column}", as_name=column))
    return tuple(schema)


def import_redis():
    r0 = redis.Redis(host='localhost', port=6379, db=0)
    r1 = redis.Redis(host='localhost', port=6379, db=1)

    games = get_games_with_embedded_data()

    insert(r0, r1, games, 'game')
    insert(r0, r1, read_data('ARTISTS'), 'artist')
    insert(r0, r1, read_data('DESIGNERS'), 'designer')
    insert(r0, r1, read_data('PUBLISHERS'), 'publisher')
    insert(r0, r1, read_data('THEMES'), 'theme')
    insert(r0, r1, read_data('MECHANICS'), 'mechanic')
    insert(r0, r1, read_data('SUBCATEGORIES'), 'subcategory')


if __name__ == "__main__":
    import_redis()
