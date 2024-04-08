from pymongo import MongoClient

from file_manager import *


def insert_collection(db, df, collection_name):
    collection = db[collection_name]
    df.rename(columns={df.columns[0]: "_id"}, inplace=True)
    collection.insert_many(df.to_dict(orient='records'))
    print(f"Imported {collection_name}")


def embed_one_to_one(main_table, sub_table, column_name, join_column='BGGId'):
    sub_dict = sub_table.set_index(join_column).to_dict(orient='index')
    main_table[column_name] = main_table[join_column].apply(lambda x: sub_dict.get(x, {}))


def embed_many_to_many(main_table, assoc_table, column_name):
    main_id, foreign_id = assoc_table.columns.values.tolist()

    merged_data = pd.merge(main_table, assoc_table, on=main_id, how='left')
    grouped_data_dict = merged_data.groupby(main_id)[foreign_id].apply(list).to_dict()

    main_table[column_name] = main_table[main_id].map(grouped_data_dict)


client = MongoClient('mongodb://localhost:27017/')
db = client['boardgameDB']

games = read_data('GAMES')

embed_one_to_one(games, read_data('DEMAND'), 'demand')
embed_one_to_one(games, read_data('RATINGS'), 'ratings')
embed_many_to_many(games, read_data('GAMES_ARTISTS'), 'artistIds')
embed_many_to_many(games, read_data('GAMES_DESIGNERS'), 'designerIds')
embed_many_to_many(games, read_data('GAMES_PUBLISHERS'), 'publisherIds')
embed_many_to_many(games, read_data('GAMES_THEMES'), 'themeIds')
embed_many_to_many(games, read_data('GAMES_MECHANICS'), 'mechanicIds')
embed_many_to_many(games, read_data('GAMES_SUBCATEGORIES'), 'subcategoryIds')

insert_collection(db, games, 'games')
insert_collection(db, read_data('ARTISTS'), 'artists')
insert_collection(db, read_data('DESIGNERS'), 'designers')
insert_collection(db, read_data('PUBLISHERS'), 'publishers')
insert_collection(db, read_data('THEMES'), 'themes')
insert_collection(db, read_data('MECHANICS'), 'mechanics')
insert_collection(db, read_data('SUBCATEGORIES'), 'subcategories')
# insert_collection(db, read_data('USER_RATINGS'), 'userRatings')
