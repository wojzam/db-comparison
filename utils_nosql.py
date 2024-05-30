import json

from file_manager import *


def serialize(value):
    return json.dumps(value) if isinstance(value, (list, dict)) else value


def embed_one_to_one(main_table, sub_table, column_name, join_column='BGGId'):
    sub_dict = sub_table.set_index(join_column).to_dict(orient='index')
    main_table[column_name] = main_table[join_column].apply(lambda x: sub_dict.get(x, {}))


def embed_many_to_many(main_table, foreign_table, assoc_table, column_name):
    main_id, foreign_id = assoc_table.columns.values.tolist()

    merged = pd.merge(assoc_table, foreign_table, left_on=foreign_id, right_on=foreign_table.columns[0], how='left')
    grouped_data = merged.groupby(main_id).apply(lambda x: x.drop(columns=[main_id]).to_dict('records')).to_dict()
    main_table[column_name] = main_table[main_id].map(grouped_data)
    main_table[column_name] = main_table[column_name].apply(lambda x: x if isinstance(x, list) else [])


def get_games_with_embedded_data():
    games = read_data('GAMES')

    embed_one_to_one(games, read_data('DEMAND'), 'Demand')
    embed_one_to_one(games, read_data('RATINGS'), 'Ratings')

    embed_many_to_many(games, read_data('ARTISTS'), read_data('GAMES_ARTISTS'), 'Artists')
    embed_many_to_many(games, read_data('DESIGNERS'), read_data('GAMES_DESIGNERS'), 'Designers')
    embed_many_to_many(games, read_data('PUBLISHERS'), read_data('GAMES_PUBLISHERS'), 'Publishers')
    embed_many_to_many(games, read_data('THEMES'), read_data('GAMES_THEMES'), 'Themes')
    embed_many_to_many(games, read_data('MECHANICS'), read_data('GAMES_MECHANICS'), 'Mechanics')
    embed_many_to_many(games, read_data('SUBCATEGORIES'), read_data('GAMES_SUBCATEGORIES'), 'Subcategories')

    return games
