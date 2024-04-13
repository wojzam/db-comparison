from file_manager import *


def embed_one_to_one(main_table, sub_table, column_name, join_column='BGGId'):
    sub_dict = sub_table.set_index(join_column).to_dict(orient='index')
    main_table[column_name] = main_table[join_column].apply(lambda x: sub_dict.get(x, {}))


def embed_many_to_many(main_table, assoc_table, column_name):
    main_id, foreign_id = assoc_table.columns.values.tolist()

    merged_data = pd.merge(main_table, assoc_table, on=main_id, how='left')
    grouped_data_dict = merged_data.groupby(main_id)[foreign_id].apply(list).to_dict()

    main_table[column_name] = main_table[main_id].map(grouped_data_dict)


def get_games_with_embedded_data():
    games = read_data('GAMES')

    embed_one_to_one(games, read_data('DEMAND'), 'Demand')
    embed_one_to_one(games, read_data('RATINGS'), 'Ratings')
    embed_many_to_many(games, read_data('GAMES_ARTISTS'), 'ArtistIds')
    embed_many_to_many(games, read_data('GAMES_DESIGNERS'), 'DesignerIds')
    embed_many_to_many(games, read_data('GAMES_PUBLISHERS'), 'PublisherIds')
    embed_many_to_many(games, read_data('GAMES_THEMES'), 'ThemeIds')
    embed_many_to_many(games, read_data('GAMES_MECHANICS'), 'MechanicIds')
    embed_many_to_many(games, read_data('GAMES_SUBCATEGORIES'), 'SubcategoryIds')
    return games
