from file_manager import *

BGG_ID = 'BGGId'


def columns_to_rows(filename, new_filename, main_column_name, id_column_name='id'):
    old_table = read_data(filename, SOURCE_DIRECTORY)
    old_columns = old_table.columns.values.tolist()
    old_columns.remove(BGG_ID)

    new_table = pd.DataFrame(old_columns, columns=[main_column_name])
    new_table[id_column_name] = new_table.reset_index(drop=True).index + 1

    new_columns = new_table.columns.values.tolist()
    new_columns = new_columns[-1:] + new_columns[:-1]

    save_to_file(new_table[new_columns], new_filename)
    create_relation_table(old_table, new_filename, id_column_name)


def create_relation_table(old_table, new_filename, id_column_name):
    game_id = old_table[BGG_ID]
    matrix = old_table.drop(columns=[BGG_ID]).to_numpy()
    relations = []
    for i in range(matrix.shape[0]):
        for j in range(matrix.shape[1]):
            if matrix[i, j] == 1:
                relations.append([game_id[i], j + 1])
    save_to_file(pd.DataFrame(relations, columns=[BGG_ID, id_column_name]), "GAMES_" + new_filename)


games_table = read_data("games", SOURCE_DIRECTORY)

extract_ratings = ['BGGId', 'GameWeight', 'AvgRating', 'BayesAvgRating', 'StdDev', 'NumUserRatings', 'NumComments']
save_to_file(games_table[extract_ratings], "RATINGS")

extract_demand = ['BGGId', 'NumOwned', 'NumWant', 'NumWish']
save_to_file(games_table[extract_demand], "DEMAND")

to_drop = ['GameWeight', 'AvgRating', 'BayesAvgRating', 'StdDev', 'NumUserRatings', 'NumComments', 'NumOwned',
           'NumWant', 'NumWish', 'BestPlayers', 'GoodPlayers', 'NumWeightVotes', 'Family', 'Kickstarted', 'ImagePath',
           'Rank:boardgame', 'Rank:strategygames', 'Rank:abstracts', 'Rank:familygames', 'Rank:thematic', 'Rank:cgs',
           'Rank:wargames', 'Rank:partygames', 'Rank:childrensgames', 'Cat:Thematic', 'Cat:Strategy', 'Cat:War',
           'Cat:Family', 'Cat:CGS', 'Cat:Abstract', 'Cat:Party', 'Cat:Childrens']
games_table.drop(to_drop, inplace=True, axis=1)

games_table['Description'].fillna('', inplace=True)
games_table['ComAgeRec'].fillna(0, inplace=True)
games_table['LanguageEase'].fillna(0, inplace=True)

save_to_file(games_table, "GAMES")

columns_to_rows('artists_reduced', 'ARTISTS', 'Name', 'ArtistId')
columns_to_rows('designers_reduced', 'DESIGNERS', 'Name', 'DesignerId')
columns_to_rows('publishers_reduced', 'PUBLISHERS', 'Name', 'PublisherId')
columns_to_rows('themes', 'THEMES', 'Name', 'ThemeId')
columns_to_rows('mechanics', 'MECHANICS', 'Name', 'MechanicId')
columns_to_rows('subcategories', 'SUBCATEGORIES', 'Name', 'SubcategoryId')

os.system(f'copy {SOURCE_DIRECTORY}\\user_ratings.csv {DATA_DIRECTORY}\\USER_RATINGS.csv')
