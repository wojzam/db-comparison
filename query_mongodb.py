from pymongo import MongoClient

from query_utils import *


class MongoDbQuery(Query):
    def __init__(self, limit=DEFAULT_LIMIT):
        client = MongoClient('mongodb://localhost:27017/')
        self.db = client['boardgameDB']
        self.limit = limit

    def get_all(self, collection, *args):
        games = self.db[collection]
        return pd.DataFrame(games.find(*args).limit(self.limit))

    def list_games(self):
        return self.get_all('games')

    def list_games_names(self):
        return self.get_all('games', {}, {'Name': 1, '_id': 0})

    def list_singleplayer_games_with_ratings(self):
        result = self.get_all('games', {'MinPlayers': 1},
                              {'Name': 1, 'Description': 1, 'YearPublished': 1, 'MinPlayers': 1, 'MaxPlayers': 1,
                               'Ratings': 1, '_id': 0})
        return split_dict_column(result, "Ratings")

    def list_artists_names_sorted(self):
        return pd.DataFrame(self.db['artists']
                            .find({}, {'Name': 1, '_id': 0})
                            .sort("Name")
                            .limit(self.limit))

    def list_demand_with_game_name(self):
        result = self.get_all('games', {}, {'Demand': 1, 'Name': 1, '_id': 0})
        return split_dict_column(result, "Demand")

    def list_games_with_artists(self):
        games = self.get_all('games', {}, {'Name': 1, '_id': 0, "Artists": 1})
        return extract_names(games, ['Artists'])

    def list_games_with_artists_publishers_designers(self):
        games = self.get_all('games', {}, {'Name': 1, '_id': 0, "Artists": 1, "Publishers": 1, "Designers": 1})
        return extract_names(games, ['Artists', 'Publishers', 'Designers'])

    def list_games_with_specific_theme_and_mechanic(self):
        games = self.get_all('games',
                             {"Themes": {"$regex": 'Science Fiction'}, "Mechanics": {"$regex": 'Cooperative'}},
                             {'Name': 1, '_id': 0, "Themes": 1, "Mechanics": 1})

        return extract_names(games, ['Themes', 'Mechanics'])

    def create_users(self, users):
        collection = self.db["users"]
        collection.insert_many(users.to_dict(orient='records'))

    def update_users(self):
        collection = self.db["users"]
        collection.update_many({}, {"$set": {'Username': "$Username" + "0"}})

    def delete_users(self):
        collection = self.db["users"]
        collection.delete_many({})
