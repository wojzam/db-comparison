from pymongo import MongoClient

from query_utils import *


class MongoDbQuery(Query):
    def __init__(self, limit=DEFAULT_LIMIT):
        client = MongoClient('mongodb://localhost:27017/')
        self.db = client['boardgameDB']
        self.limit = limit

    def get_all(self, collection, *args):
        return pd.DataFrame(self.db[collection].find(*args).limit(self.limit))

    def get_all_sorted(self, collection, query, fields, sort_field):
        return pd.DataFrame(self.db[collection].find(query, fields).sort(sort_field).limit(self.limit))

    def list_games(self):
        return self.get_all('games')

    def list_games_names_sorted(self):
        return self.get_all_sorted('games', {}, {'Name': 1, '_id': 0}, "Name")

    def list_artists_names_sorted(self):
        return self.get_all_sorted('artists', {}, {'Name': 1, '_id': 0}, "Name")

    def list_coop_games_names_sorted_by_playtime(self):
        result = self.get_all_sorted('games',
                                     {"Mechanics": {"$regex": 'Cooperative'}},
                                     {'Name': 1, '_id': 0, 'Mechanics': 1},
                                     "MfgPlaytime")
        return extract_names(result, ['Mechanics'])

    def list_games_names_with_themes_chronologically(self):
        result = self.get_all_sorted('games', {}, {'Name': 1, 'Themes': 1, '_id': 0}, "YearPublished")
        return extract_names(result, ["Themes"])

    def list_singleplayer_games_names_with_ratings_and_demand(self):
        result = self.get_all('games', {'MinPlayers': 1},
                              {'Name': 1, 'Ratings': 1, 'Demand': 1, '_id': 0})
        result = split_dict_column(result, "Ratings")
        result = split_dict_column(result, "Demand")
        return result

    def list_games_names_with_artists_publishers_designers(self):
        result = self.get_all('games', {}, {'Name': 1, '_id': 0, "Artists": 1, "Publishers": 1, "Designers": 1})
        return extract_names(result, ['Artists', 'Publishers', 'Designers'])

    def list_games_with_all_details(self):
        result = self.get_all('games', {}, None)
        result = split_dict_column(result, "Ratings")
        result = split_dict_column(result, "Demand")
        return extract_names(result, ['Artists', 'Publishers', 'Designers', 'Themes', 'Mechanics', 'Subcategories'])

    def create_users(self, users):
        collection = self.db["users"]
        collection.insert_many(users.to_dict(orient='records'))

    def update_users(self):
        collection = self.db["users"]
        collection.update_many({}, {"$set": {'Username': "$Username" + "0"}})

    def delete_users(self):
        collection = self.db["users"]
        collection.delete_many({})
