import json
from abc import ABC, abstractmethod

import pandas as pd

DEFAULT_LIMIT = 100


class Query(ABC):

    @abstractmethod
    def list_games(self):
        pass

    @abstractmethod
    def list_games_names_sorted(self):
        pass

    @abstractmethod
    def list_artists_names_sorted(self):
        pass

    @abstractmethod
    def list_coop_games_names_sorted_by_playtime(self):
        pass

    @abstractmethod
    def list_games_names_with_themes_chronologically(self):
        pass

    @abstractmethod
    def list_singleplayer_games_names_with_ratings_and_demand(self):
        pass

    @abstractmethod
    def list_games_names_with_artists_publishers_designers(self):
        pass

    @abstractmethod
    def list_games_with_all_details(self):
        pass

    @abstractmethod
    def create_users(self, users):
        pass

    @abstractmethod
    def update_users(self):
        pass

    @abstractmethod
    def delete_users(self):
        pass


def extract_names(df, columns: list):
    for column in columns:
        df[column] = df[column].apply(lambda values: [x['Name'] for x in json.loads(values)])
    return df


def split_dict_column(df, column):
    df[column] = df[column].apply(json.loads)
    normalized_df = pd.json_normalize(df[column])
    return df.drop(columns=[column]).join(normalized_df)
