import json
from abc import ABC, abstractmethod

import pandas as pd

DEFAULT_LIMIT = 100


class Query(ABC):

    @abstractmethod
    def list_games(self):
        pass

    @abstractmethod
    def list_games_names(self):
        pass

    @abstractmethod
    def list_singleplayer_games_with_ratings(self):
        pass

    @abstractmethod
    def list_artists_names_sorted(self):
        pass

    @abstractmethod
    def list_demand_with_game_name(self):
        pass

    @abstractmethod
    def list_games_with_artists(self):
        pass

    @abstractmethod
    def list_games_with_artists_publishers_designers(self):
        pass

    @abstractmethod
    def list_games_with_specific_theme_and_mechanic(self):
        pass

    @abstractmethod
    def _create_users(self, users):
        pass

    @abstractmethod
    def _update_users(self):
        pass

    @abstractmethod
    def _delete_users(self):
        pass


def extract_names(df, columns: list):
    for column in columns:
        df[column] = df[column].apply(lambda values: [x['Name'] for x in json.loads(values)])
    return df


def split_dict_column(df, column):
    df[column] = df[column].apply(json.loads)
    normalized_df = pd.json_normalize(df[column])
    return df.drop(columns=[column]).join(normalized_df)
