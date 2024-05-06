import itertools
from abc import ABC, abstractmethod

import pandas as pd
import redis
from pymongo import MongoClient
from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker

from sql_tables import Games, Artists, Demand, GamesArtists, Ratings

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
    def list_artists_names(self):
        pass

    @abstractmethod
    def list_demand_with_game_name(self):
        pass

    @abstractmethod
    def list_game_artists(self):
        pass

    @abstractmethod
    def list_games_with_possible_zero_players(self):
        pass

    @abstractmethod
    def delete_games_with_possible_zero_players(self):
        pass

    @abstractmethod
    def list_games_from_year_2020(self):
        pass
    
    @abstractmethod
    def change_date_2020_to_2021(self):
        pass

class SqlQuery(Query):
    def __init__(self, limit=DEFAULT_LIMIT):
        self.engine = create_engine(self.get_db_url())
        self.limit = limit

    @abstractmethod
    def get_db_url(self) -> str:
        pass

    def execute_query(self, query):
        with sessionmaker(bind=self.engine)() as session:
            statement = query(session).limit(self.limit).statement
            return pd.read_sql(statement, session.bind)

    def list_games(self):
        return self.execute_query(lambda s: s.query(Games))

    def list_games_names(self):
        return self.execute_query(lambda s: s.query(Games.Name))
    
    def list_singleplayer_games_with_ratings(self):
        return self.execute_query(lambda s: s.query(Games, Ratings).join(Ratings).where(Games.MinPlayers==1))

    def list_artists_names(self):
        return self.execute_query(lambda s: s.query(Artists.Name))

    def list_demand_with_game_name(self):
        return self.execute_query(lambda s: s.query(Demand, Games.Name).join(Games))

    def list_game_artists(self):
        return self.execute_query(
            lambda s: s.query(Games.Name, func.group_concat(Artists.Name).label('Artists')).select_from(Games).join(
                GamesArtists).join(Artists).group_by(Games.Name))
    
    def list_games_with_possible_zero_players(self):
        return self.execute_query(lambda s: s.query(Games).where(Games.MinPlayers==0))
    
    #TODO
    def delete_games_with_possible_zero_players(self):
        pass

    def list_games_from_year_2020(self):
        pass
    
    def change_date_2020_to_2021(self):
        pass


class MySqlQuery(SqlQuery):
    def get_db_url(self):
        db_url = "mysql+mysqlconnector://{USER}:{PWD}@{HOST}/{DBNAME}"
        return db_url.format(
            USER="root",
            PWD="root",
            HOST="localhost:3306",
            DBNAME="boardgameDB"
        )


class SqliteQuery(SqlQuery):
    def get_db_url(self):
        return 'sqlite:///BoardGames.db'


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
        return self.get_all('games', {'MinPlayers': 1}, {'Name': 1, 'Description': 1, 'YearPublished': 1, 'MinPlayers': 1, 'MaxPlayers': 1, 'Ratings': 1,'_id': 0})
    
    def list_artists_names(self):
        return self.get_all('artists', {}, {'Name': 1, '_id': 0})

    def list_demand_with_game_name(self):
        return self.get_all('games', {}, {'Demand': 1, 'Name': 1, '_id': 0})
    
    def list_games_with_possible_zero_players(self):
        return self.get_all('games', {'MinPlayers': 0})
    
    def delete_games_with_possible_zero_players(self):
        self.db['games'].delete_many({'MinPlayers': 0}) #TODO delete reference

    def list_games_from_year_2020(self):
        return self.get_all('games', {'YearPublished': 2020})
    
    def change_date_2020_to_2021(self):
        self.db['games'].update_many({'YearPublished': 2020}, {"$set":{'YearPublished': 20}})
    
    def delete_games_with_possible_zero_players(self):
        self.db['games'].delete_many({'MinPlayers': 0})
        #TODO delete reference

    #TODO
    def list_game_artists(self):
        pass


class RedisQuery(Query):
    def __init__(self, limit=DEFAULT_LIMIT):
        self.r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        self.limit = limit

    def get_all(self, match, fields=None):
        pipe = self.r.pipeline()

        for key in itertools.islice(self.r.scan_iter(match, count=max(1, self.limit // 2)), self.limit):
            if fields:
                pipe.hmget(key, fields)
            else:
                pipe.hgetall(key)

        data = pd.DataFrame(pipe.execute())
        if fields:
            data.columns = fields
        return data

    def list_games(self):
        return self.get_all('game:*')

    def list_games_names(self):
        return self.get_all('game:*', ['Name'])
    
    def list_singleplayer_games_with_ratings(self):
        return self.get_all('game:*', ['Name', 'Ratings']) #TODO

    def list_artists_names(self):
        return self.get_all('artist:*', ['Name'])

    def list_demand_with_game_name(self):
        return self.get_all('game:*', ['Name', 'Demand'])

    def list_game_artists(self):
        games = self.get_all("game:*", ['Name', 'ArtistIds'])
        games['Artists'] = games['ArtistIds'].apply(self.get_artist_names)
        games.drop(columns=['ArtistIds'], inplace=True)

        return games

    def get_artist_names(self, ids):
        id_list = ids[1:-1].split(",")
        return [self.r.hget(f"artist:{int(float(x))}", "Name") if x != 'NaN' else "" for x in id_list]
    
    #TODO
    def list_games_with_possible_zero_players(self):
        pass
    
    def delete_games_with_possible_zero_players(self):
        pass

    def list_games_from_year_2020(self):
        pass
    
    def change_date_2020_to_2021(self):
        pass
