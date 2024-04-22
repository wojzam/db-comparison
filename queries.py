from abc import ABC, abstractmethod

import pandas as pd
import redis
from pymongo import MongoClient
from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker

from sql_tables import Games, Artists, Demand, GamesArtists

LIMIT = 1000


class Query(ABC):

    @abstractmethod
    def list_games(self):
        pass

    @abstractmethod
    def list_games_names(self):
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


class SqlQuery(Query):
    def __init__(self):
        self.engine = create_engine(self.get_db_url(), echo=True)

    @abstractmethod
    def get_db_url(self) -> str:
        pass

    def execute_query(self, query):
        with sessionmaker(bind=self.engine)() as session:
            statement = query(session).limit(LIMIT).statement
            return pd.read_sql(statement, session.bind)

    def list_games(self):
        return self.execute_query(lambda s: s.query(Games))

    def list_games_names(self):
        return self.execute_query(lambda s: s.query(Games.Name))

    def list_artists_names(self):
        return self.execute_query(lambda s: s.query(Artists.Name))

    def list_demand_with_game_name(self):
        return self.execute_query(lambda s: s.query(Demand, Games.Name).join(Games))

    def list_game_artists(self):
        return self.execute_query(
            lambda s: s.query(Games.Name, func.group_concat(Artists.Name).label('Artists')).select_from(Games).join(
                GamesArtists).join(Artists).group_by(Games.Name))


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
    def __init__(self):
        client = MongoClient('mongodb://localhost:27017/')
        self.db = client['boardgameDB']

    def get_all(self, collection, *args):
        games = self.db[collection]
        return pd.DataFrame(games.find(*args).limit(LIMIT))

    def list_games(self):
        return self.get_all('games')

    def list_games_names(self):
        return self.get_all('games', {}, {'Name': 1, '_id': 0})

    def list_artists_names(self):
        return self.get_all('artists', {}, {'Name': 1, '_id': 0})

    def list_demand_with_game_name(self):
        return self.get_all('games', {}, {'Demand': 1, 'Name': 1, '_id': 0})

    def list_game_artists(self):
        pass


class RedisQuery(Query):
    def __init__(self):
        self.r = redis.Redis(host='localhost', port=6379, db=0)

    def get_all(self, match, fields=None):
        data = []
        for i, key in enumerate(self.r.scan_iter(match)):
            if i >= LIMIT:
                break
            if fields:
                data.append({field: self.r.hget(key, field) for field in fields})
            else:
                data.append(self.r.hgetall(key))
        return pd.DataFrame(data)

    def list_games(self):
        return self.get_all('game:*')

    def list_games_names(self):
        return self.get_all('game:*', ['Name'])

    def list_artists_names(self):
        return self.get_all('artist:*', ['Name'])

    def list_demand_with_game_name(self):
        return self.get_all('game:*', ['Name', 'Demand'])

    def list_game_artists(self):
        pass
