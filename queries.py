from abc import ABC, abstractmethod

import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from sql_tables import Games, Artists

LIMIT = 100


class Query(ABC):

    @abstractmethod
    def select_from_games(self):
        pass

    @abstractmethod
    def select_from_artists(self):
        pass


class SqlQuery(Query):
    def __init__(self):
        self.engine = create_engine(self.get_db_url())

    @abstractmethod
    def get_db_url(self) -> str:
        pass

    def select_from_games(self):
        with sessionmaker(bind=self.engine)() as session:
            query = session.query(Games).limit(LIMIT).statement
            return pd.read_sql(query, session.bind)

    def select_from_artists(self):
        with sessionmaker(bind=self.engine)() as session:
            query = session.query(Artists).limit(LIMIT).statement
            return pd.read_sql(query, session.bind)


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

    def select_from_games(self):
        games = self.db['games']
        return pd.DataFrame(list(games.find().limit(LIMIT)))

    def select_from_artists(self):
        artists = self.db['artists']
        return pd.DataFrame(list(artists.find().limit(LIMIT)))


class RedisQuery(Query):
    def select_from_games(self):
        return pd.DataFrame()

    def select_from_artists(self):
        return pd.DataFrame()
