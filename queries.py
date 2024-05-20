import itertools
from abc import ABC, abstractmethod

import pandas as pd
import redis
from pymongo import MongoClient
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

from sql_tables import *

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
    def list_games_with_artists(self):
        pass

    @abstractmethod
    def list_games_with_artists_publishers_designers(self):
        pass

    @abstractmethod
    def list_games_with_specific_theme_and_mechanic(self):
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

    @abstractmethod
    def _create_users(self, users):
        pass

    @abstractmethod
    def _update_users(self):
        pass

    @abstractmethod
    def _delete_users(self):
        pass


class SqlQuery(Query):
    def __init__(self, limit=DEFAULT_LIMIT):
        self.engine = create_engine(self.get_db_url())
        self.limit = limit

    @abstractmethod
    def get_db_url(self) -> str:
        pass

    def execute_select(self, query):
        with sessionmaker(bind=self.engine)() as session:
            statement = query(session).limit(self.limit).statement
            return pd.read_sql(statement, session.bind)

    def list_games(self):
        return self.execute_select(lambda s: s.query(Games))

    def list_games_names(self):
        return self.execute_select(lambda s: s.query(Games.Name))

    def list_singleplayer_games_with_ratings(self):
        return self.execute_select(lambda s: s.query(Games, Ratings).join(Ratings).where(Games.MinPlayers == 1))

    def list_artists_names(self):
        return self.execute_select(lambda s: s.query(Artists.Name))

    def list_demand_with_game_name(self):
        return self.execute_select(lambda s: s.query(Demand, Games.Name).join(Games))

    def list_games_with_artists(self):
        return self.execute_select(
            lambda s: s.query(Games.Name, func.group_concat(Artists.Name).label('Artists')).select_from(Games).join(
                GamesArtists).join(Artists).group_by(Games.Name))

    def list_games_with_artists_publishers_designers(self):
        return self.execute_select(
            lambda s: s.query(
                Games.Name,
                func.group_concat(Artists.Name).label('Artists'),
                func.group_concat(Publishers.Name).label('Publishers'),
                func.group_concat(Designers.Name).label('Designers')
            ).select_from(Games)
            .outerjoin(GamesArtists).outerjoin(Artists)
            .outerjoin(GamesPublishers).outerjoin(Publishers)
            .outerjoin(GamesDesigners).outerjoin(Designers)
            .group_by(Games.Name)
        )

    def list_games_with_specific_theme_and_mechanic(self):
        return self.execute_select(
            lambda s: s.query(
                Games.Name,
                func.group_concat(Themes.Name).label('Themes'),
                func.group_concat(Mechanics.Name).label('Mechanics')
            ).select_from(Games)
            .outerjoin(GamesThemes).outerjoin(Themes)
            .outerjoin(GamesMechanics).outerjoin(Mechanics)
            .filter(Themes.Name.ilike('%Science Fiction%'))
            .filter(Mechanics.Name.ilike('%Cooperative Game%'))
            .group_by(Games.Name)
        )

    def list_games_with_possible_zero_players(self):
        return self.execute_select(lambda s: s.query(Games).where(Games.MinPlayers == 0))

    # TODO
    def delete_games_with_possible_zero_players(self):
        pass

    def list_games_from_year_2020(self):
        pass

    def change_date_2020_to_2021(self):
        pass

    def _create_users(self, users):
        with sessionmaker(bind=self.engine)() as session:
            for _, row in users.head(self.limit).iterrows():
                session.add(Users(Username=row['Username']))
            session.commit()

    def _update_users(self):
        with sessionmaker(bind=self.engine)() as session:
            session.query(Users).update({Users.Username: Users.Username + "0"})
            session.commit()

    def _delete_users(self):
        with sessionmaker(bind=self.engine)() as session:
            session.query(Users).delete()
            session.commit()


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
        return self.get_all('games', {'MinPlayers': 1},
                            {'Name': 1, 'Description': 1, 'YearPublished': 1, 'MinPlayers': 1, 'MaxPlayers': 1,
                             'Ratings': 1, '_id': 0})

    def list_artists_names(self):
        return self.get_all('artists', {}, {'Name': 1, '_id': 0})

    def list_demand_with_game_name(self):
        return self.get_all('games', {}, {'Demand': 1, 'Name': 1, '_id': 0})

    def list_games_with_possible_zero_players(self):
        return self.get_all('games', {'MinPlayers': 0})

    def delete_games_with_possible_zero_players(self):
        self.db['games'].delete_many({'MinPlayers': 0})  # TODO delete reference

    def list_games_from_year_2020(self):
        return self.get_all('games', {'YearPublished': 2020})

    def change_date_2020_to_2021(self):
        self.db['games'].update_many({'YearPublished': 2020}, {"$set": {'YearPublished': 20}})

    def delete_games_with_possible_zero_players(self):
        self.db['games'].delete_many({'MinPlayers': 0})
        # TODO delete reference

    # TODO
    def list_games_with_artists(self):
        pass

    def list_games_with_artists_publishers_designers(self):
        pass

    def list_games_with_specific_theme_and_mechanic(self):
        pass

    def _create_users(self, users):
        pass

    def _update_users(self):
        pass

    def _delete_users(self):
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

    def get_multiple_fields(self, ids, name, key="Name"):
        id_list = ids[1:-1].split(",")
        return [self.r.hget(f"{name}:{int(float(x))}", key) if x != 'NaN' else "" for x in id_list]

    def list_games(self):
        return self.get_all('game:*')

    def list_games_names(self):
        return self.get_all('game:*', ['Name'])

    def list_singleplayer_games_with_ratings(self):
        return self.get_all('game:*', ['Name', 'Ratings'])  # TODO

    def list_artists_names(self):
        return self.get_all('artist:*', ['Name'])

    def list_demand_with_game_name(self):
        return self.get_all('game:*', ['Name', 'Demand'])

    def list_games_with_artists(self):
        games = self.get_all("game:*", ['Name', 'ArtistIds'])
        games['Artists'] = games['ArtistIds'].apply(lambda ids: self.get_multiple_fields(ids, "artist"))
        games.drop(columns=['ArtistIds'], inplace=True)
        return games

    def list_games_with_artists_publishers_designers(self):
        games = self.get_all("game:*", ['Name', 'ArtistIds', 'PublisherIds', 'DesignerIds'])

        games['Artists'] = games['ArtistIds'].apply(lambda ids: self.get_multiple_fields(ids, "artist"))
        games['Publishers'] = games['PublisherIds'].apply(lambda ids: self.get_multiple_fields(ids, "publisher"))
        games['Designers'] = games['DesignerIds'].apply(lambda ids: self.get_multiple_fields(ids, "designer"))

        games.drop(columns=['ArtistIds', 'PublisherIds', 'DesignerIds'], inplace=True)
        return games

    def list_games_with_specific_theme_and_mechanic(self):
        games = self.get_all("game:*", ['Name', 'ThemeIds', 'MechanicIds'])

        games['Themes'] = games['ThemeIds'].apply(lambda ids: self.get_multiple_fields(ids, "theme"))
        games['Mechanics'] = games['MechanicIds'].apply(lambda ids: self.get_multiple_fields(ids, "mechanic"))

        games.drop(columns=['ThemeIds', 'MechanicIds'], inplace=True)
        # TODO add filter
        return games

    # TODO
    def list_games_with_possible_zero_players(self):
        pass

    def delete_games_with_possible_zero_players(self):
        pass

    def list_games_from_year_2020(self):
        pass

    def change_date_2020_to_2021(self):
        pass

    def _create_users(self, users):
        pass

    def _update_users(self):
        pass

    def _delete_users(self):
        pass
