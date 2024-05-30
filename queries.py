import itertools
import json
from abc import ABC, abstractmethod

import pandas as pd
import redis
from pymongo import MongoClient
from redis.commands.search.query import Query as RQuery, NumericFilter
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

from import_redis import create_index, Path
from sql_tables import *
from utils_nosql import serialize

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

    def list_artists_names_sorted(self):
        return self.execute_select(lambda s: s.query(Artists.Name).order_by(Artists.Name))

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

    def _create_users(self, users):
        collection = self.db["users"]
        collection.insert_many(users.to_dict(orient='records'))

    def _update_users(self):
        collection = self.db["users"]
        collection.update_many({}, {"$set": {'Username': "$Username" + "0"}})

    def _delete_users(self):
        collection = self.db["users"]
        collection.delete_many({})


class RedisQuery(Query):
    def __init__(self, limit=DEFAULT_LIMIT):
        self.r = redis.Redis(host='localhost', port=6379, db=1, decode_responses=True)
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
        result = self.get_all('game:*', ['Name', 'Ratings'])  # TODO add filter
        return split_dict_column(result, "Ratings")

    def list_artists_names_sorted(self):
        pipe = self.r.pipeline()

        for key in self.r.scan_iter('artist:*', count=800):
            pipe.hmget(key, ['Name'])

        data = pd.DataFrame(pipe.execute())
        data.columns = ['Name']

        return data.sort_values(by='Name').head(self.limit)

    def list_demand_with_game_name(self):
        result = self.get_all('game:*', ['Name', 'Demand'])
        return split_dict_column(result, "Demand")

    def list_games_with_artists(self):
        games = self.get_all('game:*', ['Name', 'Artists'])
        return extract_names(games, ['Artists'])

    def list_games_with_artists_publishers_designers(self):
        games = self.get_all("game:*", ['Name', 'Artists', 'Publishers', 'Designers'])
        return extract_names(games, ['Artists', 'Publishers', 'Designers'])

    def list_games_with_specific_theme_and_mechanic(self):
        games = self.get_all("game:*", ['Name', 'Themes', 'Mechanics'])  # TODO add filter
        return extract_names(games, ['Themes', 'Mechanics'])

    def _create_users(self, users):
        for index, row in users.iterrows():
            mapping = {key: serialize(value) for key, value in row.to_dict().items()}
            self.r.hset(f"user:{index}", mapping=mapping)

    def _update_users(self):
        pass
        # TODO

    def _delete_users(self):
        self.r.hdel('user:*', 'Username')


class RedisStackQuery(Query):
    def __init__(self, limit=DEFAULT_LIMIT):
        self.r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        self.limit = limit

    def get_all(self, name, query=RQuery("*")):
        docs = self.r.ft(f"idx:{name}").search(query.paging(0, self.limit)).docs
        return pd.DataFrame([json.loads(line['json']) for line in docs])

    def get_fields(self, name, query=RQuery("*"), fields=None):
        docs = self.r.ft(f"idx:{name}").search(query.return_fields(*fields).paging(0, self.limit)).docs
        data = pd.DataFrame([[line[field] for field in fields] for line in docs])
        data.columns = fields
        return data

    def list_games(self):
        return self.get_all("game")

    def list_games_names(self):
        return self.get_fields("game", fields=["Name"])

    def list_singleplayer_games_with_ratings(self):
        result = self.get_fields("game", RQuery("*").add_filter(NumericFilter("MaxPlayers", 1, 1)),
                                 fields=['Name', 'Description', 'YearPublished', 'MinPlayers', 'MaxPlayers', 'Ratings'])
        return split_dict_column(result, "Ratings")

    def list_artists_names_sorted(self):
        return self.get_all("artist", RQuery("*").sort_by("Name"))

    def list_demand_with_game_name(self):
        result = self.get_fields("game", fields=["Name", "Demand"])
        return split_dict_column(result, "Demand")

    def list_games_with_artists(self):
        games = self.get_fields("game", fields=["Name", "Artists"])
        return extract_names(games, ['Artists'])

    def list_games_with_artists_publishers_designers(self):
        games = self.get_fields("game", fields=['Name', 'Artists', 'Publishers', 'Designers'])
        return extract_names(games, ['Artists', 'Publishers', 'Designers'])

    def list_games_with_specific_theme_and_mechanic(self):
        games = self.get_fields("game",
                                RQuery("@Themes:*Science Fiction* @Mechanics:*Cooperative Game*"),
                                fields=['Name', 'Themes', 'Mechanics'])
        return extract_names(games, ['Themes', 'Mechanics'])

    def _create_users(self, users):
        create_index(users, 'user', self.r)

        for index, row in users.iterrows():
            mapping = {key: serialize(value) for key, value in row.to_dict().items()}
            self.r.json().set(f"user:{index}", Path.root_path(), mapping)

    def _update_users(self):
        docs = self.r.ft(f"idx:user").search(RQuery("*").paging(0, self.limit)).docs
        for line in docs:
            self.r.json().set(line['id'], Path.root_path(), {"Username": "Jan"})

    def _delete_users(self):
        docs = self.r.ft(f"idx:user").search(RQuery("*").paging(0, self.limit)).docs
        for line in docs:
            self.r.json().delete(line['id'])


def extract_names(df, columns: list):
    for column in columns:
        df[column] = df[column].apply(lambda values: [x['Name'] for x in json.loads(values)])
    return df


def split_dict_column(df, column):
    df[column] = df[column].apply(json.loads)
    normalized_df = pd.json_normalize(df[column])
    return df.drop(columns=[column]).join(normalized_df)
