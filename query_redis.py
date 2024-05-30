import itertools

import redis
from redis.commands.search.query import Query as RQuery, NumericFilter

from import_redis import create_index, Path
from query_utils import *
from utils_nosql import serialize


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

    def create_users(self, users):
        for index, row in users.iterrows():
            mapping = {key: serialize(value) for key, value in row.to_dict().items()}
            self.r.hset(f"user:{index}", mapping=mapping)

    def update_users(self):
        pass
        # TODO

    def delete_users(self):
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

    def create_users(self, users):
        create_index(users, 'user', self.r)

        for index, row in users.iterrows():
            mapping = {key: serialize(value) for key, value in row.to_dict().items()}
            self.r.json().set(f"user:{index}", Path.root_path(), mapping)

    def update_users(self):
        docs = self.r.ft(f"idx:user").search(RQuery("*").paging(0, self.limit)).docs
        for line in docs:
            self.r.json().set(line['id'], Path.root_path(), {"Username": "Jan"})

    def delete_users(self):
        docs = self.r.ft(f"idx:user").search(RQuery("*").paging(0, self.limit)).docs
        for line in docs:
            self.r.json().delete(line['id'])
