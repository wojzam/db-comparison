from analysis import result_timed, show_time_comparison_plot
from queries import MySqlQuery, SqliteQuery, MongoDbQuery, RedisQuery, Query

MYSQL = "MySql"
SQLITE = "Sqlite"
MONGODB = "MongoDb"
REDIS = "Redis"

DATABASES_LABELS = [MYSQL, SQLITE, MONGODB, REDIS]
QUERIES = sorted([q for q in dir(Query) if callable(getattr(Query, q)) and not q.startswith("__")], key=len)
QUERIES_LABELS = [q.replace("_", " ").upper() for q in QUERIES]
QUERIES_DICT = dict(zip(QUERIES_LABELS, QUERIES))


class Model:
    databases = {MYSQL: MySqlQuery(), SQLITE: SqliteQuery(), MONGODB: MongoDbQuery(), REDIS: RedisQuery()}

    def execute_query(self, db_label, query_label):
        db = self.databases[db_label]
        query = QUERIES_DICT[query_label]
        return result_timed(getattr(db, query))

    def benchmark_query(self, query_label):
        query = QUERIES_DICT[query_label]
        results = {name: result_timed(getattr(db, query), 20)[1:] for name, db in self.databases.items()}
        show_time_comparison_plot(query_label, results)
