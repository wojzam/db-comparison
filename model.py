from analysis import result_timed, varied_limits_results, show_time_comparison_plot
from import_mongodb import import_mongodb
from import_mysql import import_mysql
from import_redis import import_redis
from import_sqlite import import_sqlite
from queries import MySqlQuery, SqliteQuery, MongoDbQuery, RedisQuery, Query
from transform_data import transform

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
    import_functions = {MYSQL: import_mysql, SQLITE: import_sqlite, MONGODB: import_mongodb, REDIS: import_redis}

    def execute_query(self, db_label, query_label):
        db = self.databases[db_label]
        query = QUERIES_DICT[query_label]
        return result_timed(getattr(db, query))

    def benchmark_query(self, query_label, max_rows, test_iterations):
        query = QUERIES_DICT[query_label]
        results = {name: varied_limits_results(db, query, max_rows, test_iterations)
                   for name, db in self.databases.items()}
        show_time_comparison_plot(query_label, results)

    @staticmethod
    def transform_data(source_directory):
        transform(source_directory)

    def import_data(self, db_label):
        self.import_functions[db_label]()
