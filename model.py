from analysis import result_timed, varied_limits_results, show_time_comparison_plot
from file_manager import *
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
QUERIES = sorted([q for q in dir(Query) if callable(getattr(Query, q)) and not q.startswith("_")], key=len)
QUERIES_LABELS = [q.replace("_", " ").upper() for q in QUERIES]
QUERIES_DICT = dict(zip(QUERIES_LABELS, QUERIES))


class Model:
    databases = {MYSQL: MySqlQuery(), SQLITE: SqliteQuery(), MONGODB: MongoDbQuery(), REDIS: RedisQuery()}
    import_functions = {MYSQL: import_mysql, SQLITE: import_sqlite, MONGODB: import_mongodb, REDIS: import_redis}
    users_cache = pd.DataFrame()

    def execute_query(self, db_label, query_label):
        db = self.databases[db_label]
        query = QUERIES_DICT[query_label]
        return result_timed(getattr(db, query))

    def benchmark_query(self, query_label, max_rows, iterations):
        query = QUERIES_DICT[query_label]
        results = {name: varied_limits_results(db, getattr(db, query), max_rows=max_rows, iterations=iterations)
                   for name, db in self.databases.items()}
        show_time_comparison_plot(query_label, results)

    def benchmark_create(self, max_rows, iterations):
        users = self._generate_users(max_rows)
        results = {name: varied_limits_results(db,
                                               func=lambda: db._create_users(users),
                                               func_after=lambda: db._delete_users(),
                                               max_rows=max_rows,
                                               iterations=iterations)
                   for name, db in self.databases.items()}
        show_time_comparison_plot("Create", results)

    def benchmark_update(self, max_rows, iterations):
        users = self._generate_users(max_rows)
        results = {name: varied_limits_results(db,
                                               func_before=lambda: db._create_users(users),
                                               func=lambda: db._update_users(),
                                               func_after=lambda: db._delete_users(),
                                               max_rows=max_rows,
                                               iterations=iterations)
                   for name, db in self.databases.items()}
        show_time_comparison_plot("Update", results)

    def benchmark_delete(self, max_rows, iterations):
        users = self._generate_users(max_rows)
        results = {name: varied_limits_results(db,
                                               func_before=lambda: db._create_users(users),
                                               func=lambda: db._delete_users(),
                                               max_rows=max_rows,
                                               iterations=iterations)
                   for name, db in self.databases.items()}
        show_time_comparison_plot("Delete", results)

    def _generate_users(self, count):
        if self.users_cache.size < count:
            users = read_data("USERS", max_rows=count)
            self.users_cache = users.sample(n=count, replace=True)
        return self.users_cache.head(count)

    @staticmethod
    def transform_data(source_directory):
        transform(source_directory)

    def import_data(self, db_label):
        self.import_functions[db_label]()
