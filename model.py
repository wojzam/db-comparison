from analysis import result_timed, varied_limits_results, show_time_comparison_plot
from file_manager import *
from import_mongodb import import_mongodb
from import_mysql import import_mysql
from import_redis import import_redis
from import_sqlite import import_sqlite
from query_mongodb import MongoDbQuery
from query_redis import RedisQuery, RedisStackQuery
from query_sql import MySqlQuery, SqliteQuery
from query_utils import Query
from transform_data import transform

MYSQL = "MySql"
SQLITE = "Sqlite"
MONGODB = "MongoDb"
REDIS = "Redis"
REDIS_STACK = "Redis Stack"

DATABASES_LABELS = [MYSQL, SQLITE, MONGODB, REDIS, REDIS_STACK]
IMPORT_FUNC = {MYSQL: import_mysql, SQLITE: import_sqlite, MONGODB: import_mongodb, REDIS: import_redis}
QUERIES = sorted([q for q in dir(Query) if callable(getattr(Query, q)) and not q.startswith("_")], key=len)
QUERIES_LABELS = [q.replace("_", " ").upper() for q in QUERIES]
QUERIES_DICT = dict(zip(QUERIES_LABELS, QUERIES))


class Model:
    databases = {MYSQL: MySqlQuery(), SQLITE: SqliteQuery(), MONGODB: MongoDbQuery(), REDIS: RedisQuery(),
                 REDIS_STACK: RedisStackQuery()}
    users_cache = pd.DataFrame()

    def execute_query(self, db_label, query_label):
        db = self.databases[db_label]
        query = QUERIES_DICT[query_label]
        return result_timed(getattr(db, query))

    def benchmark_query(self, query_label, db_labels, max_rows, iterations, step_count):
        query = QUERIES_DICT[query_label]
        results = {name: varied_limits_results(db,
                                               getattr(db, query),
                                               max_rows=max_rows,
                                               iterations=iterations,
                                               step_count=step_count)
                   for name, db in self._filter_databases(db_labels)}
        show_time_comparison_plot(query_label, results)

    def benchmark_create(self, db_labels, max_rows, iterations, step_count):
        users = self._generate_users(max_rows)
        results = {name: varied_limits_results(db,
                                               func=lambda: db._create_users(users),
                                               func_after=lambda: db._delete_users(),
                                               max_rows=max_rows,
                                               iterations=iterations,
                                               step_count=step_count)
                   for name, db in self._filter_databases(db_labels)}
        show_time_comparison_plot("Create", results)

    def benchmark_update(self, db_labels, max_rows, iterations, step_count):
        users = self._generate_users(max_rows)
        results = {name: varied_limits_results(db,
                                               func_before=lambda: db._create_users(users),
                                               func=lambda: db._update_users(),
                                               func_after=lambda: db._delete_users(),
                                               max_rows=max_rows,
                                               iterations=iterations,
                                               step_count=step_count)
                   for name, db in self._filter_databases(db_labels)}
        show_time_comparison_plot("Update", results)

    def benchmark_delete(self, db_labels, max_rows, iterations, step_count):
        users = self._generate_users(max_rows)
        results = {name: varied_limits_results(db,
                                               func_before=lambda: db._create_users(users),
                                               func=lambda: db._delete_users(),
                                               max_rows=max_rows,
                                               iterations=iterations,
                                               step_count=step_count)
                   for name, db in self._filter_databases(db_labels)}
        show_time_comparison_plot("Delete", results)

    def _generate_users(self, count):
        if self.users_cache.size < count:
            users = read_data("USERS", max_rows=count)
            self.users_cache = users.sample(n=count, replace=True)
        return self.users_cache.head(count)

    def _filter_databases(self, db_labels):
        return [(label, self.databases[label]) for label in db_labels]

    @staticmethod
    def transform_data(source_directory):
        transform(source_directory)

    @staticmethod
    def import_data(db_label):
        IMPORT_FUNC[db_label]()
