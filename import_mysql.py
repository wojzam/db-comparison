from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database

from sql_tables import *
from utils_sql import insert_all_tables


def import_mysql():
    db_url = "mysql+mysqlconnector://{USER}:{PWD}@{HOST}/{DBNAME}"
    db_url = db_url.format(
        USER="root",
        PWD="root",
        HOST="localhost:3306",
        DBNAME="boardgameDB"
    )

    engine = create_engine(db_url)
    if not database_exists(engine.url):
        create_database(engine.url)

    Base.metadata.create_all(engine, checkfirst=True)

    insert_all_tables(engine)


if __name__ == "__main__":
    import_mysql()
