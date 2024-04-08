from sqlalchemy import create_engine

from file_manager import *


def insert_table(engine, df, table_name):
    with engine.begin() as conn:
        df.to_sql(
            name=table_name,
            con=conn,
            if_exists='replace',
            index=False,
        )


db_url = "mysql+mysqlconnector://{USER}:{PWD}@{HOST}/{DBNAME}"
db_url = db_url.format(
    USER="root",
    PWD="root",
    HOST="localhost:3306",
    DBNAME="boardgameDB"
)

engine = create_engine(db_url, echo=False)

insert_table(engine, read_data("GAMES"), "GAMES")
insert_table(engine, read_data("DEMAND"), "DEMAND")
insert_table(engine, read_data("RATINGS"), "RATINGS")
insert_table(engine, read_data('ARTISTS'), 'ARTISTS')
insert_table(engine, read_data('DESIGNERS'), 'DESIGNERS')
insert_table(engine, read_data('PUBLISHERS'), 'PUBLISHERS')
insert_table(engine, read_data('THEMES'), 'THEMES')
insert_table(engine, read_data('MECHANICS'), 'MECHANICS')
insert_table(engine, read_data('SUBCATEGORIES'), 'SUBCATEGORIES')
