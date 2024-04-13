from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database

from file_manager import *
from mysql_tables import *


def convert_value(value, dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return int(value)
    elif pd.api.types.is_float_dtype(dtype):
        return float(value)
    else:
        return value


def insert_df_to_table(df, table):
    with Session() as session:
        for _, row in df.iterrows():
            instance_data = {col: convert_value(row[col], df[col]) for col in df.columns}
            session.add(table(**instance_data))
        session.commit()
        print(f"Imported {table.__tablename__}")


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

Session = sessionmaker(bind=engine)

insert_df_to_table(read_data('GAMES'), Games)
insert_df_to_table(read_data('DEMAND'), Demand)
insert_df_to_table(read_data('RATINGS'), Ratings)

insert_df_to_table(read_data('ARTISTS'), Artists)
insert_df_to_table(read_data('DESIGNERS'), Designers)
insert_df_to_table(read_data('PUBLISHERS'), Publishers)
insert_df_to_table(read_data('THEMES'), Themes)
insert_df_to_table(read_data('MECHANICS'), Mechanics)
insert_df_to_table(read_data('SUBCATEGORIES'), Subcategories)

insert_df_to_table(read_data('GAMES_ARTISTS'), GamesArtists)
insert_df_to_table(read_data('GAMES_DESIGNERS'), GamesDesigners)
insert_df_to_table(read_data('GAMES_PUBLISHERS'), GamesPublishers)
insert_df_to_table(read_data('GAMES_MECHANICS'), GamesMechanics)
insert_df_to_table(read_data('GAMES_SUBCATEGORIES'), GamesSubcategories)
