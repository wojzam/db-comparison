import sqlite3
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from file_manager import *
from sql_tables import *

def convert_value(value, dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return int(value)
    elif pd.api.types.is_float_dtype(dtype):
        return float(value)
    else:
        return value


def insert_table(df, table):
    with Session() as session:
        for _, row in df.iterrows():
            instance_data = {col: convert_value(row[col], df[col]) for col in df.columns}
            session.add(table(**instance_data))
        session.commit()
        print(f"Imported {table.__tablename__}")

connection = sqlite3.connect("BoardGames.db")

engine = create_engine('sqlite:///BoardGames.db', echo=True)

Base.metadata.create_all(engine, checkfirst=True)

Session = sessionmaker(bind=engine)

insert_table(read_data('GAMES'), Games)
insert_table(read_data('DEMAND'), Demand)
insert_table(read_data('RATINGS'), Ratings)

insert_table(read_data('ARTISTS'), Artists)
insert_table(read_data('DESIGNERS'), Designers)
insert_table(read_data('PUBLISHERS'), Publishers)
insert_table(read_data('THEMES'), Themes)
insert_table(read_data('MECHANICS'), Mechanics)
insert_table(read_data('SUBCATEGORIES'), Subcategories)

insert_table(read_data('GAMES_ARTISTS'), GamesArtists)
insert_table(read_data('GAMES_DESIGNERS'), GamesDesigners)
insert_table(read_data('GAMES_PUBLISHERS'), GamesPublishers)
insert_table(read_data('GAMES_MECHANICS'), GamesMechanics)
insert_table(read_data('GAMES_SUBCATEGORIES'), GamesSubcategories)
