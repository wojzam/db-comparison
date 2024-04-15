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


def insert_table(engine, df, table):
    with sessionmaker(bind=engine)() as session:
        for _, row in df.iterrows():
            instance_data = {col: convert_value(row[col], df[col]) for col in df.columns}
            session.add(table(**instance_data))
        session.commit()
        print(f"Imported {table.__tablename__}")


def insert_all_tables(engine):
    insert_table(engine, read_data('GAMES'), Games)
    insert_table(engine, read_data('DEMAND'), Demand)
    insert_table(engine, read_data('RATINGS'), Ratings)

    insert_table(engine, read_data('ARTISTS'), Artists)
    insert_table(engine, read_data('DESIGNERS'), Designers)
    insert_table(engine, read_data('PUBLISHERS'), Publishers)
    insert_table(engine, read_data('THEMES'), Themes)
    insert_table(engine, read_data('MECHANICS'), Mechanics)
    insert_table(engine, read_data('SUBCATEGORIES'), Subcategories)

    insert_table(engine, read_data('GAMES_ARTISTS'), GamesArtists)
    insert_table(engine, read_data('GAMES_DESIGNERS'), GamesDesigners)
    insert_table(engine, read_data('GAMES_PUBLISHERS'), GamesPublishers)
    insert_table(engine, read_data('GAMES_MECHANICS'), GamesMechanics)
    insert_table(engine, read_data('GAMES_SUBCATEGORIES'), GamesSubcategories)
