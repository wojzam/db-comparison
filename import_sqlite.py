import sqlite3

from sqlalchemy import create_engine

from sql_tables import *
from utils_sql import insert_all_tables

connection = sqlite3.connect("BoardGames.db")

engine = create_engine('sqlite:///BoardGames.db', echo=True)

Base.metadata.create_all(engine, checkfirst=True)

insert_all_tables(engine)
