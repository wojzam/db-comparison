from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

from query_utils import *
from sql_tables import *


class SqlQuery(Query):
    def __init__(self, limit=DEFAULT_LIMIT):
        self.engine = create_engine(self.get_db_url())
        self.limit = limit

    @abstractmethod
    def get_db_url(self) -> str:
        pass

    def execute_select(self, query):
        with sessionmaker(bind=self.engine)() as session:
            statement = query(session).limit(self.limit).statement
            return pd.read_sql(statement, session.bind)

    def list_games(self):
        return self.execute_select(lambda s: s.query(Games))

    def list_games_names(self):
        return self.execute_select(lambda s: s.query(Games.Name))

    def list_singleplayer_games_with_ratings(self):
        return self.execute_select(lambda s: s.query(Games, Ratings).join(Ratings).where(Games.MinPlayers == 1))

    def list_artists_names_sorted(self):
        return self.execute_select(lambda s: s.query(Artists.Name).order_by(Artists.Name))

    def list_demand_with_game_name(self):
        return self.execute_select(lambda s: s.query(Demand, Games.Name).join(Games))

    def list_games_with_artists(self):
        return self.execute_select(
            lambda s: s.query(Games.Name, func.group_concat(Artists.Name).label('Artists')).select_from(Games).join(
                GamesArtists).join(Artists).group_by(Games.Name))

    def list_games_with_artists_publishers_designers(self):
        return self.execute_select(
            lambda s: s.query(
                Games.Name,
                func.group_concat(Artists.Name).label('Artists'),
                func.group_concat(Publishers.Name).label('Publishers'),
                func.group_concat(Designers.Name).label('Designers')
            ).select_from(Games)
            .outerjoin(GamesArtists).outerjoin(Artists)
            .outerjoin(GamesPublishers).outerjoin(Publishers)
            .outerjoin(GamesDesigners).outerjoin(Designers)
            .group_by(Games.Name)
        )

    def list_games_with_specific_theme_and_mechanic(self):
        return self.execute_select(
            lambda s: s.query(
                Games.Name,
                func.group_concat(Themes.Name).label('Themes'),
                func.group_concat(Mechanics.Name).label('Mechanics')
            ).select_from(Games)
            .outerjoin(GamesThemes).outerjoin(Themes)
            .outerjoin(GamesMechanics).outerjoin(Mechanics)
            .filter(Themes.Name.ilike('%Science Fiction%'))
            .filter(Mechanics.Name.ilike('%Cooperative Game%'))
            .group_by(Games.Name)
        )

    def _create_users(self, users):
        with sessionmaker(bind=self.engine)() as session:
            for _, row in users.head(self.limit).iterrows():
                session.add(Users(Username=row['Username']))
            session.commit()

    def _update_users(self):
        with sessionmaker(bind=self.engine)() as session:
            session.query(Users).update({Users.Username: Users.Username + "0"})
            session.commit()

    def _delete_users(self):
        with sessionmaker(bind=self.engine)() as session:
            session.query(Users).delete()
            session.commit()


class MySqlQuery(SqlQuery):
    def get_db_url(self):
        db_url = "mysql+mysqlconnector://{USER}:{PWD}@{HOST}/{DBNAME}"
        return db_url.format(
            USER="root",
            PWD="root",
            HOST="localhost:3306",
            DBNAME="boardgameDB"
        )


class SqliteQuery(SqlQuery):
    def get_db_url(self):
        return 'sqlite:///BoardGames.db'
