from sqlalchemy import Column, ForeignKey, Integer, Float, String, Text, Boolean
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Games(Base):
    __tablename__ = 'games'
    BGGId = Column(Integer, primary_key=True)
    Name = Column(String(255))
    Description = Column(Text)
    YearPublished = Column(Integer)
    MinPlayers = Column(Integer)
    MaxPlayers = Column(Integer)
    ComAgeRec = Column(Float)
    LanguageEase = Column(Float)
    MfgPlaytime = Column(Integer)
    ComMinPlaytime = Column(Integer)
    ComMaxPlaytime = Column(Integer)
    MfgAgeRec = Column(Integer)
    NumAlternates = Column(Integer)
    NumExpansions = Column(Integer)
    NumImplementations = Column(Integer)
    IsReimplementation = Column(Boolean)

    demand = relationship("Demand", uselist=False, backref="games")
    ratings = relationship("Ratings", uselist=False, backref="games")
    artists = relationship('Artists', secondary='games_artists', back_populates='games')
    designers = relationship('Designers', secondary='games_designers', back_populates='games')
    publishers = relationship('Publishers', secondary='games_publishers', back_populates='games')
    themes = relationship('Themes', secondary='games_themes', back_populates='games')
    subcategories = relationship('Subcategories', secondary='games_subcategories', back_populates='games')
    mechanics = relationship('Mechanics', secondary='games_mechanics', back_populates='games')


class Demand(Base):
    __tablename__ = 'demand'
    BGGId = Column(Integer, ForeignKey('games.BGGId'), primary_key=True)
    NumOwned = Column(Integer)
    NumWant = Column(Integer)
    NumWish = Column(Integer)


class Ratings(Base):
    __tablename__ = 'ratings'
    BGGId = Column(Integer, ForeignKey('games.BGGId'), primary_key=True)
    GameWeight = Column(Integer)
    AvgRating = Column(Float)
    BayesAvgRating = Column(Float)
    StdDev = Column(Float)
    NumUserRatings = Column(Integer)
    NumComments = Column(Integer)


class Artists(Base):
    __tablename__ = 'artists'
    ArtistId = Column(Integer, primary_key=True)
    Name = Column(String(255))
    games = relationship('Games', secondary='games_artists', back_populates='artists')


class Designers(Base):
    __tablename__ = 'designers'
    DesignerId = Column(Integer, primary_key=True)
    Name = Column(String(255))
    games = relationship('Games', secondary='games_designers', back_populates='designers')


class Publishers(Base):
    __tablename__ = 'publishers'
    PublisherId = Column(Integer, primary_key=True)
    Name = Column(String(255))
    games = relationship('Games', secondary='games_publishers', back_populates='publishers')


class Themes(Base):
    __tablename__ = 'themes'
    ThemeId = Column(Integer, primary_key=True)
    Name = Column(String(255))
    games = relationship('Games', secondary='games_themes', back_populates='themes')


class Subcategories(Base):
    __tablename__ = 'subcategories'
    SubcategoryId = Column(Integer, primary_key=True)
    Name = Column(String(255))
    games = relationship('Games', secondary='games_subcategories', back_populates='subcategories')


class Mechanics(Base):
    __tablename__ = 'mechanics'
    MechanicId = Column(Integer, primary_key=True)
    Name = Column(String(255))
    games = relationship('Games', secondary='games_mechanics', back_populates='mechanics')


class GamesArtists(Base):
    __tablename__ = 'games_artists'
    BGGId = Column(Integer, ForeignKey('games.BGGId'), primary_key=True)
    ArtistId = Column(Integer, ForeignKey('artists.ArtistId'), primary_key=True)


class GamesDesigners(Base):
    __tablename__ = 'games_designers'
    BGGId = Column(Integer, ForeignKey('games.BGGId'), primary_key=True)
    DesignerId = Column(Integer, ForeignKey('designers.DesignerId'), primary_key=True)


class GamesPublishers(Base):
    __tablename__ = 'games_publishers'
    BGGId = Column(Integer, ForeignKey('games.BGGId'), primary_key=True)
    PublisherId = Column(Integer, ForeignKey('publishers.PublisherId'), primary_key=True)


class GamesThemes(Base):
    __tablename__ = 'games_themes'
    BGGId = Column(Integer, ForeignKey('games.BGGId'), primary_key=True)
    ThemeId = Column(Integer, ForeignKey('themes.ThemeId'), primary_key=True)


class GamesSubcategories(Base):
    __tablename__ = 'games_subcategories'
    BGGId = Column(Integer, ForeignKey('games.BGGId'), primary_key=True)
    SubcategoryId = Column(Integer, ForeignKey('subcategories.SubcategoryId'), primary_key=True)


class GamesMechanics(Base):
    __tablename__ = 'games_mechanics'
    BGGId = Column(Integer, ForeignKey('games.BGGId'), primary_key=True)
    MechanicId = Column(Integer, ForeignKey('mechanics.MechanicId'), primary_key=True)


class Users(Base):
    __tablename__ = 'users'
    UserId = Column(Integer, primary_key=True, autoincrement=True)
    Username = Column(String(255))
