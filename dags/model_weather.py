import uuid
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, String, Integer, Float, DateTime, Date


class   Connection(object):

    def __init__(self, db_connection):
        engine = create_engine(db_connection)
        self.engine = engine

    def get_session(self):
        Session = sessionmaker(bind=self.engine)

        return Session()

    def get_engine(self):
        return self.engine


Base = declarative_base()


def init_db(db_connection):
    engine = create_engine(db_connection)
    Base.metadata.create_all(bind=engine)





class Weather(Base):
    __tablename__ = 'weather'

    time = Column(Integer)
    date_time = Column(DateTime , primary_key=True)
    weathervalue = Column(String)
    precipmm = Column(Float)
    def __init__(self,  date_time, time, weathervalue,precipmm):
        self.weathervalue = weathervalue
        self.date_time = date_time
        self.time = time
        self.precipmm=precipmm
