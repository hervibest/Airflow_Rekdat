import argparse
from pathlib import Path
from model import Connection
import config
import os
import pandas as pd 
from sqlalchemy import  Column, String, Integer, Float, DateTime, Date, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
from datetime import timedelta, datetime

# Initialize Tomtom Table


def get_yesterday_date(fetch_date):
    return datetime.strptime(fetch_date, '%Y-%m-%d').date() - timedelta(1)

def get_file_path(fetch_date):
    yesterday = get_yesterday_date(fetch_date)
    filename = "Traffic S_{}.csv".format(yesterday)
    
    return os.path.join(config.WEATHER_FILE_DIR, filename)
def save_new_data_to_csv(data_to_append, fetch_date):
    filename = get_file_path(fetch_date)

    data_to_append.to_csv(filename, encoding='utf-8')






def get_file_path(fetch_date):
    yesterday = get_yesterday_date(fetch_date)
    filename = "Traffic S_{}.csv".format(yesterday)
    
    return os.path.join(config.WEATHER_FILE_DIR, filename)

def main(fetch_date):
    SQLALCHEMY_DATABASE_URI = 'postgresql://postgres:informatika@mydatabase-instance.csbnsdtoskt5.ap-northeast-1.rds.amazonaws.com/Dietalicious_database'
    engine = create_engine(SQLALCHEMY_DATABASE_URI, pool_pre_ping=True, echo=False)
    db = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
    Base = declarative_base(bind=engine)


    class Currency(Base):
        """The `Currency`-table"""
        __tablename__ = "tomtom"


    
        timestamp = Column(Integer, primary_key=True)
        date_time = Column(DateTime)
        traffic_index = Column(Integer)
        jams_count = Column(Integer)
        jams_length = Column(Float)
        jams_delay = Column(Float)
        traffic_index_weekago = Column(Integer)
        weekday = Column(String)


    # Defining the SQLAlchemy-query
    currency_query = db.query(Currency).with_entities(Currency.timestamp, Currency.date_time, Currency.traffic_index,Currency.jams_length, Currency.jams_count, Currency.jams_delay,Currency.traffic_index_weekago, Currency.weekday)

    # Getting all the entries via SQLAlchemy
    currencies = currency_query.all()

    # We provide also the (alternate) column names and set the index here,
    # renaming the column `id` to `currency__id`
    df_from_records = pd.DataFrame.from_records(currencies
        , index='timestamp'
        , columns=['timestamp', 'date_time',"traffic_index",'jams_count', 'jams_length',"jams_delay",'traffic_index_weekago', 'weekday'])
    print(df_from_records.head(5))
    save_new_data_to_csv(df_from_records, fetch_date)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, type=str)
    args = parser.parse_args()
    main(args.date)