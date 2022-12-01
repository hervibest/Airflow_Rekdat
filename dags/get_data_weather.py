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
    filename = "Weather S_{}.csv".format(yesterday)
    
    return os.path.join(config.WEATHER_FILE_DIR, filename)
def save_new_data_to_csv(data_to_append, fetch_date):
    filename = get_file_path(fetch_date)

    data_to_append.to_csv(filename, encoding='utf-8')






def get_file_path(fetch_date):
    yesterday = get_yesterday_date(fetch_date)
    filename = "Weather S_{}.csv".format(yesterday)
    
    return os.path.join(config.WEATHER_FILE_DIR, filename)

def main(fetch_date):
    SQLALCHEMY_DATABASE_URI = 'postgresql://postgres:informatika@mydatabase-instance.csbnsdtoskt5.ap-northeast-1.rds.amazonaws.com/Dietalicious_database'
    engine = create_engine(SQLALCHEMY_DATABASE_URI, pool_pre_ping=True, echo=False)
    db = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
    Base = declarative_base(bind=engine)


    class Currency(Base):
        """The `Currency`-table"""
        __tablename__ = "weather"
        time = Column(Integer)
        date_time = Column(DateTime , primary_key=True)
        weathervalue = Column(String)
        precipmm = Column(Float)


    # Defining the SQLAlchemy-query
    currency_query = db.query(Currency).with_entities(Currency.time, Currency.date_time, Currency.weathervalue,Currency.precipmm)

    # Getting all the entries via SQLAlchemy
    currencies = currency_query.all()

    # We provide also the (alternate) column names and set the index here,
    # renaming the column `id` to `currency__id`
    df_from_records = pd.DataFrame.from_records(currencies
        , index='date_time'
        , columns=['time', 'date_time',"weathervalue","percipmm"])
    print(df_from_records.head(5))   
    save_new_data_to_csv(df_from_records, fetch_date)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, type=str)
    args = parser.parse_args()
    main(args.date)