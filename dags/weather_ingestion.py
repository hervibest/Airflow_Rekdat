import requests
import argparse
import os
import logging
from datetime import timedelta, datetime
import pandas as pd
import json
import urllib.request
import urllib.parse
import config
    
def get_yesterday_date(fetch_date):
    return datetime.strptime(fetch_date, '%Y-%m-%d').date() - timedelta(1)

def get_file_path(fetch_date):
    yesterday = get_yesterday_date(fetch_date)
    filename = "Weather_{}.csv".format(yesterday)
    
    return os.path.join(config.WEATHER_FILE_DIR, filename)

def import_data():  
    today = datetime.today()
    yesterday = today - timedelta(days=1)
    test = datetime.strftime(yesterday, '%Y-%m-%d')
    url = f"https://api.worldweatheronline.com/premium/v1/past-weather.ashx?key=01749ef9ac104e669c9133109222811&q=Jakarta&format=json&date={test}&tp=1"


    # data_req = requests.get(url)
    # data_json = data_req.json()

    json_page = urllib.request.urlopen(url, timeout=10)
    json_data = json.loads(json_page.read().decode())
    logging.info(json_data)
    return json_data

def transform_data(data_json):
    df_hist = pd.DataFrame()
    data=data_json['data']['weather']
    df_this_month = extract_monthly_data(data)
    df_hist = pd.concat([df_hist, df_this_month])
    dataframe = pd.DataFrame(df_hist)
    # convert timezone(UTC) to local time(Jakarta)

    return dataframe

def get_new_data(df, fetch_date):
    yesterday = get_yesterday_date(fetch_date)
    data_to_append = df[(df['date_time'].dt.date == yesterday)]
    return data_to_append

def extract_monthly_data(data):
    num_days = len(data)
    # initialize df_month to store return data
    df_month = pd.DataFrame()
    for i in range(num_days):
        # extract this day
        d = data[i]
        # astronomy data is the same for the whole day
        astr_df = pd.DataFrame(d['astronomy'])
        # hourly data; temperature for each hour of the day
        hourly_df = pd.DataFrame(d['hourly'])
        # this wanted_key will be duplicated and use 'ffill' to fill up the NAs
        wanted_keys = ['date', 'maxtempC', 'mintempC', 'totalSnow_cm', 'sunHour', 'uvIndex']  # The keys you want
        subset_d = dict((k, d[k]) for k in wanted_keys if k in d)
        this_df = pd.DataFrame(subset_d, index=[0])
        df = pd.concat([this_df.reset_index(drop=True), astr_df], axis=1)
        # concat selected astonomy columns with hourly data
        df = pd.concat([df, hourly_df], axis=1)
        df = df.fillna(method='ffill')
        
        # make date_time columm to proper format
        # fill leading zero for hours to 4 digits (0000-2400 hr)
        df['weathervalue'] = df['weatherDesc'].apply(lambda x: x[0]['value'])
        df['time'] = df['time'].apply(lambda x: x.zfill(4))
        # keep only first 2 digit (00-24 hr) 
        df['time'] = df['time'].str[:2]
        # convert to pandas datetime
        df['date_time'] = pd.to_datetime(df['date'] + ' ' + df['time'])
        # keep only interested columns
        col_to_keep = ['date_time', 'time','weathervalue',"precipMM"]
        df = df[col_to_keep]
        df = df.loc[:,~df.columns.duplicated()]
        df_month = pd.concat([df_month, df])
    return (df_month)



def save_new_data_to_csv(data_to_append, fetch_date):
    filename = get_file_path(fetch_date)
    
    if not data_to_append.empty:
        logging.info("tertambahkan")
        data_to_append.to_csv(filename, encoding='utf-8', index=False)

def main(fetch_date):   
    data_json = import_data()
    df = transform_data(data_json)
    data_to_append = get_new_data(df, fetch_date)
    save_new_data_to_csv(data_to_append, fetch_date)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, type=str)
    args = parser.parse_args()
    main(args.date)