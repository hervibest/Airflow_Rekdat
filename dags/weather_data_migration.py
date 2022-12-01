import argparse
from pathlib import Path
from model_weather import Connection
import config

# Initialize Weather Table
def main(db_connection):
    Path(config.CSV_FILE_DIR).mkdir(parents=True, exist_ok=True)
    
    connection = Connection(db_connection)
    session = connection.get_session()
    session.execute('''CREATE TABLE IF NOT EXISTS weather (
    time int,
    precipMM DECIMAL,
    date_time TIMESTAMP PRIMARY KEY, 
    weathervalue VARCHAR(50) 
    )''')
    session.commit()
    session.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--connection", required=True, type=str)
    args = parser.parse_args()
    main(args.connection)