import time
import requests
import pandas as pd
import json
from sqlalchemy import create_engine

# If file exists and no new updates, fetch data from existing file
def fetch_data(*, update: bool = False, json_cache: str):
    if update:
        json_data = None
    else:
        try:
            with open(json_cache, 'r') as f:
                json_data = json.load(f) # Retrieve data from local cache
        except(FileNotFoundError, json.JSONDecodeError) as e:
            json_data = None
    
    # make new api call
    if not json_data:
        json_data = []
        for i in range(len(tickers)):
            url = f'https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={tickers[i]}&apikey={api_key}'
            r = requests.get(url)
            data = r.json()
            json_data.append(data)
        
            # Calculate sleep time to maintain rate limit
            start = time.time()
            elapsed = time.time() - start
            sleep_time = max(0, interval - elapsed)
            time.sleep(sleep_time)
    
        # Store new data in local cache
        with open(json_cache, "w") as f:
            json.dump(json_data, f, indent=4)
    
    return json_data

if __name__ == '__main__':
    api_key = "STNE15X0T16UDA4F"
    tickers = ["IBM", "MSFT", "AAPL", "UBS"]
    calls_per_minute = 75
    interval = 60.0 / calls_per_minute  # Time between calls in seconds
    json_cache = "global_quote_endpoint.json"
    csv_filename = "global_quote_endpoint.csv"
    data = fetch_data(update = True, json_cache = json_cache) # Call function to fetch existing data or make new api call
    # Set update to true for new data

    # Rename columns to remove numbers (ex. '01. symbol' becomes just 'symbol')
    # split keys and remove if they can be converted to int

    # global_qoute = data["Global Quote Endpoint"]
    #
    # df = (pd.DataFrame.from_dict(quote_endpoint, orient="index")
    #       .rename_axis("date")
    #       .reset_index())

    # Above code is good for renaming single columns, below code is good renaming all columns
    # Below code is also good for iterating through nested dictionaries
    # new_data = [] # Create new list to store updated data
    # for i in range(len(data)):
    #     def rename_columns(d):
    #         new_dict = {} # Create new dictionary with modified key names
    #         for key, value in d.items():
    #             if isinstance(value, dict):
    #                 rename_columns(value)
    #             else:
    #                 parts = key.split('. ')
    #                 for part in parts:
    #                     new_dict[part] = value
    #                 keys_to_remove = []
    #                 for key1, value1 in new_dict.items():
    #                     try:
    #                         int(key1)
    #                         keys_to_remove.append(key1)
    #                     except ValueError:
    #                         continue
    #                 for j in keys_to_remove:
    #                     del new_dict[j]               
    #         # Append new dict to list if dict is not empty
    #         if new_dict:
    #             new_data.append(new_dict)

    #     rename_columns(data[i]) # Call function to rename columns
    
    # Export data into csv file
    # df = pd.DataFrame(new_data)
    # df.to_csv(csv_filename)

    # Export data to csv
    df = pd.json_normalize(data)
    df.to_csv(csv_filename)
    # Rename columns

    # Create another pandas dataframe with just 2 columns (symbol, quantity), default value 1000 for both
    symbol_quantity = []
    symbol_quantity_csv = "global_quote_symbol_quantity.csv"
    symbol_quantity_dict = {'symbol': '1000', 'quantity': '1000'}
    symbol_quantity.append(symbol_quantity_dict)
    df = pd.DataFrame(symbol_quantity)
    df.to_csv(symbol_quantity_csv)

    # log data to mysql database
    # MySQL database connection details
    DB_USER = 'root'
    DB_PASSWORD = ''
    DB_HOST = 'localhost'
    DB_NAME = 'stock_apis'
    CSV_FILE_PATH = csv_filename
    TABLE_NAME = 'global_quote_endpoint'

    # Create a database engine using SQLAlchemy
    engine = create_engine(f'mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}')

    # Read the CSV file into a Pandas DataFrame
    df = pd.read_csv(CSV_FILE_PATH)

    # Insert the data into the MySQL table
    # if_exists='replace' drops previous table and creates new one with updated data
    df.to_sql(TABLE_NAME, con=engine, index=False, if_exists='replace')

    print(f"Data from {CSV_FILE_PATH} successfully imported into table {TABLE_NAME}.")

    # Close the connection (handled by the engine implicitly, but good practice to be aware)
    engine.dispose()

# No longer need an external file keeping track of run times, program now throws "KeyboardInterrupt" error
# if run again before previous run is complete

# "KeyboardInterrupt" error cancels the last run and immediately proceeds with the new run

# With the calls evenly spread out with time.sleep(), program will not be able to run again until all 
# tickers are processed

# Also spreading out the calls evenly will ensure the rate limit will never be hit

# Something to do in the future: Plot data

# Start this week: Real world stock analysis using code I've already developed
# Couple more things need to be done here first
# Course focused on financial market analysis using Python Pandas
# Use data from Time Series Daily Adjusted endpoint (same code as I have here,
# change names of files and update url to pull from Time Series Daily Adjusted)
# Only use one ticker for now
# Time Series Daily Adjusted shows last 100 days
# Create new file so that the global quote code is saved