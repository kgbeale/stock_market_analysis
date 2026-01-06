import time
import requests
import pandas as pd
import json

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

        # Convert json to csv without reading external file
        df = pd.json_normalize(json_data) # converts semi-structured json data into flat table (dataframe)
        df.to_csv(csv_filename)
    
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
    data: dict = fetch_data(update = False, json_cache = json_cache) # Call function to fetch existing data or make new api call
    # Set update to true if need new data

# No longer need an external file keeping track of run times, program now throws "KeyboardInterrupt" error
# if run again before previous run is complete

# "KeyboardInterrupt" error cancels the last run and immediately proceeds with the new run

# With the calls evenly spread out with time.sleep(), program will not be able to run again until all 
# tickers are processed

# Also spreading out the calls evenly will ensure the rate limit will never be hit