import requests
from dotenv import load_dotenv
import os
import pandas as pd
from datetime import date
import json
import csv


load_dotenv()
RAW_PATH = "./test_data"
URL = "https://www.alphavantage.co/query"
API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")

def crawl_markets():
    # fetch data from api
    params = {
        "function": "TIME_SERIES_DAILY", 
        "symbol": "TSLA",
        "apikey": API_KEY
    }
    response = requests.get(url=URL, params=params)
    if response.status_code == 200:
        data = response.json()
        save_path = os.path.join(RAW_PATH, "time_series_daily_tesla.json")
        with open(save_path, 'w') as f:
            f.write(json.dumps(data))
        # # save data to csv
        # df = pd.DataFrame(data=data)
        # today_date = date.today().strftime("%Y_%m_%d")
        # filepath = os.path.join(RAW_PATH, f"crawl_markets_{today_date}.csv")
        # df.to_csv(filepath, index=False)
        # print(f"Data saved to {filepath}")
    else:
        print(f"Error: {response.status_code}")

    
if __name__ == "__main__":
    crawl_markets()