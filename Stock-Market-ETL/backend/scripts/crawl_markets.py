import requests
from dotenv import load_dotenv
import os
import pandas as pd
from datetime import date


load_dotenv()
RAW_PATH = "E:\\Intern\\Stock-Market-ETL\\backend\\data\\raw\\"
URL = "https://www.alphavantage.co/query"
API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")


def crawl_markets():
    # fetch data from api
    params = {"function": "MARKET_STATUS", "apikey": API_KEY}
    response = requests.get(url=URL, params=params)
    if response.status_code == 200:
        data = response.json()["markets"]
        print(f"Extracted {len(data)} markets")
        
        # save data to csv
        df = pd.DataFrame(data=data)
        today_date = date.today().strftime("%Y_%m_%d")
        filepath = RAW_PATH + f"crawl_markets_{today_date}.csv"
        df.to_csv(filepath, index=False)
        print(f"Data saved to {filepath}")
    else:
        print(f"Error: {response.status_code}")

    
if __name__ == "__main__":
    crawl_markets()