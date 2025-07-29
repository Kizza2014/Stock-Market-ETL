import requests
from dotenv import load_dotenv
import os
from datetime import date
import json


load_dotenv()
RAW_PATH = "/home/zap/raw_data/crawl_markets_list"
URL = "https://www.alphavantage.co/query"
API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")


def crawl_markets():
    # fetch data from api
    params = {"function": "MARKET_STATUS", "apikey": API_KEY}
    response = requests.get(url=URL, params=params)
    if response.status_code == 200:
        data = response.json()["markets"]
        print(f"Extracted {len(data)} markets")
    else:
        print(f"Error: {response.status_code}")

    # save raw file
    crawl_date = date.today().strftime("%Y_%m_%d")
    savepath = os.path.join(RAW_PATH, f"crawl_markets_list_{crawl_date}.json")
    with open(savepath, "w") as file:
        file.write(json.dumps(data))
    print(f"Raw data saved to {savepath}")
    
if __name__ == "__main__":
    crawl_markets()