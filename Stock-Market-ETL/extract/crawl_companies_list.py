import requests
from datetime import date
from dotenv import load_dotenv
import os
import json

load_dotenv()

EXCHANGES = ["NYSE", "NASDAQ", "BATS"]  # list of stock exchanges to extract data from
URL = "https://api.sec-api.io/mapping/exchange/"  # sec api
API_KEY = os.getenv("SEC_API_KEY") # api key for sec
RAW_PATH = "/home/zap/raw_data/crawl_companies_list"


def crawl_companies():
    # iterate through exchanges to fetch data
    companies_list = []
    for exchange in EXCHANGES:
        url = URL + exchange
        params = {"token": API_KEY}
        response = requests.get(url=url, params=params)

        if response.status_code == 200: 
            data = response.json()
            companies_list.extend(data)        
            print(f"Extracted {len(data)} companies from exchange {exchange}")    
        else:
            print(f"Error: {response.status_code}")

    # save raw file
    crawl_date = date.today().strftime("%Y_%m_%d")
    savepath = os.path.join(RAW_PATH, f"crawl_companies_list_{crawl_date}.json")
    with open(savepath, "w") as file:
        file.write(json.dumps(companies_list))
    print(f"Raw data saved to {savepath}")

if __name__ == "__main__":
    crawl_companies()
