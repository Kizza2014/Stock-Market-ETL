import requests
from datetime import date
import pandas as pd
from dotenv import load_dotenv
import os


load_dotenv()

RAW_PATH = "./data/raw"
EXCHANGES = ["NASY", "NASDAQ"]  # list of stock exchanges to extract data from
URL = "https://api.sec-api.io/mapping/exchange/"  # sec api
API_KEY = os.getenv("SEC_API_KEY") # api key for sec


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

    # save crawled data to csv
    df = pd.DataFrame(data=companies_list)
    today_date = date.today().strftime("%Y_%m_%d")
    filepath = os.path.join(RAW_PATH, f"crawl_companies_{today_date}.csv")
    df.to_csv(filepath, index=False)
    print(f"Data saved to path: {filepath}")


if __name__ == "__main__":
    crawl_companies()
