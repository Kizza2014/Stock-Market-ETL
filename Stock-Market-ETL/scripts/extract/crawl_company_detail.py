import requests
from dotenv import load_dotenv
import os
from datetime import date
import json


load_dotenv()
RAW_PATH = "/home/zap/raw_data"
URL = "https://www.alphavantage.co/query"
API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")


def crawl_income_statement_for_ticker(ticker):
    # fetch data from api
    params = {"function": "INCOME_STATEMENT", "symbol": ticker,"apikey": API_KEY}
    response = requests.get(url=URL, params=params).json()["quarterlyReports"]
    if response.status_code == 200:
        data = response.json()
    else:
        print(f"Error: {response.status_code}")

    # save raw file
    crawl_date = date.today().strftime("%Y_%m_%d")
    savepath = os.path.join(RAW_PATH, "crawl_company_detail", "income_statement", f"{ticker}_{crawl_date}.json")
    with open(savepath, "w") as file:
        file.write(json.dumps(data))
    print(f"Raw data saved to {savepath}")

def crawl_income_statement():
    pass

if __name__ == "__main__":
    companies_list = ["TSLA", "APPL", "IBM"]
    crawl_income_statement(companies_list)