import requests
from dotenv import load_dotenv
import os
import pandas as pd
from datetime import date
import json


load_dotenv()
SAVE_PATH = "/home/zap/simulation_data/fundamental_data"
URL = "https://www.alphavantage.co/query"
API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY9")

def crawl_fundamental_data(symbol):
    # fetch data from api
    data_to_crawl = {
        "income_statment": {
            "function": "INCOME_STATEMENT",
            "symbol": symbol, 
            "apikey": API_KEY
        },
        "balance_sheet": {
            "function": "BALANCE_SHEET",
            "symbol": symbol, 
            "apikey": API_KEY
        },
        "cashflow": {
            "function": "CASH_FLOW",
            "symbol": symbol, 
            "apikey": API_KEY
        },
        "earnings": {
            "function": "EARNINGS",
            "symbol": symbol, 
            "apikey": API_KEY
        }
    }

    for data_type, param in data_to_crawl.items():
        response = requests.get(url=URL, params=param)
        if response.status_code == 200:
            data = response.json()
            filepath = os.path.join(SAVE_PATH, symbol)
            # check if path exists
            if not os.path.exists(filepath):
                os.makedirs(filepath)
            save_path = os.path.join(filepath, f"{symbol.upper()}_{data_type}.json")
            with open(save_path, 'w') as f:
                f.write(json.dumps(data))
            print(f"Crawled {data_type} data for {symbol.upper()}")
        else:
            print(f"Error: {response.status_code}")

    
if __name__ == "__main__":
    # NYSE (48 tickers)
    # symbols = ["A", "AA", "AAAGY", "AAC", "AAC1", "AACH"]
    # symbols = ["AACT", "AAH", "AAIC", "AAM", "AAMI", "AAMRQ"]
    # symbols = ["AAN", "AAP", "AAQC", "ABWTQ", "MER-PK", "MET-PB"]
    # symbols = ["MBSC.WS", "MBAC.WS", "MAV1", "LYG", "LXP-PC", "RGT"]
    # symbols = ["LOKM.U", "LNFA.U", "KYOCY", "KUKE", "KLR.WS", "KFN-P"]
    # symbols = ["JPM-PA", "JMEI", "JKS", "KNMCY", "JBI.WS", "IX"]
    # symbols = ["IVR-PA", "ITUB", "ITCL", "ISA", "IRC-PB", "HZAC.U"]
    # symbols = ["HWKZ.U", "HSEB", "HSEA", "HMY", "GUCG", "GTR1"]

    # NASDAQ

    # BATS


    symbols = []
    for symbol in symbols:
        crawl_fundamental_data(symbol)