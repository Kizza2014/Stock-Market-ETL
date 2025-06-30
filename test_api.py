import requests

URL = "https://www.alphavantage.co/query"
API_KEY = "NB09SU9L61LSYOGT"

params = {"function": "MARKET_STATUS", "apikey": API_KEY}
response = requests.get(url=URL, params=params)

if response.status_code == 200:
    data = response.json()
    print(data)
else:
    print(f"Error: {response.status_code}")