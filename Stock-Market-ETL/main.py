from extract import MostActiveQuoteCrawler
from datetime import date
import os


MOST_ACTIVE_QUOTE_PATH = "active_tickers"

def crawl_tickers(save_path):
    URL = "https://finance.yahoo.com/markets/stocks/most-active/"
    crawler = MostActiveQuoteCrawler()
    crawler.crawl_all_tickers(URL, save_path)


if __name__ == "__main__":
    crawl_date = date.today().strftime("%Y_%m_%d")
    print(f"Crawling date: {crawl_date}")

    # crawl active tickers
    active_quotes_path = os.path.join(MOST_ACTIVE_QUOTE_PATH, crawl_date)
    print(f"Path to save crawled data: {active_quotes_path}")
    crawl_tickers(active_quotes_path)
    print("\nCrawling completed.")