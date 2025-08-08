from extract import MostActiveQuoteCrawler
from load import MostActiveQuoteParser
from datetime import date
from dotenv import load_dotenv
from minio_utils import MinioClient

load_dotenv()  # Load environment variables from .env file


def crawl_active_tickers(crawl_date):
    print("\n\n================== MOST ACTIVE QUOTES CRAWLING ==================\n")
    crawler = MostActiveQuoteCrawler()
    crawler.crawl_all_tickers(crawl_date=crawl_date)
    print("\nCrawling completed.")

def parse_active_tickers(parse_date):
    print("\n\n================== MOST ACTIVE QUOTES PARSING ==================\n")
    parser = MostActiveQuoteParser()
    parser.parse_all_html(parse_date=parse_date)
    print("\nParsing completed.")

def crawl_histories(crawl_date):
    print("\n\n================== HISTORY CRAWLING ==================\n")
    # Implement history crawling logic here

def parse_histories(parse_date):
    print("\n\n================== HISTORY PARSING ==================\n")
    # Implement history parsing logic here

def crawl_profiles(crawl_date):
    print("\n\n================== PROFILE CRAWLING ==================\n")
    # Implement profile crawling logic here

def parse_profiles(parse_date):
    print("\n\n================== PROFILE PARSING ==================\n")
    # Implement profile parsing logic here

def crawl_statistics(crawl_date):
    print("\n\n================== STATISTICS CRAWLING ==================\n")
    # Implement statistics crawling logic here  

def parse_statistics(parse_date):
    print("\n\n================== STATISTICS PARSING ==================\n")
    # Implement statistics parsing logic here

def crawl_financials(crawl_date):
    print("\n\n================== FINANCIALS CRAWLING ==================\n")
    # Implement financials crawling logic here

def parse_financials(parse_date):
    print("\n\n================== FINANCIALS PARSING ==================\n")
    # Implement financials parsing logic here


if __name__ == "__main__":
    load_dotenv()  # Load environment variables from .env file

    crawl_date = date.today().strftime("%Y_%m_%d")
    print(f"Crawling date: {crawl_date}")

    # # crawl active tickers
    # crawl_active_tickers(crawl_date=crawl_date)

    # parse active tickers
    # parse_active_tickers(parse_date=crawl_date)