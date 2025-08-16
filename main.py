from extract import (MostActiveQuoteCrawler, HistoryCrawler, ProfileCrawler, StatisticsCrawler,
                     IncomeStatementCrawler)
from load import (MostActiveQuoteParser, HistoryParser, ProfileParser, StatisticsParser,
                  IncomeStatementParser)
from datetime import date
from dotenv import load_dotenv


load_dotenv()  # Load environment variables from .env file


def crawl_active_tickers(crawl_date):
    print("\n\n================== MOST ACTIVE QUOTES CRAWLING ==================\n")
    crawler = MostActiveQuoteCrawler()
    crawler.crawl_all_tickers(crawl_date=crawl_date)
    print("\nCrawling completed.")

def parse_active_tickers(parse_date):
    print("\n\n================== MOST ACTIVE QUOTES PARSING ==================\n")
    parser = MostActiveQuoteParser()
    most_active_tickers = parser.parse_all_html(parse_date=parse_date)
    print("\nParsing completed.")
    return most_active_tickers

def crawl_histories(tickers, crawl_date):
    print("\n\n================== HISTORY CRAWLING ==================\n")
    crawler = HistoryCrawler()
    crawler.crawl_all_daily_histories(tickers=tickers, crawl_date=crawl_date)
    print("\nCrawling completed.")

def parse_histories(parse_date):
    print("\n\n================== HISTORY PARSING ==================\n")
    parser = HistoryParser()
    parser.parse_all_html(parse_date=parse_date)
    print("\nParsing completed.")

def crawl_profiles(tickers, crawl_date):
    print("\n\n================== PROFILE CRAWLING ==================\n")
    crawler = ProfileCrawler()
    crawler.crawl_profile(tickers, crawl_date)
    print("\nCrawling completed.")

def parse_profiles(parse_date):
    print("\n\n================== PROFILE PARSING ==================\n")
    parser = ProfileParser()
    parser.parse_all_html(parse_date=parse_date)
    print("\nParsing completed.")

def crawl_statistics(tickers, crawl_date):
    print("\n\n================== STATISTICS CRAWLING ==================\n")
    crawler = StatisticsCrawler()
    crawler.crawl_statistics(tickers, crawl_date=crawl_date)
    print("\nCrawling completed.") 

def parse_statistics(parse_date):
    print("\n\n================== STATISTICS PARSING ==================\n")
    parser = StatisticsParser()
    parser.parse_all_html(parse_date=parse_date)
    print("\nParsing completed.")

def crawl_income_statement(tickers, crawl_date):
    print("\n\n================== INCOME STATEMENT CRAWLING ==================\n")
    crawler = IncomeStatementCrawler()
    crawler.crawl_income_statement(tickers, crawl_date=crawl_date)
    print("\nCrawling completed.")

def parse_income_statement(parse_date):
    print("\n\n================== INCOME STATEMENT PARSING ==================\n")
    parser = IncomeStatementParser()
    parser.parse_all_html(parse_date=parse_date)
    print("\nParsing completed.")


if __name__ == "__main__":
    load_dotenv()  # Load environment variables from .env file

    # crawl_date = date.today().strftime("%Y_%m_%d")
    crawl_date = "2025_08_02"
    print(f"Crawling date: {crawl_date}")

    # # crawl active tickers
    # crawl_active_tickers(crawl_date=crawl_date)

    # # parse active tickers
    # most_active_tickers = parse_active_tickers(parse_date=crawl_date) # liệt kê danh sách tickers để crawl thông tin chi tiết

    # # crawl history
    # crawl_histories(tickers=most_active_tickers, crawl_date=crawl_date)

    # # parse history
    # parse_histories(parse_date=crawl_date)

    # # crawl profile
    # crawl_profiles(tickers=most_active_tickers, crawl_date=crawl_date)

    # # parse profile
    # parse_profiles(parse_date=crawl_date)

    # # crawl statistics
    # crawl_statistics(most_active_tickers, crawl_date=crawl_date)

    # # parse statistics
    # parse_statistics(parse_date=crawl_date)

    # #  crawl income statement
    # crawl_income_statement(tickers=most_active_tickers, crawl_date=crawl_date)

    # parse income statment
    parse_income_statement(parse_date=crawl_date)

    # transform profile
    